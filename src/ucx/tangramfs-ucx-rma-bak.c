#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <mpi.h>
#include <ucp/api/ucp.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

ucp_context_h       g_ucp_context;
ucp_worker_h        g_request_worker;       // for sending rma request, main thread uses
ucp_worker_h        g_respond_worker;       // for sending rma respond, pthread progress on this

ucp_address_t**     g_respond_addrs;        // Processor's respond worker address
ucp_address_t*      g_request_addr;         // my rma request worker address
size_t              g_worker_addrlen;

static int          g_mpi_rank;
static int          g_mpi_size;

static volatile bool g_running = true;
static volatile int  g_current_reqs = 0;
static volatile bool g_received_respond = false;

// The user of RMA serice needs to provide
// this funciton to provide the actual data to
// send though RMA
void* (*g_serve_rma_data)(void*, size_t *size);

static pthread_t g_progress_thread;


typedef struct ptrs_to_free {
    int op;
    ucp_address_t *peer_worker_addr;
    ucp_rkey_h *rkey_p;
    void* rma_data;
} ptrs_to_free_t;


/* Main thread wait for a request to finsih
 * Since we have a dedicate pthread doing progress
 * Here we only need to check the request status
 *
 * Note!!
 * the dedicate pthread can not call this function as
 * it will block forever since the pthread is also
 * responsible for progressing.
 */
void wait_request(void* request) {
    ucs_status_t status;
    if(request == NULL)
        return;

    if(UCS_PTR_IS_ERR(request)) {
        status = UCS_PTR_STATUS(request);
        printf("Error at wait_request(): %s\n", ucs_status_string(status));
        return;
    }

    do {
        status = ucp_request_check_status(request);
    } while(status == UCS_INPROGRESS);
    assert(status == UCS_OK);

    ucp_request_free(request);
}



// Main thread should not use worker_flush() in -comm.c
// This function only checks the status of the request,
// as we have dedicated progre_thread doing the progress in background
void worker_flush_from_main_thread() {
    void* request = ucp_worker_flush_nb(g_respond_worker, 0, empty_callback);
    wait_request(request);
}


void* pack_request_arg(ucp_address_t* worker_addr, size_t worker_addr_len,
                            uint64_t my_addr, void* rkey_buf, size_t rkey_buf_size,
                            void* user_arg, size_t user_arg_size, size_t *total_size) {
    *total_size = sizeof(int) + sizeof(uint64_t) + sizeof(size_t)*3 + worker_addr_len + rkey_buf_size + user_arg_size;

    int pos = 0;
    void* send_buf = malloc(*total_size);

    memcpy(send_buf+pos, &g_mpi_rank, sizeof(int));
    pos += sizeof(int);

    memcpy(send_buf+pos, &worker_addr_len, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(send_buf+pos, worker_addr, worker_addr_len);
    pos += worker_addr_len;

    memcpy(send_buf+pos, &my_addr, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(send_buf+pos, &rkey_buf_size, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(send_buf+pos, rkey_buf, rkey_buf_size);
    pos += rkey_buf_size;

    memcpy(send_buf+pos, &user_arg_size, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(send_buf+pos, user_arg, user_arg_size);
    pos += user_arg_size;

    return send_buf;
}



void pthread_sent_respond_cb(void *request, ucs_status_t status, void *user_data) {
    ptrs_to_free_t* ptrs = (ptrs_to_free_t*) user_data;
    ucp_rkey_destroy(*(ptrs->rkey_p));
    free(ptrs->rkey_p);
    free(ptrs->rma_data);
    free(ptrs);
    ucp_request_free(request);
    g_current_reqs--;
}


/**
 * Careful here, this function is invoked upon the
 * RMA request, which means this function is invoked
 * by the pthread ucp_worker_progress().
 *
 * So,
 * - We can not call ucp_worker_progress() inside this function
 * - We can not call wait_request() as it will loop forever.
 */
void rma_respond(void* data) {

    ptrs_to_free_t *ptrs = malloc(sizeof(ptrs_to_free_t));

    int dest_rank;
    uint64_t remote_addr;
    size_t worker_addr_len, rkey_buf_size, user_arg_size;
    void *rkey_buf, *user_arg;

    int pos = 0;
    memcpy(&dest_rank, data+pos, sizeof(int));
    pos += sizeof(int);

    memcpy(&worker_addr_len, data+pos, sizeof(size_t));
    pos += sizeof(size_t);

    ptrs->peer_worker_addr = malloc(worker_addr_len);
    memcpy(ptrs->peer_worker_addr, data+pos, worker_addr_len);
    pos += worker_addr_len;

    memcpy(&remote_addr, data+pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(&rkey_buf_size, data+pos, sizeof(size_t));
    pos += sizeof(size_t);

    rkey_buf = malloc(rkey_buf_size);
    memcpy(rkey_buf, data+pos, rkey_buf_size);
    pos += rkey_buf_size;

    memcpy(&user_arg_size, data+pos, sizeof(size_t));
    pos += sizeof(size_t);
    user_arg = data+pos;

    // Connect EP
    ucp_ep_h ep;
    ep_connect(ptrs->peer_worker_addr, g_respond_worker, &ep);

    // Get rkey
    ucs_status_t status;
    ptrs->rkey_p = malloc(sizeof(ucp_rkey_h));
    status = ucp_ep_rkey_unpack(ep, rkey_buf, ptrs->rkey_p);
    assert(status == UCS_OK);
    free(rkey_buf);

    // actual RMA
    size_t rma_data_size;
    ptrs->rma_data = g_serve_rma_data(user_arg, &rma_data_size);
    ucp_put_nbi(ep, ptrs->rma_data, rma_data_size, remote_addr, *(ptrs->rkey_p));

    // Make sure the RMA operation completes before the following ACK message.
    ucp_worker_fence(g_respond_worker);

    // Send ACK
    ucp_request_param_t am_param;
    am_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS    |
                            UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_USER_DATA;
    am_param.flags        = UCP_AM_SEND_FLAG_EAGER;
    am_param.cb.send      = pthread_sent_respond_cb;
    am_param.user_data    = ptrs;
    ptrs->op = OP_RMA_RESPOND;      // cant use a stack variable as the send call maybe delayed
    g_current_reqs++;
    void *request = ucp_am_send_nbx(ep, UCX_AM_ID_RMA, &ptrs->op, sizeof(int), NULL, 0, &am_param);
    // complete in place, the callback will not be called.
    if(request == NULL) {
        free(ptrs->peer_worker_addr);
        ucp_rkey_destroy(*(ptrs->rkey_p));
        free(ptrs->rkey_p);
        free(ptrs->rma_data);
        free(ptrs);
        g_current_reqs--;
    }
}

// Handler for active messages
static ucs_status_t rma_request_listener(void *arg, const void *header, size_t header_length,
                              void *data, size_t length,
                              const ucp_am_recv_param_t *param) {

    int op = *((int*)header);
    assert(op == OP_RMA_REQUEST);

    // Pthread worker
    // 1. Received an RMA request
    // Need to send data thorugh RMA,then respond through AM.
    rma_respond(data);

    return UCS_OK;
}

// Handler for active messages
static ucs_status_t rma_respond_listener(void *arg, const void *header, size_t header_length,
                              void *data, size_t length,
                              const ucp_am_recv_param_t *param) {

    int op = *((int*)header);
    assert(op == OP_RMA_RESPOND);

    // Main thread worker
    // 2. Received the respond for my previous RMA request
    g_received_respond = true;

    return UCS_OK;
}



/** Send a RMA request
 * (1) connect
 * (2) register memory
 * (3) send my rkey
 * (4) wait for ack
 *
 * void* recv_buf is the user's buffer in tfs_read()
 * we will check if it is page-aligned, if so we
 * will use it directly for RMA.
 * if not we let UCX to allocate memory for RMA
 * and copy it to user's buffer.
 *
 * this function should be called by the main thread.
 */
void tangram_ucx_rma_request(int dest_rank, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size) {
    // TODO: should directly put data to user's buffer
    g_received_respond = false;

    ucp_am_handler_param_t am_handler_param;
    am_handler_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                                  UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_handler_param.id         = UCX_AM_ID_RMA;
    am_handler_param.cb         = rma_respond_listener;
    ucs_status_t status         = ucp_worker_set_am_recv_handler(g_request_worker, &am_handler_param);
    assert(status == UCS_OK);

    ucp_ep_h ep;
    ep_connect(g_respond_addrs[dest_rank], g_request_worker, &ep);

    // mem map
    ucp_mem_map_params_t params;
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    params.length = recv_size;
    params.address = NULL;                              // Let UCP allocate memory for us
    params.field_mask |= UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.flags = UCP_MEM_MAP_ALLOCATE;

    ucp_mem_h memh;
    status = ucp_mem_map(g_ucp_context, &params, &memh);
    assert(status == UCS_OK);

    // retrive the addressed allocated by UCX,
    // while will be sent to the remote so the remote process
    // can use it as the remote_addr parameter
    ucp_mem_attr_t attr;
    attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;
    ucp_mem_query(memh, &attr);
    uint64_t mem_addr = (uint64_t) attr.address;

    // pack rkey_buf
    char* rkey_buf = NULL;
    size_t rkey_buf_size;
    status = ucp_rkey_pack(g_ucp_context, memh, (void**)&rkey_buf, &rkey_buf_size);
    assert(status == UCS_OK && rkey_buf);

    size_t total_buf_size;
    void* sendbuf = pack_request_arg(g_request_addr, g_worker_addrlen, mem_addr, rkey_buf, rkey_buf_size, user_arg, user_arg_size, &total_buf_size);

    ucp_request_param_t am_param;
    am_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    am_param.flags        = UCP_AM_SEND_FLAG_EAGER;
    int op = OP_RMA_REQUEST;
    void *request = ucp_am_send_nbx(ep, UCX_AM_ID_RMA, &op, sizeof(int), sendbuf, total_buf_size, &am_param);
    request_finalize(g_request_worker, request);
    free(sendbuf);

    // Wait for the respond
    while(!g_received_respond) {
        ucp_worker_progress(g_request_worker);
    }

    // Now we should have the data ready
    memcpy(recv_buf, attr.address, recv_size);

    status = ucp_mem_unmap(g_ucp_context, memh);
    assert(status == UCS_OK);

    ep_close(g_request_worker, ep);
}

void exchange_address(void* addr, size_t addr_len) {
    MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

    void* all_addrs = malloc(addr_len*g_mpi_size);
    MPI_Allgather(addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    g_respond_addrs = malloc(g_mpi_size*sizeof(ucp_address_t*));

    for(int rank = 0; rank < g_mpi_size; rank++) {
        g_respond_addrs[rank] = malloc(addr_len);
        memcpy(g_respond_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);
}

void* progress_loop(void* arg) {
    while(g_running) {
        ucp_worker_progress(g_respond_worker);
    }
}

void tangram_ucx_rma_service_start(void* (serve_rma_data)(void*, size_t*)) {

    g_serve_rma_data = serve_rma_data;

    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_request_worker, true);
    init_worker(g_ucp_context, &g_respond_worker, true);

    ucs_status_t status;
    ucp_address_t *addr = NULL;
    size_t addr_len;
    status = ucp_worker_get_address(g_request_worker, &g_request_addr, &g_worker_addrlen);
    assert(status == UCS_OK);
    status = ucp_worker_get_address(g_respond_worker, &addr, &addr_len);
    assert(status == UCS_OK && addr);
    exchange_address(addr, addr_len);

    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID | UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_RMA;
    am_param.cb         = rma_request_listener;
    status              = ucp_worker_set_am_recv_handler(g_respond_worker, &am_param);
    assert(status == UCS_OK);

    pthread_create(&g_progress_thread, NULL, progress_loop, NULL);
}

void tangram_ucx_rma_service_stop() {
    // Make sure all RMA communications are finished
    //worker_flush_from_main_thread();
    while(g_current_reqs>0){
    }
    g_running = false;
    pthread_join(g_progress_thread, NULL);

    MPI_Barrier(MPI_COMM_WORLD);

    // free addrs
    ucp_worker_release_address(g_respond_worker, g_respond_addrs[g_mpi_rank]);
    for(int rank = 0; rank < g_mpi_size; rank++) {
        if(rank != g_mpi_rank)
            free(g_respond_addrs[rank]);
    }

    free(g_respond_addrs);
    ucp_worker_destroy(g_request_worker);
    ucp_worker_destroy(g_respond_worker);
    ucp_cleanup(g_ucp_context);
}

