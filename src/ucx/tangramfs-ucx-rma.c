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
ucp_worker_h        g_rma_worker;       // for rma
ucp_address_t**     g_rma_addrs;        // rma_addrs[i] is Processor i's rma service address


static volatile bool g_running = true;
static volatile bool g_ack = false;

// The user of RMA serice needs to provide
// this funciton to provide the actual data to
// send though RMA
void* (*g_serve_rma_data)(void*, size_t *size);

static pthread_t g_progress_thread;


typedef struct ptrs_to_free {
    int op;
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
// This function only checks the status of the requeset,
// as we have dedicated progre_thread doing the progress in background
void worker_flush_from_main_thread() {
    void* request = ucp_worker_flush_nb(g_rma_worker, 0, empty_callback);
    wait_request(request);
}


void connect_ep(int rank, ucp_ep_h *ep) {
    ucs_status_t status;
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = g_rma_addrs[rank];
    status = ucp_ep_create(g_rma_worker, &ep_params, ep);
    assert(status == UCS_OK);
}

void* pack_request_arg(uint64_t my_addr, void* rkey_buf, size_t rkey_buf_size,
                                void* user_arg, size_t user_arg_size, size_t *total_size) {
    int my_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    *total_size = sizeof(int) + sizeof(uint64_t) + sizeof(size_t)*2 + rkey_buf_size + user_arg_size;

    int pos = 0;
    void* send_buf = malloc(*total_size);

    memcpy(send_buf+pos, &my_rank, sizeof(int));
    pos += sizeof(int);

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


/** Send a RMA requeset
 * (1) connect
 * (2) register memory
 * (3) send my rkey
 * (4) wait for ack
 *
 * void* recv_buf is the user's buffer in tfs_read()
 * we will check if it is page-aligned, if so we
 * will use it directly for RMA.
 *
 * if not we let UCX to allocate memory for RMA
 * and copy it to user's buffer.
 */
void tangram_ucx_rma_request(int dest_rank, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size) {
    bool aligned = false;
    /* TODO: this check does not guarantee the addr works
    if(recv_buf && ((uint64_t)recv_buf) % 4096 == 0) {
        aligned = true;
        printf("aligned!\n");
    }
    */

    ucp_ep_h ep;
    connect_ep(dest_rank, &ep);

    // mem map
    ucp_mem_map_params_t params;
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    params.length = recv_size;
    if(aligned) {
        params.address = recv_buf;
    } else {
        params.address = NULL;                              // Let UCP allocate memory for us
        params.field_mask |= UCP_MEM_MAP_PARAM_FIELD_FLAGS;
        params.flags = UCP_MEM_MAP_ALLOCATE;
    }
    ucp_mem_h memh;
    ucs_status_t status;
    status = ucp_mem_map(g_ucp_context, &params, &memh);
    assert(status == UCS_OK);

    // retrive the addressed allocated by UCX,
    // while will be sent to the remote so the remote process
    // can use it as the remote_addr parameter
    ucp_mem_attr_t attr;
    attr.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH;
    ucp_mem_query(memh, &attr);
    uint64_t mem_addr = (uint64_t) attr.address;
    if(aligned) assert(mem_addr == (uint64_t)recv_buf);

    // pack rkey_buf
    char* rkey_buf = NULL;
    size_t rkey_buf_size;
    status = ucp_rkey_pack(g_ucp_context, memh, (void**)&rkey_buf, &rkey_buf_size);
    assert(status == UCS_OK && rkey_buf);

    size_t total_buf_size;
    void* sendbuf = pack_request_arg(mem_addr, rkey_buf, rkey_buf_size, user_arg, user_arg_size, &total_buf_size);


    // send it to the other side
    ucp_request_param_t am_param;
    am_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    am_param.flags        = UCP_AM_SEND_FLAG_EAGER;
    int op = OP_RMA_REQUEST;
    void *request = ucp_am_send_nbx(ep, UCX_AM_ID_RMA, &op, sizeof(int), sendbuf, total_buf_size, &am_param);
    wait_request(request);
    free(sendbuf);

    // Wait for the respond
    while(g_ack == false) {
    }

    // Now we should have the data ready
    if(!aligned) {
        memcpy(recv_buf, attr.address, recv_size);
    }

    status = ucp_mem_unmap(g_ucp_context, memh);
    assert(status == UCS_OK);
    request = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
    wait_request(request);
}

void pthread_sent_respond_cb(void *request, ucs_status_t status, void *user_data) {
    ptrs_to_free_t* ptrs = (ptrs_to_free_t*) user_data;
    ucp_rkey_destroy(*(ptrs->rkey_p));
    free(ptrs->rkey_p);
    free(ptrs->rma_data);
    free(ptrs);
    ucp_request_free(request);
}


/**
 * Careful here, this function is invoked upon the
 * RMA request, which means this function is invoked
 * by the pthread ucp_worker_progress().
 *
 * So,
 * - We can not call ucp_worker_progress() inside this function
 * - We can not call wait_request() while will loop forever.
 */
void rma_respond(void* data) {

    int dest_rank;
    uint64_t remote_addr;
    size_t rkey_buf_size, user_arg_size;
    void *rkey_buf, *user_arg;

    int pos = 0;
    memcpy(&dest_rank, data+pos, sizeof(int));
    pos += sizeof(int);

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
    connect_ep(dest_rank, &ep);

    // Get rkey
    ucs_status_t status;
    ucp_rkey_h *rkey_p = malloc(sizeof(ucp_rkey_h));
    status = ucp_ep_rkey_unpack(ep, rkey_buf, rkey_p);
    assert(status == UCS_OK);
    free(rkey_buf);

    // actual RMA
    size_t rma_data_size;
    void* rma_data = g_serve_rma_data(user_arg, &rma_data_size);
    ucp_put_nbi(ep, rma_data, rma_data_size, remote_addr, *rkey_p);

    // Make sure the RMA operation completes before the following ACK message.
    ucp_worker_fence(g_rma_worker);

    // Send ACK
    ptrs_to_free_t *ptrs = malloc(sizeof(ptrs_to_free_t));
    ptrs->rkey_p = rkey_p;
    ptrs->rma_data = rma_data;

    ucp_request_param_t am_param;
    am_param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS    |
                            UCP_OP_ATTR_FIELD_CALLBACK |
                            UCP_OP_ATTR_FIELD_USER_DATA;
    am_param.flags        = UCP_AM_SEND_FLAG_EAGER;
    am_param.cb.send      = pthread_sent_respond_cb;
    am_param.user_data    = ptrs;
    ptrs->op = OP_RMA_RESPOND;      // cant use a stack variable as the send call maybe delayed
    void *request = ucp_am_send_nbx(ep, UCX_AM_ID_RMA, &ptrs->op, sizeof(int), NULL, 0, &am_param);
    // complete in place, the callback will not be called.
    if(request == NULL) {
        ucp_rkey_destroy(*rkey_p);
        free(rkey_p);
        free(rma_data);
        free(ptrs);
    }

    // TODO close ep?
}

// Handler for active messages
static ucs_status_t am_handler(void *arg, const void *header, size_t header_length,
                              void *data, size_t length,
                              const ucp_am_recv_param_t *param) {

    int op = *((int*)header);

    // 1. Received an RMA requeset
    // Need to send data thorugh RMA, and also the respond through AM.
    if(op == OP_RMA_REQUEST) {
        rma_respond(data);
    }else {
        op = OP_RMA_RESPOND;
    }

    // 2. Received the respond for my previous RMA request
    if(op == OP_RMA_RESPOND) {
        g_ack = true;
    }

    return UCS_OK;
}

void exchange_address(void* addr, size_t addr_len) {
    int mpi_size;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    void* all_addrs = malloc(addr_len*mpi_size);
    MPI_Allgather(addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    g_rma_addrs = malloc(mpi_size*sizeof(ucp_address_t*));
    for(int rank = 0; rank < mpi_size; rank++) {
        g_rma_addrs[rank] = malloc(addr_len);
        memcpy(g_rma_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }

    free(all_addrs);
}

void* progress_loop(void* arg) {
    while(g_running) {
        ucp_worker_progress(g_rma_worker);
    }
}

void tangram_ucx_rma_service_start(void* (serve_rma_data)(void*, size_t*)) {

    g_serve_rma_data = serve_rma_data;

    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_rma_worker, false);
    ucp_address_t *addr = NULL;
    size_t addr_len;
    ucs_status_t status = ucp_worker_get_address(g_rma_worker, &addr, &addr_len);
    assert(status == UCS_OK && addr);

    exchange_address(addr, addr_len);

    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID | UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_RMA;
    am_param.cb         = am_handler;
    status              = ucp_worker_set_am_recv_handler(g_rma_worker, &am_param);
    assert(status == UCS_OK);

    pthread_create(&g_progress_thread, NULL, progress_loop, NULL);
}

void tangram_ucx_rma_service_stop() {
    g_running = false;
    pthread_join(g_progress_thread, NULL);

    // Make sure all RMA communications are finished
    worker_flush_from_main_thread();

    int mpi_size, mpi_rank;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_rank);
    MPI_Barrier(MPI_COMM_WORLD);

    // free addrs
    for(int rank = 0; rank < mpi_size; rank++) {
        if(rank == mpi_rank)
            ucp_worker_release_address(g_rma_worker, g_rma_addrs[mpi_rank]);
        else
            free(g_rma_addrs[rank]);
    }
    free(g_rma_addrs);

    ucp_worker_destroy(g_rma_worker);
    ucp_cleanup(g_ucp_context);
}

