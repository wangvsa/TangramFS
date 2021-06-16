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

static pthread_t g_progress_thread;


// Main thread wait for a request to finsih
// Since we have a dedicate pthread doing progress
// Here we only need to check the request status
void wait_request(void* request) {
    ucs_status_t status;
    do {
        status = ucp_request_check_status(request);
    } while(status == UCS_INPROGRESS);
    assert(status == UCS_OK);
}


void connect_ep(int rank, ucp_ep_h *ep) {
    ucs_status_t status;
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = g_rma_addrs[rank];
    status = ucp_ep_create(g_rma_worker, &ep_params, ep);
    assert(status == UCS_OK);
}

/** Send a RMA requeset
 * (1) connect
 * (2) register memory
 * (3) send my rkey
 * (4) wait for ack
 */
void rma_request(int rank) {

    ucp_ep_h ep;
    connect_ep(rank, &ep);

    ucp_mem_map_params_t params;
    params.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS | UCP_MEM_MAP_PARAM_FIELD_LENGTH | UCP_MEM_MAP_PARAM_FIELD_FLAGS;
    params.address = NULL;                              // Let UCP allocate memory for us
    params.length = 4096;
    params.flags = UCP_MEM_MAP_ALLOCATE;

    // mem map and let UCX allocate memory
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

    // pack rkey_buf
    char* rkey_buf = NULL;
    size_t rkey_buf_size;
    status = ucp_rkey_pack(g_ucp_context, memh, (void**)&rkey_buf, &rkey_buf_size);
    assert(status == UCS_OK && rkey_buf);
    printf("rkey_buf length: %lu, mem ptr: %p, mem len: %lu\n", rkey_buf_size, attr.address, attr.length);

    size_t total_buf_size = sizeof(int) + sizeof(size_t) + rkey_buf_size + sizeof(uint64_t);
    void* sendbuf = malloc(total_buf_size);
    memcpy(sendbuf, &rank, sizeof(int));
    memcpy(sendbuf+sizeof(int), &rkey_buf_size, sizeof(size_t));
    memcpy(sendbuf+sizeof(int)+sizeof(size_t), rkey_buf, rkey_buf_size);
    memcpy(sendbuf+sizeof(int)+sizeof(size_t)+rkey_buf_size, &mem_addr, sizeof(uint64_t));

    // send it to the other side
    g_ack = 0;
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    int op = OP_RMA_REQUEST;
    void *request = ucp_am_send_nbx(ep, UCX_AM_ID_RMA, &op, sizeof(int), sendbuf, total_buf_size, &am_params);
    wait_request(request);
    free(sendbuf);

    while(!g_ack) {
    }

    // TODO unmap
    request = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
    wait_request(request);
}

void rma_respond(void* data) {

    int dest_rank;
    size_t rkey_buf_size;
    char* rkey_buf;
    uint64_t remote_addr;

    memcpy(&dest_rank, data, sizeof(int));
    memcpy(&rkey_buf_size, data+sizeof(int), sizeof(size_t));
    rkey_buf = malloc(rkey_buf_size);
    memcpy(rkey_buf, data+sizeof(int)+sizeof(size_t), rkey_buf_size);
    memcpy(&remote_addr, data+sizeof(int)+sizeof(size_t)+rkey_buf_size, sizeof(uint64_t));
    printf("rkey_buf_size: %lu, remote addr: %p\n", rkey_buf_size, (void*)remote_addr);

    ucp_ep_h ep;
    connect_ep(dest_rank, &ep);

    ucs_status_t status;
    ucp_rkey_h rkey;
    status = ucp_ep_rkey_unpack(ep, rkey_buf, &rkey);
    assert(status == UCS_OK);

    // actual RMA
    int local_data = 100;
    ucp_put_nbi(ep, &local_data, sizeof(int), remote_addr, rkey);

    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    int op = OP_RMA_RESPOND;
    void *request = ucp_am_send_nbx(ep, UCX_AM_ID_RMA, &op, sizeof(int), NULL, 0, &am_params);
    wait_request(request);

    free(rkey_buf);
    ucp_rkey_destroy(rkey);
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

void tangram_ucx_rma_service_start() {
    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_rma_worker, false);
    ucp_address_t *addr = NULL;
    size_t addr_len;
    ucs_status_t status = ucp_worker_get_address(g_rma_worker, &addr, &addr_len);
    assert(status == UCS_OK && addr);

    exchange_address(addr, addr_len);

    // Set am callback to receive respond from server
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
    worker_flush(g_rma_worker);

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

