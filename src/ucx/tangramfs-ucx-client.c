#define _POSIX_C_SOURCE 200112L
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include "tangramfs-ucx-rma.h"
#include "tangramfs-ucx-client.h"
#include "tangramfs-ucx-delegator.h"

static tfs_info_t*           g_tfs_info;
static ucs_async_context_t*  g_client_async;


/*
 * Send AMs to node-local delegator and wait for the respond.
 *
 * Only one outgoing AM at a time, use intra-node device (share memory)
 */
static tangram_uct_context_t g_client_intra_context;


/**
 * A seperate context for sending ep addr
 * as a part of rma_respond() call
 *
 * Can not use g_client_intra_context for this as it
 * potentially causes deadlock.
 * e.g., rank 0 -> rma request -> rank 1
 *       rank 1 -> rma request -> rank 0
 * both block on sendrecv_client and wait for dest's ep addr.
 */
static tangram_uct_context_t g_client_inter_context;


static uct_ep_h g_ep_delegator;
static uct_ep_h g_ep_server;


/**
 * Handles both query and post respond from server
 */
static ucs_status_t am_inter_respond_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    uint64_t seq_id;
    unpack_rpc_buffer(buf, buf_len, &seq_id, TANGRAM_UCT_ADDR_IGNORE, g_client_inter_context.respond_ptr);
    g_client_inter_context.respond_flag = true;
    return UCS_OK;
}

static ucs_status_t am_intra_respond_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    uint64_t seq_id;
    unpack_rpc_buffer(buf, buf_len, &seq_id, TANGRAM_UCT_ADDR_IGNORE, g_client_intra_context.respond_ptr);
    g_client_intra_context.respond_flag = true;
    return UCS_OK;
}

/**
 * Send a RPC request and wait for the respond
 * This is the core function for ucx communications
 */
void client_sendrecv_core(uint8_t id, tangram_uct_context_t* context, uct_ep_h ep, void* data, size_t length, void** respond_ptr) {
    context->respond_ptr  = respond_ptr;
    context->respond_flag = false;

    do_uct_am_short_progress(context->worker, ep, id, 0, &context->self_addr, data, length);

    // if need to wait for a respond
    if(respond_ptr != NULL) {
        while(!context->respond_flag) {
            uct_worker_progress(context->worker);
        }
    }
}

void sendrecv_inter(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    uct_ep_h ep;
    uct_ep_create_connect(g_client_inter_context.iface, dest, &ep);
    client_sendrecv_core(id, &g_client_inter_context, ep, data, length, respond_ptr);
    uct_ep_destroy(ep);
}

void tangram_ucx_sendrecv_client(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    sendrecv_inter(id, dest, data, length, respond_ptr);
}

void tangram_ucx_send_ep_addr(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length) {
    sendrecv_inter(id, dest, data, length, NULL);
}

void tangram_ucx_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr) {
    client_sendrecv_core(id, &g_client_inter_context, g_ep_server, data, length, respond_ptr);
}

void tangram_ucx_sendrecv_delegator(uint8_t id, void* data, size_t length, void** respond_ptr) {
    client_sendrecv_core(id, &g_client_intra_context, g_ep_delegator, data, length, respond_ptr);
}


/**
 * Send server a short AM that contains only the header
 * Do not require a respond from server
 *
 * Used to send the stop server signal
 */
void tangram_ucx_stop_delegator() {
    tangram_ucx_sendrecv_delegator(AM_ID_STOP_REQUEST, NULL, 0, NULL);
}

void tangram_ucx_stop_server() {
    tangram_ucx_sendrecv_server(AM_ID_STOP_REQUEST, NULL, 0, NULL);
}

void set_delegator_intra_addr(tangram_uct_context_t* context) {
    // Broadcast local server address
    void* buf;
    size_t len;

    if(g_tfs_info->mpi_intra_rank == 0)
        buf = tangram_uct_addr_serialize(tangram_ucx_delegator_intra_addr(), &len);

    // Get delegator address length first
    MPI_Bcast(&len, sizeof(len), MPI_BYTE, 0, g_tfs_info->mpi_intra_comm);
    if(g_tfs_info->mpi_intra_rank != 0)
        buf = malloc(len);

    // Bcast delegator adderss
    MPI_Bcast(buf, len, MPI_BYTE, 0, g_tfs_info->mpi_intra_comm);

    tangram_uct_addr_deserialize(buf, &context->delegator_addr);
    free(buf);
}

void tangram_ucx_client_start(tfs_info_t *tfs_info) {
    g_tfs_info = tfs_info;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_client_async);

    tangram_uct_context_init(g_client_async, g_tfs_info, true,  &g_client_intra_context);
    tangram_uct_context_init(g_client_async, g_tfs_info, false, &g_client_inter_context);
    if(tfs_info->use_delegator) {
        set_delegator_intra_addr(&g_client_intra_context);
        uct_ep_create_connect(g_client_intra_context.iface, &g_client_intra_context.delegator_addr, &g_ep_delegator);
    }
    uct_ep_create_connect(g_client_inter_context.iface, &g_client_inter_context.server_addr, &g_ep_server);

    // Communicatinos between global server, use inter_context
    uct_iface_set_am_handler(g_client_inter_context.iface, AM_ID_QUERY_RESPOND, am_inter_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_inter_context.iface, AM_ID_POST_RESPOND, am_inter_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_inter_context.iface, AM_ID_UNPOST_FILE_RESPOND, am_inter_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_inter_context.iface, AM_ID_UNPOST_CLIENT_RESPOND, am_inter_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_inter_context.iface, AM_ID_STAT_RESPOND, am_inter_respond_listener, NULL, 0);

    // Communications between node-local delegator, use intra_context
    uct_iface_set_am_handler(g_client_intra_context.iface, AM_ID_ACQUIRE_LOCK_RESPOND, am_intra_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_intra_context.iface, AM_ID_RELEASE_LOCK_RESPOND, am_intra_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_intra_context.iface, AM_ID_RELEASE_LOCK_FILE_RESPOND, am_intra_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_client_intra_context.iface, AM_ID_RELEASE_LOCK_CLIENT_RESPOND, am_intra_respond_listener, NULL, 0);
}

void tangram_ucx_client_stop() {
    ucs_status_t status = uct_iface_flush(g_client_intra_context.iface, UCT_FLUSH_FLAG_LOCAL, NULL);
    tangram_assert(status == UCS_OK);

    if(g_tfs_info->use_delegator)
        uct_ep_destroy(g_ep_delegator);
    uct_ep_destroy(g_ep_server);

    MPI_Barrier(g_tfs_info->mpi_comm);

    tangram_uct_context_destroy(&g_client_intra_context);
    tangram_uct_context_destroy(&g_client_inter_context);
    ucs_async_context_destroy(g_client_async);
}

tangram_uct_addr_t* tangram_ucx_client_inter_addr() {
    return &g_client_inter_context.self_addr;
}

#define MIN(a,b) (((a)<(b))?(a):(b))

size_t tangram_uct_am_short_max_size() {
    if(g_tfs_info->use_delegator)
        return MIN(g_client_inter_context.iface_attr.cap.am.max_short, g_client_intra_context.iface_attr.cap.am.max_short);
    else
        return g_client_inter_context.iface_attr.cap.am.max_short;
}

