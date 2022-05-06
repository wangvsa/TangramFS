#define _POSIX_C_SOURCE 200112L
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <pthread.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

static tfs_info_t*           g_tfs_info;
static ucs_async_context_t*  g_client_async;


/*
 * Send AMs and wait for the respond.
 *
 * Only one outgoing AM at a time
 */
static tangram_uct_context_t g_client_context;


/**
 * A seperate context for sending ep addr
 * as a part of rma_respond() call
 *
 * Can not use g_client_context for this as it
 * potentially causes deadlock.
 * e.g., rank 0 -> rma request -> rank 1
 *       rank 1 -> rma request -> rank 0
 * both block on sendrecv_client and wait for dest's ep addr.
 */
static tangram_uct_context_t g_epaddr_context;


void (*g_revoke_lock_cb)(void*);



/**
 * Handles both query and post respond from server
 */
static ucs_status_t am_server_respond_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_client_context.respond_ptr);
    g_client_context.respond_flag = true;
    return UCS_OK;
}

void* handle_revoke_lock_request(void* arg) {
    g_revoke_lock_cb(arg);
    free(arg);

    // Note here we use g_client_context to send out the respond
    //
    // We can not use g_client_context, as it is used by the main thread
    // only, and it allows only one outgoing RPC at a time:
    //   -- Using it can cause deadlock when acquiring and revoking at the same time
    uct_ep_h ep;
    uct_ep_create_connect(g_client_context.iface, &g_client_context.delegator_addr, &ep);
    do_uct_am_short_progress(g_client_context.worker, ep, AM_ID_REVOKE_LOCK_RESPOND, &g_client_context.self_addr, NULL, 0);
    uct_ep_destroy(ep);
    return NULL;
}

static ucs_status_t am_revoke_lock_request_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    void* data;
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, &data);
    pthread_t thread;
    pthread_create(&thread, NULL, handle_revoke_lock_request, data);
    return UCS_OK;
}

/**
 * Note this am handler func is called by pthread-progress funciton.
 * We can not do am_send here as no one will be performing the progress
 * We need to create a new thread to handle this rma request
 */
static ucs_status_t am_rma_request_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    tangram_rma_req_in_t *arg_in = malloc(sizeof(tangram_rma_req_in_t));
    unpack_rpc_buffer(buf, buf_len, &arg_in->src, &arg_in->data);
    pthread_t thread;
    pthread_create(&thread, NULL, rma_respond, arg_in);
    return UCS_OK;
}

static ucs_status_t am_ep_addr_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_client_context.respond_ptr);
    g_client_context.respond_flag = true;
    return UCS_OK;
}


/**
 * Send a RPC request and wait for the respond
 * This is the core function for ucx communications
 */
void sendrecv(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    g_client_context.respond_ptr  = respond_ptr;
    g_client_context.respond_flag = false;

    uct_ep_h ep;
    uct_ep_create_connect(g_client_context.iface, dest, &ep);

    do_uct_am_short_progress(g_client_context.worker, ep, id, &g_client_context.self_addr, data, length);

    // if need to wait for a respond
    if(respond_ptr != NULL) {
        while(!g_client_context.respond_flag) {
            uct_worker_progress(g_client_context.worker);
        }
    }

    uct_ep_destroy(ep);
}

void tangram_ucx_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr) {
    sendrecv(id, &g_client_context.server_addr, data, length, respond_ptr);
}

void tangram_ucx_sendrecv_delegator(uint8_t id, void* data, size_t length, void** respond_ptr) {
    sendrecv(id, &g_client_context.delegator_addr, data, length, respond_ptr);
}

void tangram_ucx_sendrecv_client(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    sendrecv(id, dest, data, length, respond_ptr);
}

void tangram_ucx_send_ep_addr(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length) {
    sendrecv(id, dest, data, length, NULL);
}


/**
 * Send server a short AM that contains only the header
 * Do not require a respond from server
 *
 * Used to send the stop server signal
 */
void tangram_ucx_stop_delegator() {
    sendrecv(AM_ID_STOP_REQUEST, &g_client_context.delegator_addr, NULL, 0, NULL);
}

void tangram_ucx_stop_server() {
    sendrecv(AM_ID_STOP_REQUEST, &g_client_context.server_addr, NULL, 0, NULL);
}

void set_delegator_addr(tangram_uct_context_t* context) {
    // Broadcast local server address
    void* buf;
    size_t len;
    if(g_tfs_info->mpi_intra_rank == 0)
        buf = tangram_uct_addr_serialize(tangram_ucx_server_addr(), &len);
    else
        buf = tangram_uct_addr_serialize(&context->self_addr, &len);
    MPI_Bcast(buf, len, MPI_BYTE, 0, g_tfs_info->mpi_intra_comm);
    tangram_uct_addr_deserialize(buf, &context->delegator_addr);
    free(buf);
}

void tangram_ucx_client_start(tfs_info_t *tfs_info, void (revoke_lock_cb)(void*)) {
    g_tfs_info = tfs_info;

    g_revoke_lock_cb = revoke_lock_cb;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_client_async);

    tangram_uct_context_init(g_client_async, g_tfs_info, &g_client_context);
    tangram_uct_context_init(g_client_async, g_tfs_info, &g_epaddr_context);
    if(tfs_info->use_delegator) {
        set_delegator_addr(&g_client_context);
        set_delegator_addr(&g_epaddr_context);
    }

    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_QUERY_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_POST_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_UNPOST_FILE_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_UNPOST_CLIENT_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_STAT_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_ACQUIRE_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_RELEASE_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_RELEASE_LOCK_FILE_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_RELEASE_LOCK_CLIENT_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_REVOKE_LOCK_REQUEST, am_revoke_lock_request_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_RMA_REQUEST, am_rma_request_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_context.iface, AM_ID_RMA_EP_ADDR, am_ep_addr_listener, NULL, 0);
    assert(status == UCS_OK);
}

void tangram_ucx_client_stop() {
    ucs_status_t status = uct_iface_flush(g_client_context.iface, UCT_FLUSH_FLAG_LOCAL, NULL);
    assert(status == UCS_OK);

    MPI_Barrier(g_tfs_info->mpi_comm);

    tangram_uct_context_destroy(&g_client_context);
    tangram_uct_context_destroy(&g_epaddr_context);
    ucs_async_context_destroy(g_client_async);
}

tangram_uct_addr_t* tangram_ucx_client_addr() {
    return &g_client_context.self_addr;
}

size_t tangram_uct_am_short_max_size() {
    return g_client_context.iface_attr.cap.am.max_short;
}

