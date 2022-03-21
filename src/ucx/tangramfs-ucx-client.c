#define _POSIX_C_SOURCE 200112L
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <sys/time.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

static bool                  g_running = true;
tfs_info_t*                  g_tfs_info;


static ucs_async_context_t*  g_client_async;

static tangram_uct_addr_t*   g_peer_addrs;

/*
 * Listen for incoming AMs from server or peers.
 *
 * Dedicated thread to make progress
 */
static tangram_uct_context_t g_recv_context;
static pthread_t             g_recv_progress_thread;


/*
 * Send AMs and wait for the respond.
 *
 * Only one outgoing AM at a time
 */
static tangram_uct_context_t g_send_context;


/**
 * A seperate context for sending ep addr
 * as a part of rma_respond() call
 *
 * Can not use g_send_context for this as it
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
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_send_context.respond_ptr);

    pthread_mutex_lock(&g_send_context.cond_mutex);
    g_send_context.respond_flag = true;
    pthread_cond_signal(&g_send_context.cond);
    pthread_mutex_unlock(&g_send_context.cond_mutex);
    return UCS_OK;
}

void* handle_revoke_lock_request(void* arg) {
    g_revoke_lock_cb(arg);
    free(arg);

    // Note here we use g_recv_context to send out the respond
    //
    // We can not use g_send_context, as it is used by the main thread
    // only, and it allows only one outgoing RPC at a time:
    //   -- Using it can cause deadlock when acquiring and revoking at the same time
    pthread_mutex_lock(&g_recv_context.mutex);
    uct_ep_h ep;
    uct_ep_create_connect(g_recv_context.iface, &g_recv_context.server_addr, &ep);

    do_uct_am_short_progress(g_recv_context.worker, ep, AM_ID_REVOKE_LOCK_RESPOND, &g_recv_context.self_addr, NULL, 0);

    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_recv_context.mutex);
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
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_send_context.respond_ptr);

    pthread_mutex_lock(&g_send_context.cond_mutex);
    g_send_context.respond_flag = true;
    pthread_cond_signal(&g_send_context.cond);
    pthread_mutex_unlock(&g_send_context.cond_mutex);
    return UCS_OK;
}


void* receiver_progress_loop(void* arg) {
    while(g_running) {
        pthread_mutex_lock(&g_recv_context.mutex);
        uct_worker_progress(g_recv_context.worker);
        pthread_mutex_unlock(&g_recv_context.mutex);
    }
}


/**
 * Send server a RPC request and wait for the respond
 *
 * Used to send post/query request
 */
void tangram_ucx_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr) {

    pthread_mutex_lock(&g_send_context.mutex);
    g_send_context.respond_ptr  = respond_ptr;
    g_send_context.respond_flag = false;

    uct_ep_h ep;
    uct_ep_create_connect(g_send_context.iface, &g_send_context.server_addr, &ep);

    do_uct_am_short_progress(g_send_context.worker, ep, id, &g_recv_context.self_addr, data, length);

    if(respond_ptr != NULL) { // need to wait for respond
        pthread_mutex_lock(&g_send_context.cond_mutex);
        while(!g_send_context.respond_flag)
            pthread_cond_wait(&g_send_context.cond, &g_send_context.cond_mutex);
        pthread_mutex_unlock(&g_send_context.cond_mutex);
    }

    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_send_context.mutex);
}

/**
 * Send to a client an AM and wait for the respond
 *
 * Used to send RMA request
 */
void tangram_ucx_sendrecv_client(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    pthread_mutex_lock(&g_send_context.mutex);
    g_send_context.respond_ptr  = respond_ptr;
    g_send_context.respond_flag = false;

    uct_ep_h ep;
    uct_ep_create_connect(g_send_context.iface, dest, &ep);

    do_uct_am_short_progress(g_send_context.worker, ep, id, &g_recv_context.self_addr, data, length);

    // if need to wait for a respond
    if(respond_ptr != NULL) {
        pthread_mutex_lock(&g_send_context.cond_mutex);
        while(!g_send_context.respond_flag)
            pthread_cond_wait(&g_send_context.cond, &g_send_context.cond_mutex);
        pthread_mutex_unlock(&g_send_context.cond_mutex);
    }

    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_send_context.mutex);
}

void tangram_ucx_send_ep_addr(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length) {
    pthread_mutex_lock(&g_epaddr_context.mutex);

    uct_ep_h ep;
    uct_ep_create_connect(g_epaddr_context.iface, dest, &ep);

    do_uct_am_short_progress(g_epaddr_context.worker, ep, id, &g_recv_context.self_addr, data, length);

    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_epaddr_context.mutex);
}


/**
 * Send server a short AM that contains only the header
 * Do not require a respond from server
 *
 * This is used to send server the mpi size, and the stop server signal
 */
void tangram_ucx_send_server(uint8_t id) {
    pthread_mutex_lock(&g_send_context.mutex);

    uct_ep_h ep;
    uct_ep_create_connect(g_send_context.iface, &g_send_context.server_addr, &ep);

    do_uct_am_short_progress(g_send_context.worker, ep, id, &g_recv_context.self_addr, NULL, 0);
    uct_ep_destroy(ep);

    pthread_mutex_unlock(&g_send_context.mutex);
}

void tangram_ucx_stop_server() {
    tangram_ucx_send_server(AM_ID_STOP_REQUEST);
}

void tangram_ucx_rpc_service_start(tfs_info_t *tfs_info, void (revoke_lock_cb)(void*)) {
    g_tfs_info = tfs_info;

    g_revoke_lock_cb = revoke_lock_cb;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_client_async);

    tangram_uct_context_init(g_client_async, g_tfs_info->rpc_dev_name, g_tfs_info->rpc_tl_name, false, &g_send_context);
    tangram_uct_context_init(g_client_async, g_tfs_info->rpc_dev_name, g_tfs_info->rpc_tl_name, false, &g_recv_context);
    tangram_uct_context_init(g_client_async, g_tfs_info->rpc_dev_name, g_tfs_info->rpc_tl_name, false, &g_epaddr_context);

    g_peer_addrs = malloc(g_tfs_info->mpi_size * sizeof(tangram_uct_addr_t));
    exchange_dev_iface_addr(&g_recv_context, g_peer_addrs);

    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_QUERY_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_POST_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_STAT_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_ACQUIRE_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_RELEASE_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_RELEASE_ALL_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_REVOKE_LOCK_REQUEST, am_revoke_lock_request_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_RMA_REQUEST, am_rma_request_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_recv_context.iface, AM_ID_RMA_EP_ADDR, am_ep_addr_listener, NULL, 0);
    assert(status == UCS_OK);

    pthread_create(&g_recv_progress_thread, NULL, receiver_progress_loop, NULL);
    MPI_Barrier(g_tfs_info->mpi_comm);
}


void tangram_ucx_rpc_service_stop() {
    ucs_status_t status = uct_iface_flush(g_send_context.iface, UCT_FLUSH_FLAG_LOCAL, NULL);
    assert(status == UCS_OK);

    MPI_Barrier(g_tfs_info->mpi_comm);
    g_running = false;
    pthread_join(g_recv_progress_thread, NULL);

    tangram_uct_context_destroy(&g_send_context);
    tangram_uct_context_destroy(&g_epaddr_context);
    tangram_uct_context_destroy(&g_recv_context);
    ucs_async_context_destroy(g_client_async);
}

tangram_uct_addr_t* tangram_ucx_get_client_addr() {
    return &g_recv_context.self_addr;
}

size_t tangram_uct_am_short_max_size() {
    return g_send_context.iface_attr.cap.am.max_short;
}

