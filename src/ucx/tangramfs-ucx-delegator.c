#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include "utlist.h"
#include "tangramfs-ucx-delegator.h"
#include "tangramfs-ucx-taskmgr.h"

#define NUM_THREADS 1

static tfs_info_t*           g_tfs_info;
static taskmgr_t             g_taskmgr;

volatile static bool         g_delegator_running = true;
static ucs_async_context_t*  g_delegator_async;
static tangram_uct_context_t g_delegator_context;

void* (*user_am_data_handler)(int8_t, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len);

/*
 * Use one global mutex to protect revoke rpcs
 *
 * We wait on g_delegator_context.cond, so it is
 * necessary that we have at most one outgoing revoke
 * request at a time.
 * Wait on multiple clients at the same time may cause deadlock
 *
 * TODO we can implement it in a way without this global lock.
 */
static pthread_mutex_t       g_revoke_lock_mutex = PTHREAD_MUTEX_INITIALIZER;

static ucs_status_t am_acquire_lock_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task_to_worker(&g_taskmgr, AM_ID_ACQUIRE_LOCK_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_release_lock_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task_to_worker(&g_taskmgr, AM_ID_RELEASE_LOCK_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_release_lock_file_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task_to_worker(&g_taskmgr, AM_ID_RELEASE_LOCK_FILE_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_release_lock_client_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task_to_worker(&g_taskmgr, AM_ID_RELEASE_LOCK_CLIENT_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_revoke_lock_request_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task_to_worker(&g_taskmgr, AM_ID_REVOKE_LOCK_REQUEST, buf, buf_len, 0);
    char hostname[128];
    gethostname(hostname, 128);
    printf("CHEN delegator %s received revoke lock request\n", hostname);
    return UCS_OK;
}
static ucs_status_t am_server_respond_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    pthread_mutex_lock(&g_delegator_context.cond_mutex);
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_delegator_context.respond_ptr);
    g_delegator_context.respond_flag = true;
    pthread_cond_signal(&g_delegator_context.cond);
    pthread_mutex_unlock(&g_delegator_context.cond_mutex);
    return UCS_OK;
}

static ucs_status_t am_stop_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    g_delegator_running = false;
    return UCS_OK;
}

void delegator_handle_task(task_t* task) {
    pthread_mutex_lock(&g_delegator_context.mutex);
    uct_ep_h ep;
    uct_ep_create_connect(g_delegator_context.iface, &task->client, &ep);
    pthread_mutex_unlock(&g_delegator_context.mutex);

    task->respond = (*user_am_data_handler)(task->id, &task->client, task->data, &task->id, &task->respond_len);
    do_uct_am_short(&g_delegator_context.mutex, ep, task->id, &g_delegator_context.self_addr, task->respond, task->respond_len);

    pthread_mutex_lock(&g_delegator_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_delegator_context.mutex);
}

void tangram_ucx_revoke_lock2(tangram_uct_addr_t* client, void* data, size_t length) {
    pthread_mutex_lock(&g_revoke_lock_mutex);

    g_delegator_context.respond_flag = false;

    pthread_mutex_lock(&g_delegator_context.mutex);
    uct_ep_h ep;
    uct_ep_create_connect(g_delegator_context.iface, client, &ep);
    pthread_mutex_unlock(&g_delegator_context.mutex);

    // this call will lock the mutext itself
    do_uct_am_short(&g_delegator_context.mutex, ep, AM_ID_REVOKE_LOCK_REQUEST, &g_delegator_context.self_addr, data, length);

    pthread_mutex_lock(&g_delegator_context.cond_mutex);
    while(!g_delegator_context.respond_flag)
        pthread_cond_wait(&g_delegator_context.cond, &g_delegator_context.cond_mutex);
    pthread_mutex_unlock(&g_delegator_context.cond_mutex);

    pthread_mutex_lock(&g_delegator_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_delegator_context.mutex);

    pthread_mutex_unlock(&g_revoke_lock_mutex);
}

void tangram_ucx_delegator_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr) {
    pthread_mutex_lock(&g_delegator_context.mutex);
    g_delegator_context.respond_flag = false;
    g_delegator_context.respond_ptr  = respond_ptr;
    uct_ep_h ep;
    uct_ep_create_connect(g_delegator_context.iface, &g_delegator_context.server_addr, &ep);
    pthread_mutex_unlock(&g_delegator_context.mutex);

    // this call will lock the mutext itself
    do_uct_am_short(&g_delegator_context.mutex, ep, id, &g_delegator_context.self_addr, data, length);

    // wait for server respond
    if(respond_ptr != NULL) {
        pthread_mutex_lock(&g_delegator_context.cond_mutex);
        while(!g_delegator_context.respond_flag)
            pthread_cond_wait(&g_delegator_context.cond, &g_delegator_context.cond_mutex);
        pthread_mutex_unlock(&g_delegator_context.cond_mutex);
    }

    pthread_mutex_lock(&g_delegator_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_delegator_context.mutex);
}

void tangram_ucx_delegator_init(tfs_info_t *tfs_info) {
    g_tfs_info = tfs_info;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_delegator_async);

    tangram_uct_context_init(g_delegator_async, tfs_info, &g_delegator_context);

    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_ACQUIRE_LOCK_REQUEST, am_acquire_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_RELEASE_LOCK_REQUEST, am_release_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_RELEASE_LOCK_FILE_REQUEST, am_release_lock_file_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_RELEASE_LOCK_CLIENT_REQUEST, am_release_lock_client_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_REVOKE_LOCK_REQUEST, am_revoke_lock_request_listener, NULL, 0);

    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_ACQUIRE_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    /*
     * Not used for now. delegator does not send those requests to server for now.
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_RELEASE_LOCK_RESPOND, am_server_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_RELEASE_LOCK_FILE_RESPOND, am_server_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_RELEASE_LOCK_CLIENT_RESPOND, am_server_respond_listener, NULL, 0);
    */

    uct_iface_set_am_handler(g_delegator_context.iface, AM_ID_STOP_REQUEST, am_stop_listener, NULL, 0);

    taskmgr_init(&g_taskmgr, 1, delegator_handle_task);
}

void tangram_ucx_delegator_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*)) {
    user_am_data_handler = user_handler;
}

void* delegator_progress_loop(void* arg) {
    while(g_delegator_running) {
        pthread_mutex_lock(&g_delegator_context.mutex);
        uct_worker_progress(g_delegator_context.worker);
        pthread_mutex_unlock(&g_delegator_context.mutex);
    }

    return NULL;
}

void tangram_ucx_delegator_start() {
    pthread_t thread;
    pthread_create(&thread, NULL, delegator_progress_loop, NULL);
}

void tangram_ucx_delegator_stop() {

    while(g_delegator_running) {
        printf("Delegator still running...");
        sleep(1);
    }

    taskmgr_finalize(&g_taskmgr);
    tangram_uct_context_destroy(&g_delegator_context);
    ucs_async_context_destroy(g_delegator_async);
}

tangram_uct_addr_t* tangram_ucx_delegator_addr() {
    return &g_delegator_context.self_addr;
}
