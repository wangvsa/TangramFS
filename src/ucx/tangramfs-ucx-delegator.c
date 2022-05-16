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


// intra-node communication with node-local clients, use share memory
static tangram_uct_context_t g_delegator_intra_context;

// inter-node communication with server and other delegators, use network
static tangram_uct_context_t g_delegator_inter_context;


void* (*delegator_am_handler)(int8_t, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len);

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
static ucs_status_t am_split_lock_request_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task_to_worker(&g_taskmgr, AM_ID_SPLIT_LOCK_REQUEST, buf, buf_len, 0);
    return UCS_OK;
}
static ucs_status_t am_respond_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    pthread_mutex_lock(&g_delegator_inter_context.cond_mutex);
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_delegator_inter_context.respond_ptr);
    g_delegator_inter_context.respond_flag = true;
    pthread_cond_signal(&g_delegator_inter_context.cond);
    pthread_mutex_unlock(&g_delegator_inter_context.cond_mutex);
    return UCS_OK;
}

static ucs_status_t am_stop_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    g_delegator_running = false;
    return UCS_OK;
}

// Handle node-local clients RPC tasks, use intra_context
void delegator_handle_client_task(task_t* task) {
    pthread_mutex_lock(&g_delegator_intra_context.mutex);
    uct_ep_h ep;
    uct_ep_create_connect(g_delegator_intra_context.iface, &task->client, &ep);
    pthread_mutex_unlock(&g_delegator_intra_context.mutex);

    task->respond = (*delegator_am_handler)(task->id, &task->client, task->data, &task->id, &task->respond_len);
    do_uct_am_short(&g_delegator_intra_context.mutex, ep, task->id, &g_delegator_intra_context.self_addr, task->respond, task->respond_len);

    pthread_mutex_lock(&g_delegator_intra_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_delegator_intra_context.mutex);
}

// Send RCP and receive respond from server and other delegators, use inter_context
void delegator_sendrecv_inter(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    pthread_mutex_lock(&g_delegator_inter_context.mutex);
    g_delegator_inter_context.respond_flag = false;
    g_delegator_inter_context.respond_ptr  = respond_ptr;
    uct_ep_h ep;
    uct_ep_create_connect(g_delegator_inter_context.iface, dest, &ep);
    pthread_mutex_unlock(&g_delegator_inter_context.mutex);

    // this call will lock the mutext itself
    do_uct_am_short(&g_delegator_inter_context.mutex, ep, id, &g_delegator_inter_context.self_addr, data, length);

    // wait for respond
    if(respond_ptr != NULL) {
        pthread_mutex_lock(&g_delegator_inter_context.cond_mutex);
        while(!g_delegator_inter_context.respond_flag)
            pthread_cond_wait(&g_delegator_inter_context.cond, &g_delegator_inter_context.cond_mutex);
        pthread_mutex_unlock(&g_delegator_inter_context.cond_mutex);
    }

    pthread_mutex_lock(&g_delegator_inter_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_delegator_inter_context.mutex);
}

void tangram_ucx_delegator_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr) {
    delegator_sendrecv_inter(id, &g_delegator_inter_context.server_addr, data, length, respond_ptr);
}

void tangram_ucx_delegator_sendrecv_delegator(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    delegator_sendrecv_inter(id, dest, data, length, respond_ptr);
}

void tangram_ucx_delegator_init(tfs_info_t *tfs_info) {
    g_tfs_info = tfs_info;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_delegator_async);

    tangram_uct_context_init(g_delegator_async, tfs_info, true, &g_delegator_intra_context);
    tangram_uct_context_init(g_delegator_async, tfs_info, false, &g_delegator_inter_context);


    // From node-local clients, use intra_context
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_ACQUIRE_LOCK_REQUEST, am_acquire_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_RELEASE_LOCK_REQUEST, am_release_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_RELEASE_LOCK_FILE_REQUEST, am_release_lock_file_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_RELEASE_LOCK_CLIENT_REQUEST, am_release_lock_client_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_intra_context.iface, AM_ID_STOP_REQUEST, am_stop_listener, NULL, 0);

    // From other delegators, use inter_context
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_SPLIT_LOCK_REQUEST, am_split_lock_request_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_SPLIT_LOCK_RESPOND, am_respond_listener, NULL, 0);

    // From server, respond to our acquire_lock and release_lock request
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_ACQUIRE_LOCK_RESPOND, am_respond_listener, NULL, 0);
    uct_iface_set_am_handler(g_delegator_inter_context.iface, AM_ID_RELEASE_LOCK_RESPOND, am_respond_listener, NULL, 0);


    taskmgr_init(&g_taskmgr, 1, delegator_handle_client_task);
}

void tangram_ucx_delegator_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*)) {
    delegator_am_handler = user_handler;
}

void* delegator_progress_loop(void* arg) {
    while(g_delegator_running) {
        pthread_mutex_lock(&g_delegator_intra_context.mutex);
        uct_worker_progress(g_delegator_intra_context.worker);
        pthread_mutex_unlock(&g_delegator_intra_context.mutex);

        pthread_mutex_lock(&g_delegator_inter_context.mutex);
        uct_worker_progress(g_delegator_inter_context.worker);
        pthread_mutex_unlock(&g_delegator_inter_context.mutex);
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
    tangram_uct_context_destroy(&g_delegator_intra_context);
    tangram_uct_context_destroy(&g_delegator_inter_context);
    ucs_async_context_destroy(g_delegator_async);
}

tangram_uct_addr_t* tangram_ucx_delegator_intra_addr() {
    return &g_delegator_intra_context.self_addr;
}
tangram_uct_addr_t* tangram_ucx_delegator_inter_addr() {
    return &g_delegator_inter_context.self_addr;
}
