#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include "utlist.h"
#include "tangramfs-ucx-server.h"
#include "tangramfs-ucx-taskmgr.h"

static tfs_info_t*           g_tfs_info;
static taskmgr_t             g_taskmgr;

volatile static bool         g_server_running = true;
static ucs_async_context_t*  g_server_async;
static tangram_uct_context_t g_server_context;


void* (*server_am_handler)(int8_t, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len);

static ucs_status_t am_query_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    // TODO can directly use the data and return UCS_INPROGRESS
    // then free it later.
    taskmgr_append_task(&g_taskmgr, AM_ID_QUERY_REQUEST, buf, buf_len);
    return UCS_OK;
}
static ucs_status_t am_post_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task(&g_taskmgr, AM_ID_POST_REQUEST, buf, buf_len);
    return UCS_OK;
}
static ucs_status_t am_unpost_file_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task(&g_taskmgr, AM_ID_UNPOST_FILE_REQUEST, buf, buf_len);
    return UCS_OK;
}
static ucs_status_t am_unpost_client_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task(&g_taskmgr, AM_ID_UNPOST_CLIENT_REQUEST, buf, buf_len);
    return UCS_OK;
}
static ucs_status_t am_stat_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    taskmgr_append_task(&g_taskmgr, AM_ID_STAT_REQUEST, buf, buf_len);
    return UCS_OK;
}
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

static ucs_status_t am_stop_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    // TODO server.c need to be notified
    //taskmgr_append_task_to_worker(AM_ID_STOP_REQUEST, buf, buf_len, 0);
    g_server_running = false;
    return UCS_OK;
}

void server_handle_task(task_t* task) {
    pthread_mutex_lock(&g_server_context.mutex);
    uct_ep_h ep;
    uct_ep_create_connect(g_server_context.iface, &task->client, &ep);
    pthread_mutex_unlock(&g_server_context.mutex);

    task->respond = (*server_am_handler)(task->id, &task->client, task->data, &task->id, &task->respond_len);
    do_uct_am_short_lock(&g_server_context.mutex, ep, task->id, task->seq_id, &g_server_context.self_addr, task->respond, task->respond_len);

    pthread_mutex_lock(&g_server_context.mutex);
    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_server_context.mutex);
}

void tangram_ucx_server_init(tfs_info_t *tfs_info) {
    g_tfs_info = tfs_info;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_server_async);

    tangram_uct_context_init(g_server_async, tfs_info, false, &g_server_context);

    uct_iface_set_am_handler(g_server_context.iface, AM_ID_QUERY_REQUEST, am_query_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_POST_REQUEST, am_post_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_UNPOST_FILE_REQUEST, am_unpost_file_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_UNPOST_CLIENT_REQUEST, am_unpost_client_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_STAT_REQUEST, am_stat_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_ACQUIRE_LOCK_REQUEST, am_acquire_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_RELEASE_LOCK_REQUEST, am_release_lock_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_RELEASE_LOCK_FILE_REQUEST, am_release_lock_file_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_RELEASE_LOCK_CLIENT_REQUEST, am_release_lock_client_listener, NULL, 0);
    uct_iface_set_am_handler(g_server_context.iface, AM_ID_STOP_REQUEST, am_stop_listener, NULL, 0);

    taskmgr_init(&g_taskmgr, 8, server_handle_task);
}

void tangram_ucx_server_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*)) {
    server_am_handler = user_handler;
}


void tangram_ucx_server_start() {
    while(g_server_running) {
        pthread_mutex_lock(&g_server_context.mutex);
        uct_worker_progress(g_server_context.worker);
        pthread_mutex_unlock(&g_server_context.mutex);
    }
}

void tangram_ucx_server_stop() {

    while(g_server_running) {
        printf("Server still running...\n");
        sleep(1);
    }

    // Server stopped, clean up now
    taskmgr_finalize(&g_taskmgr);
    tangram_uct_context_destroy(&g_server_context);
    ucs_async_context_destroy(g_server_async);
}

tangram_uct_addr_t* tangram_ucx_server_addr() {
    return &g_server_context.self_addr;
}
