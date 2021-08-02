#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include "utlist.h"
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

#define NUM_THREADS 8

volatile static bool g_server_running = true;
static ucs_async_context_t  *g_server_context;
static uct_listener_info_t g_info;


typedef struct rpc_task {
    uint8_t id;
    void*   respond;
    size_t  respond_len;

    int     client_rank;
    void*   data;

    struct rpc_task *next, *prev;
} rpc_task_t;


struct rpc_task_worker {
    int tid;
    pthread_t thread;
    pthread_mutex_t lock;
    rpc_task_t *tasks;
};


// A thread pool, where each worker handles a list of rpc_task_t*
static struct rpc_task_worker g_workers[NUM_THREADS];
static int who = 0;

// progress lock
pthread_mutex_t g_progress_lock = PTHREAD_MUTEX_INITIALIZER;


void* (*user_am_data_handler)(int8_t, void* data, size_t *respond_len);


void append_task(uint8_t id, void* data, size_t length) {
    // uint64_t is the header in am_short();
    // we use it to send client rank

    rpc_task_t *task = malloc(sizeof(rpc_task_t));
    task->id = id;
    task->respond = NULL;
    task->respond_len = 0;
    task->client_rank = *(uint64_t*)data;
    task->data = malloc(length-sizeof(uint64_t));
    memcpy(task->data, data+sizeof(uint64_t), length-sizeof(uint64_t));

    pthread_mutex_lock(&g_workers[who].lock);
    DL_APPEND(g_workers[who].tasks, task);
    assert(g_workers[who].tasks != NULL);
    pthread_mutex_unlock(&g_workers[who].lock);
}

static ucs_status_t am_query_listener(void *arg, void *data, size_t length, unsigned flags) {
    // TODO can directly use the data and return UCS_INPROGRESS
    // then free it later.
    append_task(AM_ID_QUERY_REQUEST, data, length);
    who = (who + 1) % NUM_THREADS;
    return UCS_OK;
}

static ucs_status_t am_post_listener(void *arg, void *data, size_t length, unsigned flags) {
    append_task(AM_ID_POST_REQUEST, data, length);
    who = (who + 1) % NUM_THREADS;
    return UCS_OK;
}

static ucs_status_t am_stop_listener(void *arg, void *data, size_t length, unsigned flags) {
    printf("Server: received stop server command!\n");
    g_server_running = false;
    return UCS_OK;
}

static ucs_status_t am_client_addr_listener(void *arg, void *data, size_t length, unsigned flags) {
    size_t dev_addr_len, iface_addr_len;
    int rank = *(uint64_t*)data;
    assert(rank >= 0);
    if(g_info.client_dev_addrs[rank]) {
        free(g_info.client_dev_addrs[rank]);
        g_info.client_dev_addrs[rank] = NULL;
    }
    if(g_info.client_iface_addrs[rank]) {
        free(g_info.client_iface_addrs[rank]);
        g_info.client_iface_addrs[rank] = NULL;
    }

    void* ptr = data + sizeof(uint64_t);

    memcpy(&dev_addr_len, ptr, sizeof(size_t));
    g_info.client_dev_addrs[rank] = malloc(dev_addr_len);
    memcpy(g_info.client_dev_addrs[rank], ptr+sizeof(size_t), dev_addr_len);

    memcpy(&iface_addr_len, ptr+sizeof(size_t)+dev_addr_len, sizeof(size_t));
    g_info.client_iface_addrs[rank] = malloc(iface_addr_len);
    memcpy(g_info.client_iface_addrs[rank], ptr+2*sizeof(size_t)+dev_addr_len, iface_addr_len);

    return UCS_OK;
}

void handle_one_task(rpc_task_t* task) {
    pthread_mutex_lock(&g_progress_lock);
    uct_ep_h ep;
    uct_ep_create_connect(g_info.iface, g_info.client_dev_addrs[task->client_rank],
                          g_info.client_iface_addrs[task->client_rank], &ep);
    pthread_mutex_unlock(&g_progress_lock);

    task->respond = (*user_am_data_handler)(task->id, task->data, &task->respond_len);

    // TODO Send respond to client
    if(task->respond) {
        uint8_t id;
        if(task->id == AM_ID_QUERY_REQUEST)
            id = AM_ID_QUERY_RESPOND;
        if(task->id == AM_ID_POST_REQUEST)
            id = AM_ID_POST_RESPOND;
        do_uct_am_short(&g_progress_lock, ep, id, 0, task->respond, task->respond_len);
        free(task->respond);
    }

    uct_ep_destroy(ep);
}

void* rpc_task_worker_func(void* arg) {
    int tid = *((int*)arg);
    rpc_task_t *task = NULL;
    while(g_server_running) {

        pthread_mutex_lock(&g_workers[tid].lock);
        task = g_workers[tid].tasks;
        if(task)
            DL_DELETE(g_workers[tid].tasks, task);
        pthread_mutex_unlock(&g_workers[tid].lock);

        if(task) {
            handle_one_task(task);
            free(task->data);
            free(task);
        }
    }

    // At this point, we should have handled all tasks.
    // i.e., g_workers[tid].tasks should be empty.
    //
    // But it is possible that client stoped the server
    // before all their requests have been finished.
    int count;
    DL_COUNT(g_workers[tid].tasks, task, count);
    assert(count == 0);
}


void tangram_ucx_server_init(const char* persist_dir) {
    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_server_context);

    //init_uct_listener_info(g_server_context, "enp6s0", "tcp", true, &g_info);
    init_uct_listener_info(g_server_context, "hsi0", "tcp", true, &g_info);
    //init_uct_listener_info(g_server_context, "qib0:1", "ud_verbs", true, &g_info);

    status = uct_iface_set_am_handler(g_info.iface, AM_ID_QUERY_REQUEST, am_query_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_info.iface, AM_ID_POST_REQUEST, am_post_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_info.iface, AM_ID_STOP_REQUEST, am_stop_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_info.iface, AM_ID_CLIENT_ADDR, am_client_addr_listener, NULL, 0);
    assert(status == UCS_OK);

    for(int i = 0; i < NUM_THREADS; i++) {
        g_workers[i].tid = i;
        g_workers[i].tasks = NULL;
        int err = pthread_mutex_init(&g_workers[i].lock, NULL);
        assert(err == 0);
        pthread_create(&(g_workers[i].thread), NULL, rpc_task_worker_func, &g_workers[i].tid);
    }
}

void tangram_ucx_server_register_rpc(void* (*user_handler)(int8_t, void*, size_t*)) {
    user_am_data_handler = user_handler;
}


void tangram_ucx_server_start() {

    while(g_server_running) {
        pthread_mutex_lock(&g_progress_lock);
        uct_worker_progress(g_info.worker);
        pthread_mutex_unlock(&g_progress_lock);
    }

    // Clean up
    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(g_workers[i].thread, NULL);
    }

    destroy_uct_listener_info(&g_info);
    ucs_async_context_destroy(g_server_context);
}
