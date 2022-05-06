#ifndef _TANGRAMFS_UCX_TASKMGR_H_
#define _TANGRAMFS_UCX_TASKMGR_H_
#include "tangramfs-ucx-comm.h"

/* Represents one RPC request */
typedef struct _task {
    uint8_t id;
    void*   respond;
    size_t  respond_len;
    void*   data;
    struct  _task *next, *prev;
    tangram_uct_addr_t client;
} task_t;

/*
 * Each worker maintains a FIFO queue of RPC tasks
 * Server will insert tasks worker's queue in a
 * round-robin manner.
 */
typedef struct _worker {
    int             tid;
    int             running;
    pthread_t       thread;
    pthread_mutex_t lock;
    pthread_cond_t  cond;
    task_t*         tasks;
} worker_t;

typedef struct _taskmgr {
    int num_workers;
    int who;
    worker_t *workers;
} taskmgr_t;

void taskmgr_append_task_to_worker(taskmgr_t* mgr, uint8_t id, void* buf, size_t buf_len, int tid);

void taskmgr_append_task(taskmgr_t* mgr, uint8_t id, void* buf, size_t buf_len);

void taskmgr_init(taskmgr_t* mgr, int num_workers, void (*_task_handle_cb)(task_t* task));

void taskmgr_finalize(taskmgr_t* mgr);

#endif
