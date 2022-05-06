#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include "utlist.h"
#include "tangramfs-ucx-taskmgr.h"

void (*task_handle_cb)(task_t* task);


/*
 * uint64_t is the header in am_short();
 * We do not use it for now.
 */
task_t* create_task(uint8_t id, void* buf, size_t buf_len) {
    task_t *task      = malloc(sizeof(task_t));
    task->id          = id;
    task->respond     = NULL;
    task->respond_len = 0;
    task->data        = NULL;
    unpack_rpc_buffer(buf, buf_len, &task->client, &task->data);
    return task;
}

/**
 * Append a task into one worker's task queue,
 * then notify that worker.
 *
 * workers[who].lock is used to protect the task list.
 *
 * Sync-based implementation only uses append_task() call
 * which uses a round-robin mannter to assign tasks to workers
 *
 * For lock-based implementation, we provide this
 * append_task_to_woker() call to allow
 * specifying a single worker to handle all lock related tasks.
 * This guarantees no concurrent lock related tasks are processed
 * at the same time. This is necessary for correctness.
 */
void taskmgr_append_task_to_worker(taskmgr_t* mgr, uint8_t id, void* buf, size_t buf_len, int tid) {

    task_t* task = create_task(id, buf, buf_len);
    worker_t* worker = &(mgr->workers[tid]);

    pthread_mutex_lock(&mgr->workers[tid].lock);
    DL_APPEND(mgr->workers[tid].tasks, task);
    pthread_cond_signal(&mgr->workers[tid].cond);
    pthread_mutex_unlock(&mgr->workers[tid].lock);
}

void taskmgr_append_task(taskmgr_t* mgr, uint8_t id, void* buf, size_t buf_len) {
    taskmgr_append_task_to_worker(mgr, id, buf, buf_len, mgr->who);
    mgr->who = (mgr->who + 1) % mgr->num_workers;
}

void free_task(task_t* task) {
    if(task->data)
        free(task->data);

    // TODO if task->respond is tangram_uct_addr_t*, then we did not release all its memory space.
    //if(task->respond)
    //    free(task->respond);

    tangram_uct_addr_free(&task->client);
    free(task);
}

void* worker_func(void* arg) {

    worker_t *me = (worker_t*) arg;

    while(me->running) {

        pthread_mutex_lock(&me->lock);

        // If no task available, go to sleep
        // Delegator will insert a task and wake us up later.
        if (me->tasks == NULL) {
            pthread_cond_wait(&me->cond, &me->lock);
        }

        // Possible get the signal because delegator stoped
        if (!me->running) {
            pthread_mutex_unlock(&me->lock);
            break;
        }

        // FIFO manner
        task_t *task = me->tasks;
        assert(task != NULL);
        DL_DELETE(me->tasks, task);

        pthread_mutex_unlock(&me->lock);

        task_handle_cb(task);
        free_task(task);
    }

    // At this point, we should have handled all tasks.
    // i.e., mgr->workers[tid].tasks should be empty.
    //
    // But it is possible that client stoped the delegator
    // before all their requests have been finished.
}

void taskmgr_init(taskmgr_t* mgr, int num_workers,
                  void (*_task_handle_cb)(task_t* task) ) {

    task_handle_cb = _task_handle_cb;

    mgr->num_workers = num_workers;
    mgr->workers = tangram_malloc(num_workers * sizeof(worker_t));

    for(int i = 0; i < num_workers; i++) {
        mgr->workers[i].tid = i;
        mgr->workers[i].tasks = NULL;
        mgr->workers[i].running = 1;
        pthread_mutex_init(&mgr->workers[i].lock, NULL);
        pthread_cond_init(&mgr->workers[i].cond, NULL);
        pthread_create(&(mgr->workers[i].thread), NULL, worker_func, &mgr->workers[i]);
    }
}

void taskmgr_finalize(taskmgr_t* mgr) {

    for(int i = 0; i < mgr->num_workers; i++) {
        pthread_mutex_lock(&mgr->workers[i].lock);
        mgr->workers[i].running = 0;
        pthread_cond_signal(&mgr->workers[i].cond);
        pthread_mutex_unlock(&mgr->workers[i].lock);
        pthread_join(mgr->workers[i].thread, NULL);

        pthread_cond_destroy(&mgr->workers[i].cond);
        pthread_mutex_destroy(&mgr->workers[i].lock);
    }

    tangram_free(mgr->workers, mgr->num_workers * sizeof(worker_t));
    mgr->workers = NULL;
}
