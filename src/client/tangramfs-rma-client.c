#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <mercury_macros.h>
#include <mercury_atomic.h>
#include "tangramfs-rma-client.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;
static hg_addr_t       hg_addr = NULL;

static hg_id_t         rpc_id_post;
static hg_id_t         rpc_id_query;
static hg_id_t         rpc_id_transfer;


static volatile bool running;                       // If we are still runing the progress loop
pthread_t rma_client_progress_thread;

pthread_cond_t cond2 =  PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;


void mercury_rma_client_init();
void mercury_rma_client_finalize();
void mercury_rma_client_register_rpcs();
void* mercury_rma_client_progress_loop(void* arg);
hg_return_t rpc_transfer_callback(const struct hg_cb_info *info);


void tangram_rma_client_start(const char* server_addr) {
    mercury_rma_client_init();
    mercury_rma_client_register_rpcs();
    HG_Addr_lookup2(hg_class, server_addr, &hg_addr);

    running = true;
    pthread_create(&rma_client_progress_thread, NULL, mercury_rma_client_progress_loop, NULL);
}

void tangram_rma_client_stop() {
    running = false;
    pthread_join(rma_client_progress_thread, NULL);
    mercury_rma_client_finalize();
}


/*
 * -----------------------------------
 * Internally Used Below
 * -----------------------------------
 */
void mercury_rma_client_init() {
    hg_class = HG_Init(MERCURY_PROTOCOL, HG_FALSE);
    assert(hg_class != NULL);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);
}

void mercury_rma_client_finalize() {
    hg_return_t ret;
    ret = HG_Context_destroy(hg_context);
    assert(ret == HG_SUCCESS);

    HG_Addr_free(hg_class, hg_addr);

    ret = HG_Finalize(hg_class);
    assert(ret == HG_SUCCESS);
}

void mercury_rma_client_register_rpcs() {
    /* Register a RPC function.
     * The first two NULL correspond to what would be pointers to
     * serialization/deserialization functions for input and output datatypes
     * (not used in this example).
     * The third NULL is the pointer to the function (which is on the server,
     * so NULL here on the client).
     */
    rpc_id_transfer = MERCURY_REGISTER(hg_class, RPC_NAME_TRANSFER, rpc_transfer_in, rpc_transfer_out, NULL);
}

void* mercury_rma_client_progress_loop(void* arg) {
    hg_return_t ret;
    do {
        unsigned int count;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count);
        HG_Progress(hg_context, 0);
    } while(running);

    return NULL;
}


void signal_main_thread2() {
    pthread_mutex_lock(&mutex2);
    pthread_cond_signal(&cond2);
    pthread_mutex_unlock(&mutex2);
}

// Note: this function will be running on  the main thread
void tangram_rma_client_transfer(char* filename, int rank, size_t offset, size_t count, void* buf) {
    hg_return_t ret;
    hg_handle_t handle;

    ret = HG_Create(hg_context, hg_addr, rpc_id_transfer, &handle);
    assert(ret == HG_SUCCESS);

    rpc_transfer_in in_arg = {
        .filename = filename,
        .rank = rank,
        .offset = offset,
        .count = count,
    };

    ret = HG_Bulk_create(hg_class, 1, &buf, &count, HG_BULK_READWRITE, &in_arg.bulk_handle);
    assert(ret == HG_SUCCESS);

    ret = HG_Forward(handle, rpc_transfer_callback, NULL, &in_arg);
    assert(ret == HG_SUCCESS);

    pthread_cond_wait(&cond2, &mutex2);
    pthread_mutex_unlock(&mutex2);

    HG_Bulk_free(in_arg.bulk_handle);
    ret = HG_Destroy(handle);
    assert(ret == HG_SUCCESS);
}

hg_return_t rpc_transfer_callback(const struct hg_cb_info *info) {
    hg_handle_t handle = info->info.forward.handle;

    rpc_transfer_out out;
    HG_Get_output(handle, &out);
    HG_Free_output(handle, &out);

    signal_main_thread2();
    return HG_SUCCESS;
}
