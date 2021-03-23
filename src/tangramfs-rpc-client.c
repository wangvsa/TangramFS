#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <mercury_macros.h>
#include "tangramfs-rpc.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;
static hg_addr_t       hg_addr = NULL;

static hg_id_t         rpc_id_post;
static hg_id_t         rpc_id_query;


static bool running;                       // If we are still runing the progress loop
pthread_t client_progress_thread;

pthread_cond_t cond =  PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

rpc_query_out query_result;


void mercury_client_init();
void mercury_client_finalize();
void mercury_client_register_rpcs();
void* mercury_client_progress_loop(void* arg);
hg_return_t rpc_query_callback(const struct hg_cb_info *info);
hg_return_t rpc_post_callback(const struct hg_cb_info *info);


void tangram_rpc_client_start(const char* server_addr) {
    mercury_client_init();
    mercury_client_register_rpcs();
    HG_Addr_lookup2(hg_class, server_addr, &hg_addr);

    running = true;
    pthread_create(&client_progress_thread, NULL, mercury_client_progress_loop, NULL);
}

void tangram_rpc_client_stop() {
    running = false;
    pthread_join(client_progress_thread, NULL);
    mercury_client_finalize();
}



/*
 * -----------------------------------
 * Internally Used Below
 * -----------------------------------
 */
void mercury_client_init() {
    hg_class = HG_Init(MERCURY_PROTOCOL, HG_FALSE);
    assert(hg_class != NULL);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);
}

void mercury_client_finalize() {
    hg_return_t ret;
    ret = HG_Context_destroy(hg_context);
    assert(ret == HG_SUCCESS);

    HG_Addr_free(hg_class, hg_addr);

    ret = HG_Finalize(hg_class);
    assert(ret == HG_SUCCESS);
}

void mercury_client_register_rpcs() {
    /* Register a RPC function.
     * The first two NULL correspond to what would be pointers to
     * serialization/deserialization functions for input and output datatypes
     * (not used in this example).
     * The third NULL is the pointer to the function (which is on the server,
     * so NULL here on the client).
     */
    rpc_id_post = MERCURY_REGISTER(hg_class, RPC_NAME_POST, rpc_post_in, void, NULL);
    HG_Registered_disable_response(hg_class, rpc_id_post, HG_TRUE);

    rpc_id_query = MERCURY_REGISTER(hg_class, RPC_NAME_QUERY, rpc_query_in, rpc_query_out, NULL);
}

void* mercury_client_progress_loop(void* arg) {
    hg_return_t ret;
    do {
        unsigned int count;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count);
        HG_Progress(hg_context, 100);
    } while(running);

    return NULL;
}


// The main thread calls this and wait for
// the client progress thread to finish or receive the respond.
void tangram_rpc_issue_rpc(const char* rpc_name, char* filename, int rank, size_t *offsets, size_t *counts, int len) {
    if(len <= 0) return;

    hg_id_t rpc_id;
    if(strcmp(rpc_name, RPC_NAME_POST) == 0)
        rpc_id = rpc_id_post;
    if(strcmp(rpc_name, RPC_NAME_QUERY) == 0)
        rpc_id = rpc_id_query;

    hg_return_t ret;
    hg_handle_t handle;

    ret = HG_Create(hg_context, hg_addr, rpc_id, &handle);
    assert(ret == HG_SUCCESS);

    pthread_mutex_lock(&mutex);

    if(strcmp(rpc_name, RPC_NAME_POST) == 0) {

        rpc_post_in arg, tmp[len];
        int i;
        for(i = 0; i < len; i++) {
            tmp[i] = malloc(sizeof(struct rpc_post_in_t));
            tmp[i]->offset = offsets[i];
            tmp[i]->count = counts[i];
            tmp[i]->next = NULL;
        }
        for(i = 0; i < len - 1; i++)
            tmp[i]->next = tmp[i+1];

        arg = tmp[0];
        arg->filename = filename;
        arg->rank = rank;

        ret = HG_Forward(handle, rpc_post_callback, NULL, &arg);

        for(i = 0; i < len; i++)
            free(tmp[i]);
    }

    if(strcmp(rpc_name, RPC_NAME_QUERY) == 0) {
        rpc_query_in in_arg = {
            .filename = filename,
            .rank = rank,
            .offset = offsets[0],
            .count = counts[0],
        };
        ret = HG_Forward(handle, rpc_query_callback, NULL, &in_arg);
    }

    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);

    assert(ret == HG_SUCCESS);

    ret = HG_Destroy(handle);
    assert(ret == HG_SUCCESS);
}

void signal_main_thread() {
    pthread_mutex_lock(&mutex);
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&mutex);
}

hg_return_t rpc_post_callback(const struct hg_cb_info *info)
{
    signal_main_thread();
    return HG_SUCCESS;
}

hg_return_t rpc_query_callback(const struct hg_cb_info *info)
{
    hg_handle_t handle = info->info.forward.handle;

    rpc_query_out out;
    HG_Get_output(handle, &out);

    query_result.rank = out.rank;
    HG_Free_output(handle, &out);

    signal_main_thread();
    return HG_SUCCESS;
}

rpc_query_out tangram_rpc_query_result() {
    return query_result;
}
