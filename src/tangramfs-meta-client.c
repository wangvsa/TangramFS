#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include <mercury_macros.h>
#include "tangramfs-meta.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;
static hg_addr_t       hg_addr = NULL;    // addr retrived from the addr lookup callback

static hg_id_t         rpc_id_notify;
static hg_id_t         rpc_id_query;


static int running;                       // If we are still runing the progress loop
pthread_t progress_thread;



void mercury_client_init();
void mercury_client_finalize();
void mercury_register_rpcs();
void* mercury_client_progress_loop(void* arg);
hg_return_t lookup_callback(const struct hg_cb_info *callback_info);


void tangram_meta_client_start(const char* server_addr) {
    mercury_client_init();
    mercury_register_rpcs();
    HG_Addr_lookup(hg_context, lookup_callback, NULL, server_addr, HG_OP_ID_IGNORE);

    running = 1;
    pthread_create(&progress_thread, NULL, mercury_client_progress_loop, NULL);
}

void tangram_meta_client_stop() {
    running = 0;
    pthread_join(progress_thread, NULL);
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

    if(hg_addr)
        HG_Addr_free(hg_class, hg_addr);

    ret = HG_Finalize(hg_class);
    assert(ret == HG_SUCCESS);
}

void mercury_register_rpcs() {
    /* Register a RPC function.
     * The first two NULL correspond to what would be pointers to
     * serialization/deserialization functions for input and output datatypes
     * (not used in this example).
     * The third NULL is the pointer to the function (which is on the server,
     * so NULL here on the client).
     */
    rpc_id_notify = MERCURY_REGISTER(hg_class, RPC_NAME_NOTIFY, rpc_query_in, void, NULL);
    HG_Registered_disable_response(hg_class, rpc_id_notify, HG_TRUE);

    rpc_id_query = MERCURY_REGISTER(hg_class, RPC_NAME_QUERY, rpc_query_in, void, NULL);
    HG_Registered_disable_response(hg_class, rpc_id_query, HG_TRUE);
}

void* mercury_client_progress_loop(void* arg) {
    hg_return_t ret;
    while(running)
    {
        unsigned int count;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count && running);
        HG_Progress(hg_context, 100);
    }
}


void tangram_meta_issue_rpc(const char* rpc_name, const char* filename, int rank, size_t offset, size_t count) {
    while(hg_addr == NULL) {
        sleep(1);
    }

    hg_id_t rpc_id;
    if(strcmp(rpc_name, RPC_NAME_NOTIFY) == 0) {
        rpc_id = rpc_id_notify;
    }
    if(strcmp(rpc_name, RPC_NAME_QUERY) == 0) {
        rpc_id = rpc_id_query;
    }

    hg_return_t ret;
    hg_handle_t handle;

    ret = HG_Create(hg_context, hg_addr, rpc_id, &handle);
    assert(ret == HG_SUCCESS);

    /* Send the RPC. The first NULL correspond to the callback
     * function to call when receiving the response from the server
     * (we don't expect a response, hence NULL here).
     * The second NULL is a pointer to user-specified data that will
     * be passed to the response callback.
     * The third NULL is a pointer to the RPC's argument (we don't
     * use any here).
     */
    rpc_query_in arg = {
        .rank = rank,
        .offset = offset,
        .count = count,
    };

    ret = HG_Forward(handle, NULL, NULL, &arg);
    assert(ret == HG_SUCCESS);

    ret = HG_Destroy(handle);
    assert(ret == HG_SUCCESS);
}


/*
 * This function is called when the address lookup operation has running.
 */
hg_return_t lookup_callback(const struct hg_cb_info *callback_info)
{
    assert(callback_info->ret == 0);
    hg_addr = callback_info->info.lookup.addr;
    return HG_SUCCESS;
}
