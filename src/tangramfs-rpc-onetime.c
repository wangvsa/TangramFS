#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <mercury_macros.h>
#include "tangramfs-rpc.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;
static hg_addr_t       hg_addr = NULL;    // addr retrived from the addr lookup callback

static hg_id_t         rpc_id_post;
static hg_id_t         rpc_id_query;


void mercury_onetime_init();
void mercury_onetime_finalize();
void mercury_register_rpcs();
void mercury_onetime_progress_loop();
hg_return_t rpc_query_callback(const struct hg_cb_info *info);
hg_return_t rpc_post_callback(const struct hg_cb_info *info);


void tangram_rpc_onetime_start(const char* server_addr) {
    mercury_onetime_init();
    mercury_register_rpcs();
    HG_Addr_lookup2(hg_class, server_addr, &hg_addr);
    //mercury_onetime_progress_loop();
}

void tangram_rpc_onetime_stop() {
    mercury_onetime_finalize();
}



/*
 * -----------------------------------
 * Internally Used Below
 * -----------------------------------
 */
void mercury_onetime_init() {
    hg_class = HG_Init(MERCURY_PROTOCOL, HG_FALSE);
    assert(hg_class != NULL);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);
}

void mercury_onetime_finalize() {
    hg_return_t ret;
    ret = HG_Context_destroy(hg_context);
    assert(ret == HG_SUCCESS);

    HG_Addr_free(hg_class, hg_addr);

    ret = HG_Finalize(hg_class);
    assert(ret == HG_SUCCESS);
}

void mercury_register_rpcs() {
    //rpc_id_query = MERCURY_REGISTER(hg_class, RPC_NAME_QUERY, rpc_query_in, rpc_query_out, NULL);
}

void mercury_onetime_progress_loop(void* arg) {
    while(1) {
        unsigned int count = 0;
        HG_Progress(hg_context, 100);
        HG_Trigger(hg_context, 0, 1, &count);
        if (count)
            break;
    }
}

// The main thread calls this and wait for
// the client progress thread to finish or receive the respond.
void tangram_rpc_onetime(const char* rpc_name, char* filename, int rank, size_t *offsets, size_t *counts, int len) {
    if(len <= 0) return;

    hg_id_t rpc_id;
    if(strcmp(rpc_name, RPC_NAME_QUERY) == 0)
        rpc_id = rpc_id_query;

    hg_return_t ret;
    hg_handle_t handle;

    ret = HG_Create(hg_context, hg_addr, rpc_id, &handle);
    assert(ret == HG_SUCCESS);

    if(strcmp(rpc_name, RPC_NAME_QUERY) == 0) {
        rpc_query_in in_arg = {
            .filename = filename,
            .rank = rank,
            .offset = offsets[0],
            .count = counts[0],
        };
        ret = HG_Forward(handle, rpc_query_callback, NULL, &in_arg);
    }

    ret = HG_Destroy(handle);
    assert(ret == HG_SUCCESS);
}
