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

static hg_id_t         rpc_id_transfer;


void mercury_onetime_init();
void mercury_onetime_finalize();
void mercury_onetime_register_rpcs();
void mercury_onetime_progress_loop();
hg_return_t rpc_transfer_callback(const struct hg_cb_info *info);


void tangram_rpc_onetime_start(const char* server_addr) {
    mercury_onetime_init();
    mercury_onetime_register_rpcs();
    HG_Addr_lookup2(hg_class, server_addr, &hg_addr);
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

void mercury_onetime_register_rpcs() {
    rpc_id_transfer = MERCURY_REGISTER(hg_class, RPC_NAME_TRANSFER, rpc_transfer_in, rpc_transfer_out, NULL);
}

void mercury_onetime_progress_loop() {
    while(1) {
        unsigned int count = 0;
        HG_Progress(hg_context, 100);
        HG_Trigger(hg_context, 0, 1, &count);
        if (count)
            break;
    }
}

void tangram_rpc_onetime_transfer(void* buf) {

    hg_id_t rpc_id = rpc_id_transfer;
    hg_return_t ret;
    hg_handle_t handle;

    ret = HG_Create(hg_context, hg_addr, rpc_id, &handle);
    assert(ret == HG_SUCCESS);

    rpc_transfer_in in_arg = {
        //.filename = filename,
        //.rank = rank,
        //.offset = offsets[0],
        //.count = counts[0],
    };


    /*
    size_t size = 10;
    ret = HG_Bulk_create(hgi->hg_class, 1, buf, size, HG_BULK_READ_ONLY, &in.bulk_handle);

    my_rpc_state_p->bulk_handle = in.bulk_handle;
    assert(ret == 0);
    */

    ret = HG_Forward(handle, rpc_transfer_callback, NULL, &in_arg);

    mercury_onetime_progress_loop();

    ret = HG_Destroy(handle);
    assert(ret == HG_SUCCESS);
}

hg_return_t rpc_transfer_callback(const struct hg_cb_info *info) {
    hg_handle_t handle = info->info.forward.handle;
    rpc_transfer_out out;
    HG_Get_output(handle, &out);

    HG_Free_output(handle, &out);
    return HG_SUCCESS;
}
