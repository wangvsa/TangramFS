#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <mercury_macros.h>
#include "tangramfs-rpc-server.h"
#include "tangramfs-utils.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;
static hg_addr_t       hg_addr = NULL;    // addr retrived from the addr lookup callback

static hg_id_t         rpc_id_stop;

hg_return_t rpc_stop_callback(const struct hg_cb_info *info)
{
    return HG_SUCCESS;
}

void mercury_onetime_init(char* server_addr) {
    hg_class = HG_Init(MERCURY_PROTOCOL, HG_FALSE);
    assert(hg_class != NULL);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);

    rpc_id_stop = MERCURY_REGISTER(hg_class, RPC_NAME_STOP, void, void, NULL);
    HG_Registered_disable_response(hg_class, rpc_id_stop, HG_TRUE);

    HG_Addr_lookup2(hg_class, server_addr, &hg_addr);
    assert(hg_addr != NULL);
}

void mercury_onetime_finalize() {
    hg_return_t ret;

    ret = HG_Context_destroy(hg_context);
    assert(ret == HG_SUCCESS);

    HG_Addr_free(hg_class, hg_addr);

    ret = HG_Finalize(hg_class);
    assert(ret == HG_SUCCESS);
}

void tangram_server_stop(char* server_addr) {
    hg_return_t ret;
    // 1. Init
    mercury_onetime_init(server_addr);

    // 2. Issue RPC
    hg_handle_t handle;
    HG_Create(hg_context, hg_addr, rpc_id_stop, &handle);
    HG_Forward(handle, NULL, rpc_stop_callback, NULL);

    // progress loop
    while(1) {
        unsigned int count = 0;
        HG_Progress(hg_context, 100);
        ret = HG_Trigger(hg_context, 0, 1, &count);
        if (ret == HG_SUCCESS && count)
            break;
    }
    HG_Destroy(handle);

    // 3. Finalize
    mercury_onetime_finalize();
}

void print_help_msg() {
    printf("Usage:\n");
    printf("\t./server start|stop /path/to/persist/directory\n");
}

int main(int argc, char* argv[]) {
    char server_addr[128];
    char* cmd = argv[1];

    if(argc != 3) {
        print_help_msg();
        return 0;
    }

    if( strcmp(argv[1], "start") == 0 ) {
        tangram_server_start(argv[2], server_addr);
    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_read_server_addr(argv[2], server_addr);
        printf("Server addr: %s\n", server_addr);

        tangram_server_stop(server_addr);
        printf("Server stoped.\n");
    } else {
        print_help_msg();
    }

    return 0;
}
