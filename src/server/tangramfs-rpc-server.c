#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mercury_macros.h>
#include "tangramfs.h"
#include "tangramfs-rpc.h"
#include "tangramfs-utils.h"
#include "tangramfs-metadata.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;
static int running;

// List of RPC handlers
hg_return_t rpc_handler_post(hg_handle_t h);
hg_return_t rpc_handler_query(hg_handle_t h);
hg_return_t rpc_handler_stop(hg_handle_t h);

void  mercury_server_init(char* server_addr);
void  mercury_server_finalize();
void  mercury_server_register_rpcs();
void mercury_server_progress_loop();

// Only this one funciton is used by outsiders
void tangram_server_start(char* tfs_dir, char* server_addr) {
    mercury_server_init(server_addr);
    mercury_server_register_rpcs();
    running = 1;
    tangram_ms_init();

    tangram_write_server_addr(tfs_dir, server_addr);
    mercury_server_progress_loop();
}

void tangram_server_finalize() {
    mercury_server_finalize();
    tangram_ms_finalize();
    printf("Server stoped.\n");
}

void mercury_server_init(char* server_addr) {
    hg_return_t ret;

    hg_class = HG_Init(MERCURY_PROTOCOL, HG_TRUE);
    assert(hg_class != NULL);

    hg_size_t hostname_size;
    hg_addr_t self_addr;
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, server_addr, &hostname_size, self_addr);
    printf("Server running at address %s\n", server_addr);
    HG_Addr_free(hg_class, self_addr);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);
}

void mercury_server_finalize() {
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);
}

void mercury_server_register_rpcs() {
    // Register the RPC by its name
    // The two NULL arguments correspond to the functions user to
    // serialize/deserialize the input and output parameters
    hg_id_t rpc_id_post = MERCURY_REGISTER(hg_class, RPC_NAME_POST, rpc_post_in, void, rpc_handler_post);
    HG_Registered_disable_response(hg_class, rpc_id_post, HG_TRUE);
    hg_id_t rpc_id_query = MERCURY_REGISTER(hg_class, RPC_NAME_QUERY, rpc_query_in, rpc_query_out, rpc_handler_query);

    hg_id_t rpc_id_stop = MERCURY_REGISTER(hg_class, RPC_NAME_STOP, void, void, rpc_handler_stop);
    HG_Registered_disable_response(hg_class, rpc_id_stop, HG_TRUE);
}

void mercury_server_progress_loop() {
    hg_return_t ret;
    do {
        unsigned int count;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count);
        HG_Progress(hg_context, 100);
    } while(running);

    // server stoped
    tangram_server_finalize();
}


// ------------------------------------------
//  RPC handlers
//  ------------------------------------------
hg_return_t rpc_handler_post(hg_handle_t h)
{
    rpc_post_in arg, tmp;
    HG_Get_input(h, &arg);
    // printf("RPC - post: rank: %d, %s %d, %d\n", arg.rank, arg.filename, arg.offset/1024/1024, arg.count/1024/1024);

    tmp = arg;
    char* filename = tmp->filename;
    int rank = tmp->rank;
    while(tmp) {
        tangram_ms_handle_post(rank, filename, tmp->offset, tmp->count);
        tmp = tmp->next;
    }

    HG_Free_input(h, &arg);

    hg_return_t ret = HG_Destroy(h);
    assert(ret == HG_SUCCESS);

    return HG_SUCCESS;
}

hg_return_t rpc_handler_query(hg_handle_t h)
{
    rpc_query_in in;
    HG_Get_input(h, &in);

    rpc_query_out out;
    bool found = tangram_ms_handle_query(in.filename, in.offset, in.count, &out.rank);
    HG_Respond(h, NULL, NULL, &out);

    HG_Free_input(h, &in);
    hg_return_t ret = HG_Destroy(h);
    assert(ret == HG_SUCCESS);
    return HG_SUCCESS;
}

hg_return_t rpc_handler_stop(hg_handle_t h)
{
    running = 0;
    return HG_SUCCESS;
}
