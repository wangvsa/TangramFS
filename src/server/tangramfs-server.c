#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tangramfs.h"
#include "tangramfs-metadata.h"
#include "tangramfs-ucx.h"
#include "tangramfs-rpc.h"

tangram_ucx_context_t context;


void rpc_handler_post(void* data, size_t length)
{
    /*
    tmp = arg;
    char* filename = tmp->filename;
    int rank = tmp->rank;
    while(tmp) {
        tangram_ms_handle_post(rank, filename, tmp->offset, tmp->count);
        tmp = tmp->next;
    }
    */
}

void rpc_handler_query(void* data, size_t length)
{
    /*
    rpc_query_in in;
    HG_Get_input(h, &in);

    rpc_query_out out;
    bool found = tangram_ms_handle_query(in.filename, in.offset, in.count, &out.rank);
    */
}


void rpc_handler(int op, void* data, size_t length) {
    printf("server received rpc: %d\n", op);
}


int main(int argc, char* argv[]) {
    strcpy(context.server_addr, "192.168.1.249");

    assert(argc == 2);

    if( strcmp(argv[1], "start") == 0 ) {
        tangram_ucx_server_init(&context);
        tangram_ucx_server_register_rpc(&context, rpc_handler);
        tangram_ucx_server_start(&context);
    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_ucx_stop_server(&context);
    }
    return 0;
}

