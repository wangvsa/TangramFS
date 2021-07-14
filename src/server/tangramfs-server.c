#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "tangramfs.h"
#include "tangramfs-metadata.h"
#include "tangramfs-ucx.h"
#include "tangramfs-rpc.h"

/**
 * Return a respond, can be NULL
 */
void* rpc_handler(int op, void* data, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(op == OP_RPC_POST) {
        rpc_in_t* in = rpc_in_unpack(data);
        for(int i = 0; i < in->num_intervals; i++)
            tangram_ms_handle_post(in->rank, in->filename, in->intervals[i].offset, in->intervals[i].count);
        //printf("post in->rank: %d, filename: %s, offset:%lu, count: %lu\n", in->rank, in->filename, in->intervals[0].offset/1024/1024, in->intervals[0].count/1024/1024);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        return respond;
    } else if(op == OP_RPC_QUERY) {
        rpc_in_t* in = rpc_in_unpack(data);
        *respond_len = sizeof(rpc_out_t);
        rpc_out_t *out = malloc(sizeof(rpc_out_t));
        bool found = tangram_ms_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count, &(out->rank));
        //printf("query in->rank: %d, filename: %s, offset:%lu, count: %lu, out->rank: %d\n",
        //        in->rank, in->filename, in->intervals[0].offset/1024/1024, in->intervals[0].count/1024/1024, out->rank);
        // TODO, due to UCX bug, the previous post may not take effect
        //assert(found);
        rpc_in_free(in);
        respond = out;
    }

    return respond;
}


int main(int argc, char* argv[]) {
    assert(argc == 2);

    if( strcmp(argv[1], "start") == 0 ) {

        tangram_ucx_server_init("./");
        printf("Server started\n");
        tangram_ucx_server_register_rpc(rpc_handler);
        tangram_ucx_server_start();

    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_rpc_service_start("./");
        tangram_ucx_stop_server();
        tangram_rpc_service_stop();
    }

    return 0;
}

