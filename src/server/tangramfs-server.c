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
void* rpc_handler(int op, void* data, size_t length, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(op == OP_RPC_POST) {
        rpc_post_in_t* in = rpc_inout_unpack(data);
        for(int i = 0; i < in->num_intervals; i++)
            tangram_ms_handle_post(in->rank, in->filename, in->intervals[i].offset, in->intervals[i].count);
        printf("post in->rank: %d, filename: %s, offset:%lu, count: %lu\n", in->rank, in->filename, in->intervals[0].offset/1024/1024, in->intervals[0].count/1024/1024);
        rpc_inout_free(in);
    } else if(op == OP_RPC_QUERY) {
        rpc_post_in_t* in = rpc_inout_unpack(data);
        *respond_len = sizeof(rpc_query_out_t);
        rpc_query_out_t *out = malloc(sizeof(rpc_query_out_t));
        printf("query in->rank: %d, filename: %s, offset:%lu, count: %lu\n", in->rank, in->filename, in->intervals[0].offset/1024/1024, in->intervals[0].count/1024/1024);
        bool found = tangram_ms_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count, &(out->rank));
        assert(found);
        rpc_inout_free(in);
        respond = out;
    }

    return respond;
}


int main(int argc, char* argv[]) {
    assert(argc == 2);

    tangram_ucx_server_init();
    tangram_ucx_server_register_rpc(rpc_handler);

    if( strcmp(argv[1], "start") == 0 ) {
        tangram_ucx_server_start();
    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_set_server_addr();
        tangram_ucx_stop_server();
    }

    return 0;
}

