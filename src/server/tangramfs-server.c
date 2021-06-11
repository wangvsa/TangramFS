#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "tangramfs.h"
#include "tangramfs-metadata.h"
#include "tangramfs-ucx.h"
#include "tangramfs-rpc.h"

tangram_ucx_context_t context;


/**
 * Return a respond, can be NULL
 */
void* rpc_handler(int op, void* data, size_t length, size_t *respond_len) {
    *respond_len = 0;

    rpc_post_in_t* in = rpc_inout_unpack(data);

    if(op == RPC_OP_POST) {
        for(int i = 0; i < in->num_intervals; i++)
            tangram_ms_handle_post(in->rank, in->filename, in->intervals[i].offset, in->intervals[i].count);

        rpc_inout_free(in);
        return NULL;
    }

    if(op == RPC_OP_QUERY) {
        *respond_len = sizeof(rpc_query_out_t);
        rpc_query_out_t *out = malloc(sizeof(rpc_query_out_t));
        bool found = tangram_ms_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count, &(out->rank));
        assert(found);
        printf("in rank: %d, out rank: %d\n", in->rank, out->rank);

        rpc_inout_free(in);
        return out;
    }
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

