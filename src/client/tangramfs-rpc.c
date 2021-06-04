#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include "tangramfs-ucx.h"
#include "tangramfs-rpc.h"

tangram_ucx_context_t context;


// The main thread calls this and wait for
// the client progress thread to finish or receive the respond.
void tangram_rpc_issue_rpc(int op, char* filename, int rank, size_t *offsets, size_t *counts, int len, void** respond) {
    strcpy(context.server_addr, "192.168.1.249");

    rpc_post_in_t *data = malloc(sizeof(rpc_post_in_t)*len);
    for(int i = 0; i < len; i++) {
        data[i].filename = filename;
        data[i].rank = rank;
        data[i].offset = offsets[i];
        data[i].count = counts[i];
    }

    switch(op) {
        case RPC_OP_POST:
            tangram_ucx_send(&context, op, data, sizeof(rpc_post_in_t)*len);
            break;
        case RPC_OP_QUERY:
            tangram_ucx_sendrecv(&context, op, data, sizeof(rpc_post_in_t)*len, respond);
            break;
        default:
            break;
    }

    free(data);
}

void rpc_query_callback()
{
}
