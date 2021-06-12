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
void tangram_rpc_issue_rpc(int op, char* filename, int rank, size_t *offsets, size_t *counts, int num_intervals, void* respond) {

    strcpy(context.server_addr, "192.168.1.249");

    size_t size;
    void* data = rpc_inout_pack(filename, rank, num_intervals, offsets, counts, &size);

    switch(op) {
        case RPC_OP_POST:
            tangram_ucx_send(&context, op, data, size);

            ucp_mem_h memh;
            tangram_mmap_send_rkey(&context, 1024, &memh);

            break;
        case RPC_OP_QUERY:
            tangram_ucx_sendrecv(&context, op, data, size, respond);
            break;
        default:
            break;
    }

    free(data);
}

void rpc_query_callback()
{
}
