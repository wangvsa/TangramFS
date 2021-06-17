#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include "tangramfs-rpc.h"


// The main thread calls this and wait for
// the client progress thread to finish or receive the respond.
void tangram_rpc_issue_rpc(int op, char* filename, int rank, size_t *offsets, size_t *counts, int num_intervals, void* respond) {

    size_t size;
    void* data = rpc_inout_pack(filename, rank, num_intervals, offsets, counts, &size);

    switch(op) {
        case OP_RPC_POST:
            tangram_ucx_send(op, data, size);
            break;
        case OP_RPC_QUERY:
            tangram_ucx_sendrecv(op, data, size, respond);
            break;
        case OP_RMA_REQUEST:
            tangram_ucx_rma_request(rank);
            break;
        default:
            break;
    }

    free(data);
}

void tangram_set_server_addr() {
    tangram_ucx_set_server_addr("192.168.1.249");
}

void tangram_rma_service_start() {
    tangram_ucx_rma_service_start();
}

void tangram_rma_service_stop() {
    tangram_ucx_rma_service_stop();
}

