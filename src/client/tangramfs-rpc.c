#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include "tangramfs-rpc.h"


/*
 * Perform RPC or RMA.
 * The underlying implementaiton is in tangramfs-ucx.c
 *
 * int dest_rank is only required for RMA operation
 *
 */
void tangram_issue_rpc_rma(int op, char* filename, int my_rank, int dest_rank,
                            size_t *offsets, size_t *counts, int num_intervals, void* respond) {

    size_t data_size;
    size_t total_recv_size = 0;     // RMA only
    void* user_data = rpc_in_pack(filename, my_rank, num_intervals, offsets, counts, &data_size);
    int ack;

    switch(op) {
        case OP_RPC_POST:
            tangram_ucx_sendrecv(op, user_data, data_size, &ack);
            break;
        case OP_RPC_QUERY:
            tangram_ucx_sendrecv(op, user_data, data_size, respond);
            break;
        case OP_RMA_REQUEST:
            for(int i = 0; i < num_intervals; i++)
                total_recv_size += counts[i];
            tangram_ucx_rma_request(dest_rank, user_data, data_size, respond, total_recv_size);
            break;
        default:
            break;
    }

    free(user_data);
}

void tangram_set_iface_addr(const char* iface, const char* ip_addr) {
    tangram_ucx_set_iface_addr(iface, ip_addr);
}

void tangram_rma_service_start(void* (*serve_rma_data)(void*, size_t*)) {
    tangram_ucx_rma_service_start(serve_rma_data);
}

void tangram_rma_service_stop() {
    tangram_ucx_rma_service_stop();
}

