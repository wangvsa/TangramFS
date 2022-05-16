#define _GNU_SOURCE

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <mpi.h>
#include "tangramfs-rpc.h"
#include "tangramfs-delegator.h"
#include "tangramfs-ucx-rma.h"
#include "tangramfs-ucx-client.h"
#include "tangramfs-ucx-delegator.h"

static double      rma_time;
static tfs_info_t* g_tfs_info;


/*
 * Perform RPC (send to server).
 * The underlying implementaiton is in src/ucx/tangram-ucx-client.c
 */
void tangram_issue_rpc(uint8_t id, char* filename, size_t *offsets, size_t *counts, int* types, int num_intervals, void** respond_ptr) {

    // Some message does not send intervals
    if(num_intervals == 0) {
        size_t data_size;
        void* user_data = rpc_in_pack(filename, num_intervals, offsets, counts, types, &data_size);

        if(g_tfs_info->use_delegator)
            tangram_ucx_sendrecv_delegator(id, user_data, data_size, respond_ptr);
        else
            tangram_ucx_sendrecv_server(id, user_data, data_size, respond_ptr);

        free(user_data);
        return;
    }

    // We need to guarantee the message size
    // does not exceed max am size
    // In case its too large, we split it into multiple AM
    size_t am_max_size = tangram_uct_am_short_max_size();
    int num = rpc_in_intervals_per_am(filename, am_max_size);
    int remain = num_intervals;

    int i = 0;
    while(remain > 0) {

        size_t data_size;
        void* user_data = rpc_in_pack(filename, num < remain ? num : remain,
                                      &offsets[i*num], &counts[i*num], types?&types[i*num]:NULL, &data_size);
        if(g_tfs_info->use_delegator)
            tangram_ucx_sendrecv_delegator(id, user_data, data_size, respond_ptr);
        else
            tangram_ucx_sendrecv_server(id, user_data, data_size, respond_ptr);

        free(user_data);

        remain -= num;
        i++;
    }
}


/*
 * Perform RPC (between clients)
 * The underlying implementaiton is in src/ucx/tangram-ucx-client.c
 */
void tangram_issue_rma(uint8_t id, char* filename, tangram_uct_addr_t* dest,
                            size_t *offsets, size_t *counts, int num_intervals, void* recv_buf) {

    size_t data_size;
    void* user_data = rpc_in_pack(filename, num_intervals, offsets, counts, NULL, &data_size);

    size_t total_recv_size = 0;
    for(int i = 0; i < num_intervals; i++)
        total_recv_size += counts[i];

    tangram_ucx_rma_request(dest, user_data, data_size, recv_buf, total_recv_size);
}

void tangram_issue_metadata_rpc(uint8_t id, const char* path, void** respond_ptr) {
    void* data = (void*) path;
    switch(id) {
        case AM_ID_STAT_REQUEST:
            tangram_ucx_sendrecv_server(id, data, 1+strlen(path), respond_ptr);
            break;
        default:
            break;
    }
}

void tangram_rpc_service_start(tfs_info_t *tfs_info){

    g_tfs_info = tfs_info;

    // Must start delegator first
    // later the client will need to broadcast delegator's address
    // to all clients

    if(tfs_info->use_delegator && tfs_info->mpi_intra_rank == 0) {
        tangram_delegator_start(tfs_info);
    }

    sleep(2);
    MPI_Barrier(tfs_info->mpi_comm);
    tangram_ucx_client_start(tfs_info);
}

void tangram_rpc_service_stop(tfs_info_t* tfs_info) {
    if(tfs_info->use_delegator && tfs_info->mpi_intra_rank == 0) {
        tangram_ucx_stop_delegator();
        tangram_delegator_stop();
    }
    tangram_ucx_client_stop();
}

tangram_uct_addr_t* tangram_rpc_client_inter_addr() {
    return tangram_ucx_client_inter_addr();
}

void tangram_rma_service_start(tfs_info_t *tfs_info, void* (*serve_rma_data_cb)(void*, size_t*)) {
    tangram_ucx_rma_service_start(tfs_info, serve_rma_data_cb);
}

void tangram_rma_service_stop() {
    tangram_ucx_rma_service_stop();
    //tangram_debug("Total rma time: %.3f\n", rma_time);
}

