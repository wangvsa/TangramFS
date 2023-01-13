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

typedef struct rpc_rma_addr_entry {
    void*              rpc_addr_key;                 // key, serialized of rpc tangram_uct_addr_t
    tangram_uct_addr_t rma_addr;
    UT_hash_handle     hh;
} rpc_rma_addr_entry_t;

static rpc_rma_addr_entry_t *g_rpc_rma_addr_map;

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


    size_t key_len;
    void* key = tangram_uct_addr_serialize(dest, &key_len);
    rpc_rma_addr_entry_t* entry = NULL;
    HASH_FIND(hh, g_rpc_rma_addr_map, key, key_len, entry);
    free(key);
    if(entry) {
        tangram_ucx_rma_request(&entry->rma_addr, user_data, data_size, recv_buf, total_recv_size);
    } else {
        printf("No map from the given client RPC addr to RMA addr!\n");
    }

    free(user_data);
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

    if(tfs_info->use_delegator && tfs_info->mpi_intra_rank == 0)
        tangram_delegator_start(tfs_info);

    sleep(2);
    MPI_Barrier(tfs_info->mpi_comm);
    tangram_ucx_client_start(tfs_info);
}

void tangram_rpc_service_stop() {
    if(g_tfs_info->use_delegator && g_tfs_info->mpi_intra_rank == 0) {
        tangram_ucx_stop_delegator();
        tangram_delegator_stop();
    }

    tangram_ucx_client_stop();
}

tangram_uct_addr_t* tangram_rpc_client_inter_addr() {
    return tangram_ucx_client_inter_addr();
}

void build_rpc_rma_addr_map() {
    tangram_uct_addr_t* rpc_addr = tangram_ucx_client_inter_addr();
    tangram_uct_addr_t* rma_addr = tangram_ucx_rma_addr();

    size_t rpc_addr_len, rma_addr_len;
    void* rpc_addr_buf = tangram_uct_addr_serialize(rpc_addr, &rpc_addr_len);
    void* rma_addr_buf = tangram_uct_addr_serialize(rma_addr, &rma_addr_len);

    void* rpc_addrs = malloc(rpc_addr_len * g_tfs_info->mpi_size);
    void* rma_addrs = malloc(rma_addr_len * g_tfs_info->mpi_size);

    MPI_Allgather(rpc_addr_buf, rpc_addr_len, MPI_BYTE, rpc_addrs, rpc_addr_len, MPI_BYTE, g_tfs_info->mpi_comm);
    MPI_Allgather(rma_addr_buf, rma_addr_len, MPI_BYTE, rma_addrs, rma_addr_len, MPI_BYTE, g_tfs_info->mpi_comm);

    for(int rank = 0; rank < g_tfs_info->mpi_size; rank++) {
        rpc_rma_addr_entry_t* entry = malloc(sizeof(rpc_rma_addr_entry_t));
        entry->rpc_addr_key = malloc(rpc_addr_len);
        memcpy(entry->rpc_addr_key, rpc_addrs+rpc_addr_len*rank, rpc_addr_len);
        tangram_uct_addr_deserialize(rma_addrs+rma_addr_len*rank, &entry->rma_addr);
        HASH_ADD_KEYPTR(hh, g_rpc_rma_addr_map, entry->rpc_addr_key, rpc_addr_len, entry);
    }

    free(rpc_addrs);
    free(rma_addrs);
    free(rpc_addr_buf);
    free(rma_addr_buf);
}

void tangram_rma_service_start(tfs_info_t *tfs_info, void* (*serve_rma_data_cb)(void*, size_t*)) {
    tangram_ucx_rma_service_start(tfs_info, serve_rma_data_cb);
    sleep(1);
    build_rpc_rma_addr_map();
}

void tangram_rma_service_stop() {
    tangram_ucx_rma_service_stop();
    //tangram_debug("Total rma time: %.3f\n", rma_time);

    rpc_rma_addr_entry_t *entry, *tmp;
    HASH_ITER(hh, g_rpc_rma_addr_map, entry, tmp) {
        HASH_DEL(g_rpc_rma_addr_map, entry);
        free(entry->rpc_addr_key);
        tangram_uct_addr_free(&entry->rma_addr);
        free(entry);
    }

}

