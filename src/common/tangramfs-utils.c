#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <assert.h>

#include "tangramfs.h"
#include "tangramfs-utils.h"

static size_t memory_usage = 0;

void* tangram_malloc(size_t size) {
    memory_usage += size;
    return malloc(size);
}

void tangram_free(void* ptr, size_t size) {
    memory_usage -= size;
    free(ptr);
}

void tangram_info_init(tfs_info_t* tfs_info) {
    MPI_Comm_dup(MPI_COMM_WORLD, &tfs_info->mpi_comm);
    MPI_Comm_rank(tfs_info->mpi_comm, &tfs_info->mpi_rank);
    MPI_Comm_size(tfs_info->mpi_comm, &tfs_info->mpi_size);

    MPI_Comm_split_type(tfs_info->mpi_comm, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &tfs_info->mpi_intra_comm);
    MPI_Comm_rank(tfs_info->mpi_intra_comm, &tfs_info->mpi_intra_rank);
    MPI_Comm_size(tfs_info->mpi_intra_comm, &tfs_info->mpi_intra_size);

    const char* persist_dir = getenv(TANGRAM_PERSIST_DIR_ENV);
    const char* buffer_dir = getenv(TANGRAM_BUFFER_DIR_ENV);
    if(!persist_dir || !buffer_dir) {
        tangram_info("ERROR: Please set %s and %s.\n\n", TANGRAM_PERSIST_DIR_ENV, TANGRAM_BUFFER_DIR_ENV);
        return;
    }

    realpath(persist_dir, tfs_info->persist_dir);
    realpath(buffer_dir, tfs_info->tfs_dir);

    const char* rpc_dev = getenv(TANGRAM_UCX_RPC_DEV_ENV);
    const char* rpc_tl = getenv(TANGRAM_UCX_RPC_TL_ENV);
    const char* rma_dev = getenv(TANGRAM_UCX_RMA_DEV_ENV);
    const char* rma_tl = getenv(TANGRAM_UCX_RMA_TL_ENV);

    if(!rpc_dev || !rpc_tl || !rma_dev || !rma_tl) {
        tangram_info("ERROR: Please set UCX device and transport.\n\n");
        return;
    }
    strcpy(tfs_info->rpc_dev_name, rpc_dev);
    strcpy(tfs_info->rpc_tl_name, rpc_tl);
    strcpy(tfs_info->rma_dev_name, rma_dev);
    strcpy(tfs_info->rma_tl_name, rma_tl);

    tfs_info->semantics = TANGRAM_STRONG_SEMANTICS;
    const char* semantics_str = getenv(TANGRAM_SEMANTICS_ENV);
    if(semantics_str)
        tfs_info->semantics = atoi(semantics_str);

    tfs_info->debug = false;
    const char* debug = getenv(TANGRAM_DEBUG_ENV);
    if(debug)
        tfs_info->debug = atoi(debug);

    tfs_info->use_delegator = false;
    const char* use_delegator = getenv(TANGRAM_USE_DELEGATOR_ENV);
    if(use_delegator)
        tfs_info->use_delegator = atoi(use_delegator);
}

void tangram_info_finalize(tfs_info_t *tfs_info) {
    MPI_Comm_free(&tfs_info->mpi_comm);
    MPI_Comm_free(&tfs_info->mpi_intra_comm);
}

double tangram_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (time.tv_sec + ((double)time.tv_usec / 1000000));
}

