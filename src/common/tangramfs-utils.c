#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>
#include <assert.h>

#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-posix-wrapper.h"

static size_t memory_usage = 0;

void* tangram_malloc(size_t size) {
    memory_usage += size;
    return malloc(size);
}

void tangram_free(void* ptr, size_t size) {
    memory_usage -= size;
    free(ptr);
}

void tangram_get_info(tfs_info_t* tfs_info) {
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
}

void tangram_release_info(tfs_info_t *tfs_info) {
    MPI_Comm_free(&tfs_info->mpi_comm);
    MPI_Comm_free(&tfs_info->mpi_intra_comm);
}

void fill_addr_config_filename(char* cfg_path) {
    const char* persist_dir = getenv(TANGRAM_PERSIST_DIR_ENV);
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);
}

void tangram_write_uct_server_addr(void* dev_addr, size_t dev_addr_len,
                                   void* iface_addr, size_t iface_addr_len) {
    tangram_map_real_calls();

    char cfg_path[512] = {0};
    fill_addr_config_filename(cfg_path);

    FILE* f = TANGRAM_REAL_CALL(fopen)(cfg_path, "wb");
    assert(f != NULL);

    TANGRAM_REAL_CALL(fwrite)(&dev_addr_len, sizeof(dev_addr_len), 1, f);
    TANGRAM_REAL_CALL(fwrite)(dev_addr, 1, dev_addr_len, f);
    TANGRAM_REAL_CALL(fwrite)(&iface_addr_len, sizeof(iface_addr_len), 1, f);
    TANGRAM_REAL_CALL(fwrite)(iface_addr, 1, iface_addr_len, f);
    TANGRAM_REAL_CALL(fflush)(f);
    TANGRAM_REAL_CALL(fclose)(f);
}

void tangram_read_uct_server_addr(void** dev_addr, size_t* dev_addr_len,
                                  void** iface_addr, size_t* iface_addr_len) {
    tangram_map_real_calls();

    char cfg_path[512] = {0};
    fill_addr_config_filename(cfg_path);

    FILE* f = TANGRAM_REAL_CALL(fopen)(cfg_path, "r");
    assert(f != NULL);  // this assert does not work on Quart/Catalyst

    TANGRAM_REAL_CALL(fread)(dev_addr_len, sizeof(size_t), 1, f);
    *dev_addr = malloc(*dev_addr_len);
    TANGRAM_REAL_CALL(fread)(*dev_addr, 1, *dev_addr_len, f);
    TANGRAM_REAL_CALL(fread)(iface_addr_len, sizeof(size_t), 1, f);
    *iface_addr = malloc(*iface_addr_len);
    TANGRAM_REAL_CALL(fread)(*iface_addr, 1, *iface_addr_len, f);

    TANGRAM_REAL_CALL(fclose)(f);
}


double tangram_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (time.tv_sec + ((double)time.tv_usec / 1000000));
}

