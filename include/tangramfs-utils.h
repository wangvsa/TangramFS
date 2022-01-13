#ifndef _TANGRAMFS_UTILS_H_
#define _TANGRAMFS_UTILS_H_
#include <mpi.h>
#include <stdbool.h>

#define PATH_MAX    4096

#define tangram_info(f_, ...) printf((f_), ##__VA_ARGS__)
#define tangram_debug(f_, ...)      \
    if(getenv(TANGRAM_DEBUG_ENV))   \
        printf((f_), ##__VA_ARGS__)

typedef struct tfs_info {
    int mpi_rank;
    int mpi_size;
    MPI_Comm mpi_comm;
    char tfs_dir[PATH_MAX];
    char persist_dir[PATH_MAX];

    char rpc_dev_name[32];
    char rpc_tl_name[32];
    char rma_dev_name[32];
    char rma_tl_name[32];

    int semantics;  // Strong, Session or Commit; only needed in passive mode.
    bool initialized;
    bool debug;

} tfs_info_t;


void* tangram_malloc(size_t size);
void  tangram_free(void*ptr, size_t size);

void tangram_get_info(tfs_info_t *tfs_info);
void tangram_release_info(tfs_info_t *tfs_info);

void tangram_write_uct_server_addr(void* dev_addr, size_t dev_addr_len, void* iface_addr, size_t iface_addr_len);
void tangram_read_uct_server_addr(void** dev_addr, void** iface_addr);

double tangram_wtime();

#endif
