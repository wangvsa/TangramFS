#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "tangramfs-server-global.h"

int main(int argc, char* argv[]) {
    assert(argc == 2);

    MPI_Init(&argc, &argv);

    tfs_info_t tfs_info;
    tangram_info_init(&tfs_info);

    if( strcmp(argv[1], "start") == 0 ) {
        tfs_info.role = TANGRAM_UCX_ROLE_SERVER;
        tangram_info("[tangramfs] Global server started\n");
        tangram_server_global_start(&tfs_info);
    } else if( strcmp(argv[1], "stop") == 0 ) {
        tfs_info.role = TANGRAM_UCX_ROLE_CLIENT;
        tangram_rpc_service_start(&tfs_info, NULL);

        if(tfs_info.mpi_rank == 0)
            tangram_ucx_stop_global_server();

        tangram_rpc_service_stop(&tfs_info);
        tangram_info("[tangramfs] Global server stoped\n");
    }

    tangram_info_finalize(&tfs_info);
    MPI_Finalize();

    return 0;
}
