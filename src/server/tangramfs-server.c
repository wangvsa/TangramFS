#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "tangramfs-server-local.h"
#include "tangramfs-server-global.h"


int main(int argc, char* argv[]) {
    assert(argc == 2);
    PMPI_Init(&argc, &argv);

    tfs_info_t tfs_info;
    tangram_get_info(&tfs_info);

    if( strcmp(argv[1], "gstart") == 0 ) {
        tangram_info("[tangramfs] Server global started\n");
        tangram_server_global_start(&tfs_info);
    } else if( strcmp(argv[1], "lstart") == 0 ) {
        tangram_info("[tangramfs] Server local started\n");
        // Run one local server (one process) on each node
        tangram_server_local_start(&tfs_info);
    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_rpc_service_start(&tfs_info, NULL);
        if(tfs_info.mpi_rank == 0)
            tangram_ucx_stop_global_server();
        if(tfs_info.use_local_server)
            tangram_ucx_stop_local_server();
        tangram_rpc_service_stop();
        tangram_info("[tangramfs] Server stoped\n");
    }


    PMPI_Barrier(tfs_info.mpi_comm);
    tangram_release_info(&tfs_info);
    PMPI_Finalize();
    return 0;
}

