#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "tangramfs.h"
#include "tangramfs-metadata.h"

/**
 * Return a respond, can be NULL
 */
void* rpc_handler(int8_t id, tangram_uct_addr_t* client, void* data, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(id == AM_ID_POST_REQUEST) {
        tangram_uct_addr_t* client_dup = tangram_uct_addr_duplicate(client);
        rpc_in_t* in = rpc_in_unpack(data);
        for(int i = 0; i < in->num_intervals; i++)
            tangram_ms_handle_post(client_dup, in->filename, in->intervals[i].offset, in->intervals[i].count);
        tangram_debug("post, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
    } else if(id == AM_ID_QUERY_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_uct_addr_t *owner;
        owner = tangram_ms_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_debug("query, filename: %s, offset:%lu, count: %lu\n",
                in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = tangram_uct_addr_serialize(owner, respond_len);
    } else if(id == AM_ID_STAT_REQUEST) {
        char* path = data;
        *respond_len = sizeof(struct stat);
        respond = malloc(*respond_len);
        tangram_ms_handle_stat(path, (struct stat*) respond);
    }

    return respond;
}


int main(int argc, char* argv[]) {
    assert(argc == 2);
    MPI_Init(&argc, &argv);

    tfs_info_t tfs_info;
    tangram_get_info(&tfs_info);

    if( strcmp(argv[1], "start") == 0 ) {
        tangram_ucx_server_init(&tfs_info);
        tangram_info("[tangramfs] Server started\n");
        tangram_ucx_server_register_rpc(rpc_handler);
        tangram_ucx_server_start();

    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_rpc_service_start(&tfs_info);
        tangram_ucx_stop_server();
        tangram_rpc_service_stop();
        tangram_info("[tangramfs] Server stoped\n");
    }

    tangram_release_info(&tfs_info);
    MPI_Finalize();
    return 0;
}

