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
void* rpc_handler(int8_t id, void* data, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(id == AM_ID_POST_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        for(int i = 0; i < in->num_intervals; i++)
            tangram_ms_handle_post(in->rank, in->filename, in->intervals[i].offset, in->intervals[i].count);
        //printf("post in->rank: %d, filename: %s, offset:%lu, count: %lu\n", in->rank, in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
    } else if(id == AM_ID_QUERY_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        *respond_len = sizeof(rpc_out_t);
        rpc_out_t *out = malloc(sizeof(rpc_out_t));
        bool found = tangram_ms_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count, &(out->rank));
        //printf("query in->rank: %d, filename: %s, offset:%lu, count: %lu, out->rank: %d\n",
        //        in->rank, in->filename, in->intervals[0].offset, in->intervals[0].count, out->rank);
        rpc_in_free(in);
        out->res = found ? QUERY_OK: QUERY_NOT_FOUND;
        respond = out;
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
        printf("Server started\n");
        tangram_ucx_server_register_rpc(rpc_handler);
        tangram_ucx_server_start();

    } else if( strcmp(argv[1], "stop") == 0 ) {
        tangram_rpc_service_start(&tfs_info);
        tangram_ucx_stop_server();
        tangram_rpc_service_stop();
    }

    tangram_release_info(&tfs_info);
    MPI_Finalize();
    return 0;
}

