#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "tangramfs-server.h"
#include "tangramfs-ucx-server.h"
#include "tangramfs-ucx-client.h"
#include "tangramfs-metadata-manager.h"
#include "tangramfs-lock-manager.h"

static lock_table_t* g_lt;

/**
 * Return a respond, can be NULL
 */
void* server_rpc_handler(int8_t id, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(id == AM_ID_POST_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_debug("[tangramfs server] post, filename: %s, num_intervals: %d, offset:%lu, count: %lu\n",
                        in->filename, in->num_intervals, in->intervals[0].offset, in->intervals[0].count);

        for(int i = 0; i < in->num_intervals; i++)
            tangram_metamgr_handle_post(client, in->filename, in->intervals[i].offset, in->intervals[i].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_POST_RESPOND;
    } else if(id == AM_ID_UNPOST_FILE_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_metamgr_handle_unpost_file(client, in->filename);
        tangram_debug("[tangramfs server] unpost file: %s\n", in->filename);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_UNPOST_FILE_RESPOND;
        rpc_in_free(in);
    } else if(id == AM_ID_UNPOST_CLIENT_REQUEST) {
        tangram_metamgr_handle_unpost_client(client);
        tangram_debug("[tangramfs server] unpost client\n");
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_UNPOST_CLIENT_RESPOND;
    } else if(id == AM_ID_QUERY_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_uct_addr_t *owner;
        owner = tangram_metamgr_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_debug("[tangramfs server] query, filename: %s, offset:%lu, count: %lu\n",
                in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = tangram_uct_addr_serialize(owner, respond_len);
        *respond_id = AM_ID_QUERY_RESPOND;
    } else if(id == AM_ID_STAT_REQUEST) {
        char* path = data;
        *respond_len = sizeof(struct stat);
        respond = malloc(*respond_len);
        tangram_metamgr_handle_stat(path, (struct stat*) respond);
        *respond_id = AM_ID_STAT_RESPOND;
    } else if(id == AM_ID_ACQUIRE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        assert(in->num_intervals == 1);
        tangram_debug("[tangramfs server1] acquire lock, filename: %s, offset:%lu, count: %lu, g_lt: %p\n", in->filename, in->intervals[0].offset, in->intervals[0].count, g_lt);
        lock_token_t* token = tangram_lockmgr_acquire_lock(&g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count, in->intervals[0].type);
        assert(tangram_uct_addr_comp(token->owner, client) == 0);
        tangram_debug("[tangramfs server2] acquire lock, filename: %s, offset:%lu, count: %lu, g_lt: %p\n", in->filename, in->intervals[0].offset, in->intervals[0].count, g_lt);
        rpc_in_free(in);
        *respond_id = AM_ID_ACQUIRE_LOCK_RESPOND;
        respond = lock_token_serialize(token, respond_len);
    } else if(id == AM_ID_RELEASE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        assert(in->num_intervals == 1);
        tangram_debug("[tangramfs server] release lock, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_lockmgr_release_lock(g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count);
        //tangram_debug("[tangramfs server] release lock success, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_FILE_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_lockmgr_release_lock_file(g_lt, client, in->filename);
        tangram_debug("[tangramfs server] release lock file: %s\n", in->filename);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_FILE_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_CLIENT_REQUEST) {
        tangram_lockmgr_release_lock_client(g_lt, client);
        tangram_debug("[tangramfs server] release lock client.\n");
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_CLIENT_RESPOND;
    }

    return respond;
}

void tangram_server_start(tfs_info_t* tfs_info) {
    tangram_metamgr_init();
    tangram_lockmgr_init(&g_lt);
    tangram_ucx_server_init(tfs_info);
    tangram_ucx_server_register_rpc(server_rpc_handler);

    // Main thread will enther the progress loop
    // here. It will exit when the stop command
    // is received
    tangram_ucx_server_start(false);
    tangram_ucx_server_stop();

    tangram_metamgr_finalize();
    tangram_lockmgr_finalize(&g_lt);
}

int main(int argc, char* argv[]) {
    assert(argc == 2);

    MPI_Init(&argc, &argv);

    tfs_info_t tfs_info;
    tangram_info_init(&tfs_info);

    if( strcmp(argv[1], "start") == 0 ) {
        tfs_info.role = TANGRAM_UCX_ROLE_SERVER;
        tangram_info("[tangramfs] Global server started\n");
        tangram_server_start(&tfs_info);
    } else if( strcmp(argv[1], "stop") == 0 ) {
        tfs_info.role = TANGRAM_UCX_ROLE_CLIENT;
        tangram_rpc_service_start(&tfs_info, NULL);

        if(tfs_info.mpi_rank == 0)
            tangram_ucx_stop_server();

        tangram_rpc_service_stop(&tfs_info);
        tangram_info("[tangramfs] Global server stoped\n");
    }

    tangram_info_finalize(&tfs_info);
    MPI_Finalize();

    return 0;
}
