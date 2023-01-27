#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include "tangramfs-server.h"
#include "tangramfs-ucx-server.h"
#include "tangramfs-ucx-client.h"
#include "tangramfs-metadata-manager.h"
#include "tangramfs-lock-manager.h"

static lock_table_t* g_lt;
static tfs_info_t    g_tfs_info;

/**
 * Return a respond, can be NULL
 */
void* server_rpc_handler(int8_t id, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(id == AM_ID_POST_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_debug("[tangramfs server] post, filename: %s, num_intervals: %d, offset:%luKB, size:%luKB\n",
                        in->filename, in->num_intervals, in->intervals[0].offset/1024, in->intervals[0].count/1024);

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

        tangram_uct_addr_t** owners = (tangram_uct_addr_t**)malloc(sizeof(tangram_uct_addr_t*)*in->num_intervals);
        *respond_len = in->num_intervals * sizeof(bool);
        void** tmp = (void**) malloc(sizeof(void*) * in->num_intervals);
        size_t* tmp_lens = (size_t*) malloc(in->num_intervals * sizeof(size_t));

        for(int i = 0; i < in->num_intervals; i++) {
            owners[i] = tangram_metamgr_handle_query(in->filename, in->intervals[i].offset, in->intervals[i].count);
            tmp[i] = tangram_uct_addr_serialize(owners[i], &tmp_lens[i]);
            *respond_len += tmp_lens[i];
        }

        respond = malloc(*respond_len);
        memset(respond, 0, *respond_len);

        void* ptr = respond;
        for(int i = 0; i < in->num_intervals; i++) {
            bool found = (tmp[i] != NULL);
            memcpy(ptr, &found, sizeof(bool));
            ptr += sizeof(bool);
            if(found) {
                memcpy(ptr, tmp[i], tmp_lens[i]);
                ptr += tmp_lens[i];
                free(tmp[i]);
            }
        }

        tangram_debug("[tangramfs server] query, filename: %s, offset:%luKB, count: %luKB\n",
                in->filename, in->intervals[0].offset/1024, in->intervals[0].count/1024);

        free(tmp);
        free(tmp_lens);
        free(owners);
        rpc_in_free(in);

        *respond_id = AM_ID_QUERY_RESPOND;
    } else if(id == AM_ID_STAT_REQUEST) {
        char* path = data;
        *respond_len = sizeof(struct stat);
        respond = malloc(*respond_len);
        tangram_metamgr_handle_stat(path, (struct stat*) respond);
        *respond_id = AM_ID_STAT_RESPOND;
    } else if(id == AM_ID_ACQUIRE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_assert(in->num_intervals == 1);
        //tangram_debug("[tangramfs server] acquire lock, filename: %s, ask [%ld-%ld] start\n",
        //        in->filename, in->intervals[0].offset/LOCK_BLOCK_SIZE, (in->intervals[0].offset+in->intervals[0].count-1)/LOCK_BLOCK_SIZE);
        lock_acquire_result_t* res = tangram_lockmgr_server_acquire_lock(&g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count, in->intervals[0].type, g_tfs_info.lock_algo);
        tangram_assert( tangram_uct_addr_compare(res->token->owner, client) == 0);

        if(res->result == LOCK_ACQUIRE_SUCCESS) {
            tangram_debug("[tangramfs server] acquire lock, filename: %s, ask [%ld-%ld], grant [%d-%d]\n",
                    in->filename, in->intervals[0].offset/LOCK_BLOCK_SIZE, (in->intervals[0].offset+in->intervals[0].count-1)/LOCK_BLOCK_SIZE, res->token->block_start, res->token->block_end);
        } else {
            tangram_debug("[tangramfs server] acquire lock, filename: %s, ask [%ld-%ld], need split!\n",
                    in->filename, in->intervals[0].offset/LOCK_BLOCK_SIZE, (in->intervals[0].offset+in->intervals[0].count-1)/LOCK_BLOCK_SIZE);
        }

        rpc_in_free(in);
        *respond_id = AM_ID_ACQUIRE_LOCK_RESPOND;
        respond = lock_acquire_result_serialize(res, respond_len);
    } else if(id == AM_ID_RELEASE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_assert(in->num_intervals == 1);
        tangram_debug("[tangramfs server] release lock, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_lockmgr_server_release_lock(g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count);
        //tangram_debug("[tangramfs server] release lock success, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_FILE_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_lockmgr_server_release_lock_file(g_lt, client, in->filename);
        tangram_debug("[tangramfs server] release lock file: %s\n", in->filename);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_FILE_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_CLIENT_REQUEST) {
        tangram_lockmgr_server_release_lock_client(g_lt, client);
        tangram_debug("[tangramfs server] release lock client.\n");
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_CLIENT_RESPOND;
    }

    return respond;
}

void tangram_server_start() {
    tangram_metamgr_init();
    tangram_lockmgr_init(&g_lt);
    tangram_ucx_server_init(&g_tfs_info);
    tangram_ucx_server_register_rpc(server_rpc_handler);

    // Main thread will enther the progress loop
    // here. It will exit when the stop command
    // is received
    tangram_ucx_server_start();
    tangram_ucx_server_stop();

    tangram_metamgr_finalize();
    tangram_lockmgr_finalize(&g_lt);
}

int main(int argc, char* argv[]) {
    tangram_assert(argc == 2);

    MPI_Init(&argc, &argv);

    tangram_info_init(&g_tfs_info);

    if( strcmp(argv[1], "start") == 0 ) {
        g_tfs_info.role = TANGRAM_UCX_ROLE_SERVER;
        tangram_info("[tangramfs] Global server started\n");
        tangram_server_start();
    } else if( strcmp(argv[1], "stop") == 0 ) {
        g_tfs_info.role = TANGRAM_UCX_ROLE_CLIENT;
        tangram_rpc_service_start(&g_tfs_info);

        if(g_tfs_info.mpi_rank == 0)
            tangram_ucx_stop_server();

        tangram_rpc_service_stop(&g_tfs_info);
        tangram_info("[tangramfs] Global server stoped\n");
    }

    tangram_info_finalize(&g_tfs_info);
    MPI_Finalize();

    return 0;
}
