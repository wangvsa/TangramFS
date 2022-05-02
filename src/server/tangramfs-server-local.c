#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <mpi.h>
#include "tangramfs-server-local.h"
#include "tangramfs-ucx.h"
#include "tangramfs-metadata-manager.h"
#include "tangramfs-lock-manager.h"

static lock_table_t *g_lt;


/**
 * Return a respond, can be NULL
 */
void* server_local_rpc_handler(int8_t id, tangram_uct_addr_t* client, void* data, uint8_t* respond_id, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(id == AM_ID_ACQUIRE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        assert(in->num_intervals == 1);
        lock_token_t* token = tangram_lockmgr_acquire_lock(g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count, in->intervals[0].type);
        assert(tangram_uct_addr_comp(token->owner, client) == 0);
        tangram_debug("[tangramfs] acquire lock, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        *respond_id = AM_ID_ACQUIRE_LOCK_RESPOND;
        respond = lock_token_serialize(token, respond_len);
    } else if(id == AM_ID_RELEASE_LOCK_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        assert(in->num_intervals == 1);
        tangram_debug("[tangramfs] release lock, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        tangram_lockmgr_release_lock(g_lt, client, in->filename, in->intervals[0].offset, in->intervals[0].count);
        //tangram_debug("[tangramfs] release lock success, filename: %s, offset:%lu, count: %lu\n", in->filename, in->intervals[0].offset, in->intervals[0].count);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_FILE_REQUEST) {
        rpc_in_t* in = rpc_in_unpack(data);
        tangram_lockmgr_release_lock_file(g_lt, client, in->filename);
        tangram_debug("[tangramfs] release lock file: %s\n", in->filename);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_FILE_RESPOND;
    } else if(id == AM_ID_RELEASE_LOCK_CLIENT_REQUEST) {
        tangram_lockmgr_release_lock_client(g_lt, client);
        tangram_debug("[tangramfs] release lock client.\n");
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        *respond_id = AM_ID_RELEASE_LOCK_CLIENT_RESPOND;
    }

    return respond;
}

void tangram_server_local_start(tfs_info_t* tfs_info) {

    tfs_info->role = TANGRAM_UCX_ROLE_LOCAL_SERVER;

    tangram_lockmgr_init(g_lt);
    tangram_ucx_server_init(tfs_info);
    tangram_ucx_server_register_rpc(server_local_rpc_handler);

    // Enter the progress loop and exit when the
    // stop command is received
    tangram_ucx_server_start();

    tangram_lockmgr_finalize(g_lt);
}
