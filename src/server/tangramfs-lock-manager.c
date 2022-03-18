#include <stdio.h>
#include <assert.h>
#include "utlist.h"
#include "uthash.h"
#include "lock-token.h"
#include "tangramfs-utils.h"
#include "tangramfs-lock-manager.h"

typedef struct lock_table {
    char filename[256];
    lock_token_list_t token_list;
    UT_hash_handle hh;
} lock_table_t;


// Hash Map <filename, seg_tree>
static lock_table_t *g_lt = NULL;


lock_token_t* tangram_lockmgr_acquire_lock(tangram_uct_addr_t* client, char* filename, size_t offset, size_t count, int type) {

    lock_table_t* entry = NULL;
    HASH_FIND_STR(g_lt, filename, entry);

    if(!entry) {
        entry = tangram_malloc(sizeof(lock_table_t));
        lock_token_list_init(&entry->token_list);
        strcpy(entry->filename, filename);
        HASH_ADD_STR(g_lt, filename, entry);
    }

    lock_token_t* token = NULL;
    token = lock_token_find_conflict(&entry->token_list, offset, count);

    // No one has the lock for the rank yet
    // We can safely grant the lock
    if(!token) {
        token = lock_token_add(&entry->token_list, offset, count, type, client);
        return token;
    }


    // Someone has already held the lock

    // Case 1. Both are read locks
    if(type == token->type && type == LOCK_TYPE_RD) {
    // Case 2. Different lock type, revoke the current owner's lock
    // Then delete the old token and add a new one
    } else {
        size_t data_len;
        void* data = rpc_in_pack(filename, 1, &offset, &count, NULL, &data_len);
        tangram_ucx_revoke_lock(token->owner, data, data_len, NULL);
        free(data);

        lock_token_delete(&entry->token_list, token);
    }

    token = lock_token_add(&entry->token_list, offset, count, type, client);
    return token;
}

void tangram_lockmgr_release_lock(tangram_uct_addr_t* client, char* filename, size_t offset, size_t count) {

    lock_table_t* entry = NULL;
    HASH_FIND_STR(g_lt, filename, entry);

    if(!entry) return;

    lock_token_t* token = NULL;
    token = lock_token_find_exact(&entry->token_list, offset, count);

    if(token && tangram_uct_addr_compare(token->owner, client) == 0)
        lock_token_delete(&entry->token_list, token);
}

void tangram_lockmgr_release_all_lock(tangram_uct_addr_t* client) {
    lock_table_t *entry, *tmp;
    HASH_ITER(hh, g_lt, entry, tmp) {
        lock_token_t *token, *tmp2;
        lock_token_delete_client(&entry->token_list, client);
    }
}


void tangram_lockmgr_init() {
}

void tangram_lockmgr_finalize() {
    // TODO: Remeber to release/free all data structures
}


