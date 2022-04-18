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


// Hash Map <filename, token_list>
static lock_table_t *g_lt;


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

    // First see if the requestor already hold the lock
    // but simply ask to upgrade it, i.e., RD->WR
    token = lock_token_find_cover(&entry->token_list, offset, count);
    if( token && (tangram_uct_addr_compare(token->owner , client) == 0) ) {
        if(type == LOCK_TYPE_WR)
            lock_token_update_type(token, type);
        return token;
    }


    token = lock_token_find_conflict(&entry->token_list, offset, count);

    // No one has the lock for the range yet
    // We can safely grant the lock
    //
    // Two implementations:
    // 1. Grant the lock range as asked
    // 2. We can try to extend the lock range
    //    e.g., user asks for [0, 100], we can give [0, infinity]
    if(!token) {
        token = lock_token_add(&entry->token_list, offset, count, type, client);
        //token = lock_token_add_extend(&entry->token_list, offset, count, type, client);
        return token;
    }

    // Someone has already held the lock

    // Case 1. Both are read locks
    if(type == token->type && type == LOCK_TYPE_RD) {

    // Case 2. Different lock type, revoke the current owner's lock
    // Then delete the old token and add a new one
    //
    // TODO we don't consider the case wehere we have multiple conflicting owners.
    // e.g. P1:[0-10], P2:[10-20], Accquire[0-20]
    } else {
        size_t data_len;
        void* data = rpc_in_pack(filename, 1, &offset, &count, NULL, &data_len);

        tangram_ucx_revoke_lock(token->owner, data, data_len);
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
    token = lock_token_find_cover(&entry->token_list, offset, count);

    if(token && tangram_uct_addr_compare(token->owner, client) == 0)
        lock_token_delete(&entry->token_list, token);
}

void tangram_lockmgr_release_lock_file(tangram_uct_addr_t* client, char* filename) {
    lock_table_t* entry = NULL;
    HASH_FIND_STR(g_lt, filename, entry);

    if(entry) {
        lock_token_delete_client(&entry->token_list, client);
    }
}

void tangram_lockmgr_release_lock_client(tangram_uct_addr_t* client) {
    lock_table_t *entry, *tmp;
    HASH_ITER(hh, g_lt, entry, tmp) {
        lock_token_delete_client(&entry->token_list, client);
    }
}

void tangram_lockmgr_init() {
    g_lt = NULL;
}

void tangram_lockmgr_finalize() {
    lock_table_t *entry, *tmp;
    HASH_ITER(hh, g_lt, entry, tmp) {
        lock_token_list_destroy(&entry->token_list);
        HASH_DEL(g_lt, entry);
        tangram_free(entry, sizeof(lock_table_t*));
    }
}

