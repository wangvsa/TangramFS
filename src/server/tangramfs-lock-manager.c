#include <stdio.h>
#include <assert.h>
#include "utlist.h"
#include "uthash.h"
#include "lock-token.h"
#include "tangramfs-utils.h"
#include "tangramfs-lock-manager.h"
#include "tangramfs-ucx-server.h"
#include "tangramfs-ucx-delegator.h"

void* lock_acquire_result_serialize(lock_acquire_result_t* res, size_t* len) {

    size_t owner_len, token_len;
    void* token_buf = lock_token_serialize(res->token, &token_len);

    void* buf;
    if(res->result == LOCK_ACQUIRE_SUCCESS) {
        *len = sizeof(int) + token_len;
        buf = malloc(*len);
        memcpy(buf, &res->result, sizeof(int));
        memcpy(buf+sizeof(int), token_buf, token_len);
    }
    if(res->result == LOCK_ACQUIRE_CONFLICT) {
        void* owner_buf =  tangram_uct_addr_serialize(res->owner, &owner_len);
        *len = sizeof(int) + owner_len + token_len;
        buf = malloc(*len);
        memcpy(buf, &res->result, sizeof(int));
        memcpy(buf+sizeof(int), token_buf, token_len);
        memcpy(buf+sizeof(int)+token_len, owner_buf, owner_len);
        free(owner_buf);
    }

    free(token_buf);
    return buf;
}

lock_acquire_result_t* lock_acquire_result_deserialize(void* buf) {
    lock_acquire_result_t* res = malloc(sizeof(lock_acquire_result_t));
    memcpy(&res->result, buf, sizeof(int));

    size_t token_buf_size;
    res->token = lock_token_deserialize(buf+sizeof(int), &token_buf_size);

    if(res->result == LOCK_ACQUIRE_CONFLICT) {
        res->owner = malloc(sizeof(tangram_uct_addr_t));
        tangram_uct_addr_deserialize(buf+sizeof(int)+token_buf_size, res->owner);
    }
    return res;
}



lock_token_t* split_lock(lock_token_list_t* token_list, lock_token_t* conflict_token, size_t offset, size_t count, int type, tangram_uct_addr_t* owner, bool server) {

    int start = offset / LOCK_BLOCK_SIZE;
    int end   = (offset+count-1) / LOCK_BLOCK_SIZE;

    /**
     * Case 1:
     * conflict token:   |---|      |------|      |------|
     * request token:  |-------| or |-------| or |-------|
     *
     * Case 2:
     * conflict token: |------|
     * request token:     |-------|
     *
     * Case 3:
     * conflict token:    |------|
     * request token:  |-------|
     *
     * Case 4:
     * conflict token: |--------------|
     * request token:     |-------|
     */
    // Case 1, directly delete old token and then add the new one
    if(conflict_token->block_start >= start && conflict_token->block_end <= end) {
        lock_token_delete(token_list, conflict_token);
        if(server)
            return lock_token_add_extend(token_list, offset, count, type, owner);
    }
    // Case 2, shink the end
    else if(conflict_token->block_start < start && conflict_token->block_end < end) {
        conflict_token->block_end   = start - 1;
        if(server)
            return lock_token_add_extend(token_list, offset, count, type, owner);
    }
    // Case 3, shink the start
    else if(conflict_token->block_start > start && conflict_token->block_end >= end) {
        conflict_token->block_start = end + 1;
        if(server)
            return lock_token_add(token_list, offset, count, type, owner);
    }
    // Case 4, split the original lock into two
    else if(conflict_token->block_start <= start && conflict_token->block_end >= end) {
        int new_start, new_end;
        new_start = conflict_token->block_start;
        new_end   = start - 1;
        if(new_start <= new_end) {
            conflict_token->block_start = new_start;
            conflict_token->block_end   = new_end;
        }

        new_start = end + 1;
        new_end   = conflict_token->block_end;
        if(new_start <= new_end) {
            lock_token_add(token_list, new_start*LOCK_BLOCK_SIZE, new_end*LOCK_BLOCK_SIZE, conflict_token->type, conflict_token->owner);
        }
        if(server)
            return lock_token_add(token_list, offset, count, type, owner);
    }

    return NULL;
}


lock_token_t* tangram_lockmgr_delegator_acquire_lock(lock_table_t** lt, tangram_uct_addr_t* client, char* filename, size_t offset, size_t count, int type) {

    lock_table_t* entry = NULL;
    HASH_FIND_STR(*lt, filename, entry);

    if(!entry) {
        entry = malloc(sizeof(lock_table_t));
        lock_token_list_init(&entry->token_list);
        strcpy(entry->filename, filename);
        HASH_ADD_STR(*lt, filename, entry);
    }

    lock_token_t* token;

    // already hold the lock - 2 cases
    // TODO, need to compare client owner
    token = lock_token_find_cover(&entry->token_list, offset, count);
    if(token) {
        // Case 1:
        // Had the read lock but ask for a write lock
        // Delete my lock token locally and
        // ask the server to upgrade my lock
        // use the same AM_ID_ACQUIRE_LOCK_REQUEST RPC
        if(token->type != type && type == LOCK_TYPE_WR) {
            lock_token_delete(&entry->token_list, token);
        } else {
        // Case 2:
        // Had the write lock alraedy, nothing to do.
            return token;
        }
    }

    // Do not have the lock, ask server for it
    void* out;
    size_t in_size;
    void* in = rpc_in_pack(filename, 1, &offset, &count, &type, &in_size);
    tangram_ucx_delegator_sendrecv_server(AM_ID_ACQUIRE_LOCK_REQUEST, in, in_size, &out);

    lock_acquire_result_t* res = lock_acquire_result_deserialize(out);

    // No conflict, server simply granted us the lock
    if(res->result == LOCK_ACQUIRE_SUCCESS) {
    }
    // Server found conflict, now we need to contact the lock owner to ask
    // it to split the lock
    else if(res->result == LOCK_ACQUIRE_CONFLICT) {
        void* ack;
        tangram_ucx_delegator_sendrecv_delegator(AM_ID_SPLIT_LOCK_REQUEST, res->owner, in, in_size, &ack);
        free(ack);

        tangram_uct_addr_free(res->owner);
        free(res->owner);
    }

    lock_token_add_direct(&entry->token_list, res->token);

    free(res);
    free(in);
    free(out);
    return res->token;
}


// Someone tries to request a lock that conflicts with ours
// so we are asked to split our lock.
void tangram_lockmgr_delegator_split_lock(lock_table_t* lt, char* filename, size_t offset, size_t count, int type) {

    lock_table_t* entry = NULL;
    HASH_FIND_STR(lt, filename, entry);
    if(!entry) return;

    lock_token_t* token;
    token = lock_token_find_conflict(&entry->token_list, offset, count);

    if(token != NULL)
        split_lock(&entry->token_list, token, offset, count, type, TANGRAM_UCT_ADDR_IGNORE, false);
    else
        printf("Ask me to split lock, but no conflict found!\n");
}

lock_acquire_result_t* tangram_lockmgr_server_acquire_lock(lock_table_t** lt, tangram_uct_addr_t* delegator, char* filename, size_t offset, size_t count, int type) {
    lock_acquire_result_t *result = malloc(sizeof(lock_acquire_result_t));
    result->result = LOCK_ACQUIRE_SUCCESS;

    lock_table_t* entry = NULL;
    HASH_FIND_STR(*lt, filename, entry);

    if(!entry) {
        entry = malloc(sizeof(lock_table_t));
        lock_token_list_init(&entry->token_list);
        strcpy(entry->filename, filename);
        HASH_ADD_STR(*lt, filename, entry);
    }

    // First see if the requestor already hold the lock
    // but simply ask to upgrade it, i.e., RD->WR
    result->token = lock_token_find_cover(&entry->token_list, offset, count);
    if( result->token && (tangram_uct_addr_compare(result->token->owner, delegator) == 0) ) {
        if(type == LOCK_TYPE_WR)
            lock_token_update_type(result->token, type);
        return result;
    }

    lock_token_t* conflict_token = lock_token_find_conflict(&entry->token_list, offset, count);

    // No one has the lock for the range yet
    // We can safely grant the lock
    //
    // Two implementations:
    // 1. Grant the lock range as asked
    // 2. We can try to extend the lock range
    //    e.g., user asks for [0, 100], we can give [0, infinity]
    if(!conflict_token) {
        //token = lock_token_add(&entry->token_list, offset, count, type, delegator);
        result->token = lock_token_add_extend(&entry->token_list, offset, count, type, delegator);
        return result;
    }

    // Someone has already held the lock

    // Case 1. Both are read locks
    if(type == conflict_token->type && type == LOCK_TYPE_RD) {
    // Case 2. Different lock type, split the current owner's lock
    // the requestor is responsible for contatcing the owner
    //
    // TODO we don't consider the case where we have multiple conflicting owners.
    // e.g. P1:[0-10], P2:[10-20], Accquire[0-20]
    } else {
        result->result = LOCK_ACQUIRE_CONFLICT;
        result->owner  = tangram_uct_addr_duplicate(conflict_token->owner);
        result->token  = split_lock(&entry->token_list, conflict_token, offset, count, type, delegator, true);
    }

    return result;
}

void tangram_lockmgr_server_release_lock(lock_table_t* lt, tangram_uct_addr_t* client, char* filename, size_t offset, size_t count) {

    lock_table_t* entry = NULL;
    HASH_FIND_STR(lt, filename, entry);

    if(!entry) return;

    lock_token_t* token = NULL;
    token = lock_token_find_cover(&entry->token_list, offset, count);

    if(token && tangram_uct_addr_compare(token->owner, client) == 0)
        lock_token_delete(&entry->token_list, token);
}

void tangram_lockmgr_server_release_lock_file(lock_table_t* lt, tangram_uct_addr_t* client, char* filename) {
    lock_table_t* entry = NULL;
    HASH_FIND_STR(lt, filename, entry);

    if(entry) {
        lock_token_delete_client(&entry->token_list, client);
    }
}

void tangram_lockmgr_server_release_lock_client(lock_table_t* lt, tangram_uct_addr_t* client) {
    lock_table_t *entry, *tmp;
    HASH_ITER(hh, lt, entry, tmp) {
        lock_token_delete_client(&entry->token_list, client);
    }
}


void tangram_lockmgr_init(lock_table_t** lt) {
    *lt = NULL;
}

void tangram_lockmgr_finalize(lock_table_t** lt) {
    lock_table_t *entry, *tmp;
    HASH_ITER(hh, *lt, entry, tmp) {
        lock_token_list_destroy(&entry->token_list);
        HASH_DEL(*lt, entry);
        free(entry);
    }
    *lt = NULL;
}

