#include <stdio.h>
#include "utlist.h"
#include "uthash.h"
#include "lock-token.h"
#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-lock-manager.h"
#include "tangramfs-ucx-server.h"
#include "tangramfs-ucx-delegator.h"

void* lock_acquire_result_serialize(lock_acquire_result_t* res, size_t* len) {

    *len = sizeof(int);
    void* buf;

    if(res->result == LOCK_ACQUIRE_SUCCESS) {
        size_t token_len;
        void* token_buf = lock_token_serialize(res->token, &token_len);

        *len += token_len;
        buf = malloc(*len);

        memcpy(buf, &res->result, sizeof(int));
        memcpy(buf+sizeof(int), token_buf, token_len);
        free(token_buf);
    }
    if(res->result == LOCK_ACQUIRE_CONFLICT) {
        size_t owner_len;
        void* owner_buf =  tangram_uct_addr_serialize(res->owner, &owner_len);

        *len += owner_len;
        buf = malloc(*len);

        memcpy(buf, &res->result, sizeof(int));
        memcpy(buf+sizeof(int), owner_buf, owner_len);
        free(owner_buf);
    }

    return buf;
}

lock_acquire_result_t* lock_acquire_result_deserialize(void* buf) {
    lock_acquire_result_t* res = malloc(sizeof(lock_acquire_result_t));
    res->token = NULL;
    res->owner = NULL;

    memcpy(&res->result, buf, sizeof(int));

    if(res->result == LOCK_ACQUIRE_SUCCESS) {
        size_t token_buf_size;
        res->token = lock_token_deserialize(buf+sizeof(int), &token_buf_size);
    }

    if(res->result == LOCK_ACQUIRE_CONFLICT) {
        res->owner = malloc(sizeof(tangram_uct_addr_t));
        tangram_uct_addr_deserialize(buf+sizeof(int), res->owner);
    }
    return res;
}

void split_lock(lock_token_list_t* token_list, lock_token_t* conflict_token, size_t offset, size_t count, bool server) {

    int conflict_start = lock_token_start(token_list, conflict_token);
    int conflict_end   = lock_token_end(token_list, conflict_token);

    int start = offset / LOCK_BLOCK_SIZE;
    int end   = (offset+count-1) / LOCK_BLOCK_SIZE;

    /**
     * Case 1: start >= conflict_start
     * conflict token:   |---|      |------|      |-------|
     * request token:  |-------| or |-------| or |-------|
     * relinquish [start, conflict_end]
     *
     * Case 2: start < conflict_start
     * conflict token: |------|        |--------|
     * request token:     |-------| or  |------|
     * relinquish [conflict_start, end]
     *
     */
    int new_start, new_end;

    // Case 1, shink the end
    if(start >= conflict_start) {
        //printf("Case 1, server: %d, conflict: [%d-%d], ask: [%d-%d]\n", server, conflict_start, conflict_end, start, end);
        new_start = conflict_start;
        new_end   = start - 1;
    }
    // Case 2
    else {
        //printf("Case 2, server: %d, conflict: [%d-%d], ask: [%d-%d]\n", server, conflict_start, conflict_end, start, end);
        new_start = end + 1;
        new_end   = conflict_end;
    }

    if(new_start < new_end)
        lock_token_update_range(token_list, conflict_token, new_start, new_end);
    else
        lock_token_delete(token_list, conflict_token);
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
        if(lock_token_type(&entry->token_list, token) != type && type == LOCK_TYPE_WR) {
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

    while(res->result == LOCK_ACQUIRE_CONFLICT) {
        // 1. Ask the conflict token onwer to split and release the conflicting range
        void* ack;
        tangram_ucx_delegator_sendrecv_delegator(AM_ID_SPLIT_LOCK_REQUEST, res->owner, in, in_size, &ack);
        free(ack);

        // 2. Once the conflict owner release its token, we can acquire it
        // from the server again
        free(out);
        tangram_ucx_delegator_sendrecv_server(AM_ID_ACQUIRE_LOCK_REQUEST, in, in_size, &out);

        // 3. Check if we were indeed granted the token
        // Reason for while() loop due to a very rare
        // case, where before we complete the whole process
        // of handling the conflict, someone else acquired the same
        // lock token before us.
        // i.e.,
        // the conflict owner released its token,
        // then someone acquired the same token before us.
        tangram_uct_addr_free(res->owner);
        free(res->owner);
        free(res);
        res = lock_acquire_result_deserialize(out);
    }

    // Exit while loop indicates the token was successfully granted
    tangram_assert(res->result == LOCK_ACQUIRE_SUCCESS);
    lock_token_add_direct(&entry->token_list, res->token);

    // only free res, res->owner should be NULL, res->token will be returned;
    token = res->token;
    free(res);
    free(in);
    free(out);
    return token;
}


// Someone tries to request a lock that conflicts with ours
// so we are asked to split our lock.
void tangram_lockmgr_delegator_split_lock(lock_table_t* lt, char* filename, size_t offset, size_t count, int type) {
    lock_table_t* entry = NULL;
    HASH_FIND_STR(lt, filename, entry);
    if(!entry) return;

    lock_token_t* token;
    token = lock_token_find_conflict(&entry->token_list, offset, count);

    if(token != NULL) {
        split_lock(&entry->token_list, token, offset, count, false);
    } else {
        printf("CHEN Ask me to split lock, but no conflict found!\n");
    }

    // Once I split my token and released locally partial of it,
    // notify the server (use release lock request) and ask it to do the same
    void* ack;
    size_t in_size;
    void* in = rpc_in_pack(filename, 1, &offset, &count, &type, &in_size);
    tangram_ucx_delegator_sendrecv_server(AM_ID_RELEASE_LOCK_REQUEST, in, in_size, &ack);
    free(ack);
}

lock_acquire_result_t* tangram_lockmgr_server_acquire_lock(lock_table_t** lt, tangram_uct_addr_t* delegator, char* filename, size_t offset, size_t count, int type, int lock_algo) {
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
            lock_token_update_type(&entry->token_list, result->token, type);
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
        if(lock_algo == TANGRAM_LOCK_ALGO_EXACT)
            result->token = lock_token_add_exact(&entry->token_list, offset, count, type, delegator);
        if(lock_algo == TANGRAM_LOCK_ALGO_EXTEND)
            result->token = lock_token_add_extend(&entry->token_list, offset, count, type, delegator);
        return result;
    }

    // Someone has already held the lock
    int conflict_start = lock_token_start(&entry->token_list, conflict_token);
    int conflict_end   = lock_token_end(&entry->token_list, conflict_token);
    int conflict_type  = lock_token_type(&entry->token_list, conflict_token);
    tangram_uct_addr_t* conflict_owner = lock_token_owner(&entry->token_list, conflict_token);

    // Case 1. Both are read locks
    if(type == conflict_type && type == LOCK_TYPE_RD) {
    // Case 2. Different lock type, split the current owner's lock
    // the requestor is responsible for contatcing the owner
    //
    // TODO we don't consider the case where we have multiple conflicting owners.
    // e.g. P1:[0-10], P2:[10-20], Accquire[0-20]
    } else {
        //printf("server found conflict: [%d-%d], ask: [%ld-%ld]\n", conflict_start, conflict_end, offset/4096, (offset+count-1)/4096);
        result->result = LOCK_ACQUIRE_CONFLICT;
        result->owner  = tangram_uct_addr_duplicate(conflict_owner);
        result->token  = NULL;
    }

    return result;
}

void tangram_lockmgr_server_release_lock(lock_table_t* lt, tangram_uct_addr_t* delegator, char* filename, size_t offset, size_t count) {
    lock_table_t* entry = NULL;
    HASH_FIND_STR(lt, filename, entry);

    if(!entry) return;

    lock_token_t* token = NULL;
    token = lock_token_find_conflict(&entry->token_list, offset, count);

    if(token && tangram_uct_addr_compare(token->owner, delegator) == 0)
        split_lock(&entry->token_list, token, offset, count, true);
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

