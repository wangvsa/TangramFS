#include <stdio.h>
#include <assert.h>
#include "utlist.h"
#include "uthash.h"
#include "lock-token.h"
#include "tangramfs-utils.h"

#define BLOCK_SIZE 4096


void lock_token_free(lock_token_t* token) {
    tangram_uct_addr_free(token->owner);
    tangram_free(token, sizeof(lock_token_t));
    token = NULL;
}

lock_token_t* lock_token_find_conflict(lock_token_list_t* token_list, size_t offset, size_t count) {
    int start = offset / BLOCK_SIZE;
    int end   = (offset + count) / BLOCK_SIZE;

    pthread_rwlock_rdlock(&token_list->rwlock);

    lock_token_t* found = NULL;
    lock_token_t* token = NULL;
    LL_FOREACH(token_list->head, token) {
        if(start >= token->block_start && end <= token->block_end) {
            found = token;
            break;
        }
    }

    pthread_rwlock_unlock(&token_list->rwlock);

    return found;
}

lock_token_t* lock_token_find_cover(lock_token_list_t* token_list, size_t offset, size_t count) {
    int start = offset / BLOCK_SIZE;
    int end   = (offset + count) / BLOCK_SIZE;

    lock_token_t* found = NULL;
    lock_token_t* token = NULL;

    pthread_rwlock_rdlock(&token_list->rwlock);

    LL_FOREACH(token_list->head, token) {
        if(token->block_start <= start && token->block_end >= end) {
            found = token;
            break;
        }
    }

    pthread_rwlock_unlock(&token_list->rwlock);

    return found;
}

lock_token_t* lock_token_find_exact(lock_token_list_t* token_list, size_t offset, size_t count) {
    int start = offset / BLOCK_SIZE;
    int end   = (offset + count) / BLOCK_SIZE;

    pthread_rwlock_rdlock(&token_list->rwlock);

    lock_token_t* found = NULL;
    lock_token_t* token = NULL;
    LL_FOREACH(token_list->head, token) {
        if(start == token->block_start && end == token->block_end) {
            found = token;
            break;
        }
    }

    pthread_rwlock_unlock(&token_list->rwlock);

    return found;
}

void* lock_token_serialize(lock_token_t* token, size_t *size) {
    *size = sizeof(int) * 3;
    void* buf = tangram_malloc(*size);
    memcpy(buf, &token->block_start, sizeof(int));
    memcpy(buf+sizeof(int), &token->block_end, sizeof(int));
    memcpy(buf+2*sizeof(int), &token->type, sizeof(int));
    return buf;
}


lock_token_t* lock_token_add(lock_token_list_t* token_list, size_t offset, size_t count, int type, tangram_uct_addr_t* owner) {
    lock_token_t* token = tangram_malloc(sizeof(lock_token_t));
    token->block_start  = offset / BLOCK_SIZE;
    token->block_end    = (offset+count) / BLOCK_SIZE;
    token->type         = type;
    token->owner        = tangram_uct_addr_duplicate(owner);
    pthread_rwlock_wrlock(&token_list->rwlock);
    LL_APPEND(token_list->head, token);
    pthread_rwlock_unlock(&token_list->rwlock);
    return token;
}

lock_token_t* lock_token_add_from_buf(lock_token_list_t* token_list, void* buf) {
    lock_token_t* token = tangram_malloc(sizeof(lock_token_t));
    memcpy(&token->block_start, buf, sizeof(int));
    memcpy(&token->block_end, buf+sizeof(int), sizeof(int));
    memcpy(&token->type, buf+sizeof(int)+sizeof(int), sizeof(int));
    token->owner = TANGRAM_UCT_ADDR_IGNORE;
    pthread_rwlock_wrlock(&token_list->rwlock);
    LL_APPEND(token_list->head, token);
    pthread_rwlock_unlock(&token_list->rwlock);
    return token;
}

void lock_token_delete(lock_token_list_t* token_list, lock_token_t* token) {
    pthread_rwlock_wrlock(&token_list->rwlock);
    LL_DELETE(token_list->head, token);
    lock_token_free(token);
    pthread_rwlock_unlock(&token_list->rwlock);
}

void lock_token_delete_client(lock_token_list_t* token_list, tangram_uct_addr_t* client) {
    pthread_rwlock_wrlock(&token_list->rwlock);
    lock_token_t *token, *tmp;
    LL_FOREACH_SAFE(token_list->head, token, tmp) {
        if(0 == tangram_uct_addr_compare(token->owner, client)) {
            LL_DELETE(token_list->head, token);
            lock_token_free(token);
        }
    }
    pthread_rwlock_unlock(&token_list->rwlock);
}


void lock_token_list_init(lock_token_list_t* token_list) {
    token_list->head = NULL;
    pthread_rwlock_init(&token_list->rwlock, NULL);
}

void lock_token_list_destroy(lock_token_list_t* token_list) {
    pthread_rwlock_wrlock(&token_list->rwlock);
    lock_token_t *token, *tmp;
    LL_FOREACH_SAFE(token_list->head, token, tmp) {
        LL_DELETE(token_list->head, token);
        lock_token_free(token);
    }
    token_list->head = NULL;
    pthread_rwlock_unlock(&token_list->rwlock);
}

