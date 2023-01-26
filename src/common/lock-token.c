#include <stdio.h>
#include <limits.h>
#include "utlist.h"
#include "uthash.h"
#include "lock-token.h"
#include "tangramfs-utils.h"

void lock_token_free(lock_token_t* token) {
    tangram_uct_addr_free(token->owner);
    free(token->owner);
    free(token);
}

lock_token_t* lock_token_find_conflict(lock_token_list_t* token_list, size_t offset, size_t count) {
    int start = offset / LOCK_BLOCK_SIZE;
    int end   = (offset + count - 1) / LOCK_BLOCK_SIZE;

    pthread_rwlock_rdlock(&token_list->rwlock);

    lock_token_t* found = NULL;
    lock_token_t* token = NULL;
    LL_FOREACH(token_list->head, token) {
        if(start > token->block_end || end < token->block_start) {
            // non-overlapping
        } else {
            found = token;
        }
    }

    pthread_rwlock_unlock(&token_list->rwlock);

    return found;
}

lock_token_t* lock_token_find_cover(lock_token_list_t* token_list, size_t offset, size_t count) {
    int start = offset / LOCK_BLOCK_SIZE;
    int end   = (offset + count - 1) / LOCK_BLOCK_SIZE;

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
    int start = offset / LOCK_BLOCK_SIZE;
    int end   = (offset + count - 1) / LOCK_BLOCK_SIZE;

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
    size_t addr_size;
    void* addr_buf = tangram_uct_addr_serialize(token->owner, &addr_size);

    *size = sizeof(int) * 3 + sizeof(size_t) + addr_size ;
    void* buf = malloc(*size);
    memcpy(buf, &token->block_start, sizeof(int));
    memcpy(buf+sizeof(int), &token->block_end, sizeof(int));
    memcpy(buf+2*sizeof(int), &token->type, sizeof(int));
    memcpy(buf+3*sizeof(int), &addr_size, sizeof(size_t));

    // It is possible token->owner is TANGRAM_UCT_ADDR_IGNORE (which is just null)
    if(addr_size > 0)
        memcpy(buf+3*sizeof(int)+sizeof(size_t), addr_buf, addr_size);

    return buf;
}

lock_token_t* lock_token_deserialize(void* buf, size_t* size) {
    lock_token_t* token = malloc(sizeof(lock_token_t));

    memcpy(&token->block_start, buf, sizeof(int));
    memcpy(&token->block_end, buf+sizeof(int), sizeof(int));
    memcpy(&token->type, buf+sizeof(int)*2, sizeof(int));

    size_t addr_size;
    memcpy(&addr_size, buf+sizeof(int)*3, sizeof(size_t));
    if(addr_size > 0) {
        token->owner = malloc(sizeof(tangram_uct_addr_t));
        tangram_uct_addr_deserialize(buf+sizeof(int)*3+sizeof(size_t), token->owner);
    }

    *size = sizeof(int)*3 + sizeof(size_t) + addr_size;
    return token;
}
lock_token_t* lock_token_create(int start, int end, int type, tangram_uct_addr_t* owner) {
    lock_token_t* token = malloc(sizeof(lock_token_t));
    token->block_start = start;
    token->block_end   = end;
    token->type        = type;
    token->owner       = tangram_uct_addr_duplicate(owner);
    return token;
}

lock_token_t* lock_token_add_direct(lock_token_list_t* token_list, lock_token_t* token) {
    pthread_rwlock_wrlock(&token_list->rwlock);
    LL_APPEND(token_list->head, token);
    pthread_rwlock_unlock(&token_list->rwlock);
    return token;
}

lock_token_t* lock_token_add_exact(lock_token_list_t* token_list, size_t offset, size_t count, int type, tangram_uct_addr_t* owner) {
    lock_token_t* token = lock_token_create(offset/LOCK_BLOCK_SIZE, (offset+count-1)/LOCK_BLOCK_SIZE, type, owner);
    lock_token_add_direct(token_list, token);
    return token;
}

lock_token_t* lock_token_add_extend(lock_token_list_t* token_list, size_t offset, size_t count, int type, tangram_uct_addr_t* owner) {
    lock_token_t* token = lock_token_create(offset/LOCK_BLOCK_SIZE, (offset+count-1)/LOCK_BLOCK_SIZE, type, owner);
    int extend_start = 0;
    int extend_end   = INT_MAX;

    pthread_rwlock_wrlock(&token_list->rwlock);
    lock_token_t* tmp;

    int algo = 1;

    // Algorithm 1: Extend to the farest possible start and end block
    if(algo == 1) {
        LL_FOREACH(token_list->head, tmp) {
            if( (tmp->block_start > token->block_end) && (tmp->block_start-1 < extend_end ) ) {
                extend_end   = tmp->block_start - 1;
            }
            if( (tmp->block_end < token->block_start) && (tmp->block_end+1 > extend_start) ) {
                extend_start = tmp->block_end + 1;
            }
        }
    }

    // Algorithm 2: Extend to infinity or do not extend at all
    if(algo == 2) {
        LL_FOREACH(token_list->head, tmp) {
            if( tmp->block_start > token->block_end) {
                extend_end = token->block_end;
                break;
            }
        }
    }

    token->block_start = extend_start;
    token->block_end   = extend_end;
    LL_APPEND(token_list->head, token);
    pthread_rwlock_unlock(&token_list->rwlock);

    return token;
}


lock_token_t* lock_token_add_from_buf(lock_token_list_t* token_list, void* buf, tangram_uct_addr_t* owner) {
    lock_token_t* token = malloc(sizeof(lock_token_t));
    memcpy(&token->block_start, buf, sizeof(int));
    memcpy(&token->block_end, buf+sizeof(int), sizeof(int));
    memcpy(&token->type, buf+sizeof(int)+sizeof(int), sizeof(int));
    token->owner = tangram_uct_addr_duplicate(owner);;
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

void lock_token_update_type(lock_token_list_t* token_list, lock_token_t* token, int type) {
    pthread_rwlock_wrlock(&token_list->rwlock);
    token->type = type;
    pthread_rwlock_unlock(&token_list->rwlock);
}

int lock_token_update_range(lock_token_list_t* token_list, lock_token_t* token, int start, int end) {
    pthread_rwlock_wrlock(&token_list->rwlock);
    token->block_start = start;
    token->block_end   = end;
    pthread_rwlock_unlock(&token_list->rwlock);
}

int lock_token_start(lock_token_list_t* token_list, lock_token_t* token) {
    int start;
    pthread_rwlock_rdlock(&token_list->rwlock);
    start = token->block_start;
    pthread_rwlock_unlock(&token_list->rwlock);
    return start;
}

int lock_token_end(lock_token_list_t* token_list, lock_token_t* token) {
    int end;
    pthread_rwlock_rdlock(&token_list->rwlock);
    end = token->block_end;
    pthread_rwlock_unlock(&token_list->rwlock);
    return end;
}

int lock_token_type(lock_token_list_t* token_list, lock_token_t* token) {
    int type;
    pthread_rwlock_rdlock(&token_list->rwlock);
    type = token->type;
    pthread_rwlock_unlock(&token_list->rwlock);
    return type;
}

tangram_uct_addr_t* lock_token_owner(lock_token_list_t* token_list, lock_token_t* token) {
    tangram_uct_addr_t* owner;
    pthread_rwlock_rdlock(&token_list->rwlock);
    owner = token->owner;
    pthread_rwlock_unlock(&token_list->rwlock);
    return owner;
}

