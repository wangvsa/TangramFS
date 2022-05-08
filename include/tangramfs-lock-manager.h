#ifndef _TANGRAMFS_LOCK_MANAGER_H_
#define _TANGRAMFS_LOCK_MANAGER_H_
#include <stdbool.h>
#include <sys/stat.h>
#include "lock-token.h"
#include "tangramfs-rpc.h"

typedef struct lock_table {
    char filename[256];
    lock_token_list_t token_list;
    UT_hash_handle hh;
} lock_table_t;

void tangram_lockmgr_init(lock_table_t** lt);
void tangram_lockmgr_finalize(lock_table_t** lt);

// Delegator has its own acquire lock function
lock_token_t* tangram_lockmgr_delegator_acquire_lock(lock_table_t** lt, tangram_uct_addr_t* client, char* filename, size_t offset, size_t count, int type);
lock_token_t* tangram_lockmgr_server_acquire_lock(lock_table_t** lt, tangram_uct_addr_t* client, char* filename, size_t offset, size_t count, int type);

void tangram_lockmgr_delegator_revoke_lock(lock_table_t* lt, char* filename, size_t offset, size_t count, int type);

// Thse three functions are used by both delegator and server
void tangram_lockmgr_server_release_lock(lock_table_t* lt, tangram_uct_addr_t* client, char* filename, size_t offset, size_t count);
void tangram_lockmgr_server_release_lock_file(lock_table_t* lt, tangram_uct_addr_t* client, char* filename);
void tangram_lockmgr_server_release_lock_client(lock_table_t* lt, tangram_uct_addr_t* client);


#endif
