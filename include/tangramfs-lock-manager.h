#ifndef _TANGRAMFS_LOCK_MANAGER_H_
#define _TANGRAMFS_LOCK_MANAGER_H_
#include <stdbool.h>
#include <sys/stat.h>
#include "lock-token.h"
#include "tangramfs-rpc.h"

void tangram_lockmgr_init();
void tangram_lockmgr_finalize();
lock_token_t* tangram_lockmgr_acquire_lock(tangram_uct_addr_t* client, char* filename, size_t offset, size_t count, int type);
void tangram_lockmgr_release_lock(tangram_uct_addr_t* client, char* filename, size_t offset, size_t count);
void tangram_lockmgr_release_all_lock(tangram_uct_addr_t* client);

#endif
