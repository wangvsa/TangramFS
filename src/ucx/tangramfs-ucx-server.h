#ifndef _TANGRAMFS_UCX_SERVER_H_
#define _TANGRAMFS_UCX_SERVER_H_
#include "tangramfs-ucx-comm.h"

// Server
void tangram_ucx_server_init(tfs_info_t* tfs_info);
void tangram_ucx_server_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*));
void tangram_ucx_server_start();
void tangram_ucx_server_stop();
void tangram_ucx_server_revoke_delegator_lock(tangram_uct_addr_t* client, void* data, size_t length);
tangram_uct_addr_t* tangram_ucx_server_addr();

#endif
