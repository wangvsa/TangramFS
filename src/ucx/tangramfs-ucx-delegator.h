#ifndef _TANGRAMFS_UCX_DELEGATOR_H_
#define _TANGRAMFS_UCX_DELEGATOR_H_

#include "tangramfs-ucx-comm.h"

// Delegator
void tangram_ucx_delegator_init(tfs_info_t* tfs_info);
void tangram_ucx_delegator_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*));
void tangram_ucx_delegator_start();
void tangram_ucx_delegator_stop();
tangram_uct_addr_t* tangram_ucx_delegator_addr();

#endif
