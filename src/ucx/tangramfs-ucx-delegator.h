#ifndef _TANGRAMFS_UCX_DELEGATOR_H_
#define _TANGRAMFS_UCX_DELEGATOR_H_

#include "tangramfs-ucx-comm.h"

// Delegator
void tangram_ucx_delegator_init(tfs_info_t* tfs_info);
void tangram_ucx_delegator_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*));
void tangram_ucx_delegator_start();
void tangram_ucx_delegator_stop();

void tangram_ucx_delegator_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr);
void tangram_ucx_delegator_sendrecv_delegator(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr);

tangram_uct_addr_t* tangram_ucx_delegator_intra_addr();
tangram_uct_addr_t* tangram_ucx_delegator_inter_addr();

#endif
