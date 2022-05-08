#ifndef _TANGRAMFS_UCX_CLIENT_H_
#define _TANGRAMFS_UCX_CLIENT_H_

#include "tangramfs-ucx-comm.h"

void tangram_ucx_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr);
void tangram_ucx_sendrecv_delegator(uint8_t id, void* data, size_t length, void** respond_ptr);
void tangram_ucx_sendrecv_client(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr);
void tangram_ucx_send_ep_addr(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length);
void tangram_ucx_client_start(tfs_info_t* tfs_info);
void tangram_ucx_client_stop();
void tangram_ucx_stop_delegator();
void tangram_ucx_stop_server();
tangram_uct_addr_t* tangram_ucx_client_addr();
size_t tangram_uct_am_short_max_size();

#endif
