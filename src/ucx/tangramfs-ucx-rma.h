#ifndef _TANGRAMFS_UCX_RMA_H_
#define _TANGRAMFS_UCX_RMA_H_
#include "tangramfs-ucx-comm.h"

// RMA
typedef struct tangram_rma_req_in {
    tangram_uct_addr_t src;
    void* data;
} tangram_rma_req_in_t;

void tangram_ucx_rma_service_start(tfs_info_t *tfs_info, void* (*serve_rma_data_cb)(void*, size_t*));
void tangram_ucx_rma_service_stop();
void tangram_ucx_rma_request(tangram_uct_addr_t* addr, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size);
// RMA - used internally by client
void set_peer_ep_addr(uct_ep_addr_t* addr);
void* rma_respond(void* arg);

#endif
