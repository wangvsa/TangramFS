#ifndef _TANGRAMFS_UCX_RMA_H_
#define _TANGRAMFS_UCX_RMA_H_
#include "tangramfs-ucx-comm.h"

// RMA
typedef struct tangram_rma_req {
    tangram_uct_addr_t src;

    void*    ep_addr;
    size_t   ep_addr_len;

    void*    dev_addr;
    size_t   dev_addr_len;

    void*    rkey;
    size_t   rkey_len;

    uint64_t mem_addr;

    void*    user_arg;
    size_t   user_arg_len;

    struct tangram_rma_req *next, *prev;
} tangram_rma_req_t;


void tangram_ucx_rma_service_start(tfs_info_t *tfs_info, void* (*serve_rma_data_cb)(void*, size_t*));
void tangram_ucx_rma_service_stop();
void tangram_ucx_rma_request(tangram_uct_addr_t* addr, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size);


tangram_uct_addr_t* tangram_ucx_rma_addr();

#endif
