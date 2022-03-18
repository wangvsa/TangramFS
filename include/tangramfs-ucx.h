#ifndef _TANGRAMFS_UCX_H_
#define _TANGRAMFS_UCX_H_
#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include "tangramfs-ucx-comm.h"
#include "tangramfs-utils.h"


#define UCX_SERVER_PORT         13432

#define AM_ID_QUERY_REQUEST  1
#define AM_ID_QUERY_RESPOND  2
#define AM_ID_POST_REQUEST   3
#define AM_ID_POST_RESPOND   4
#define AM_ID_RMA_REQUEST    5
#define AM_ID_RMA_RESPOND    6
#define AM_ID_RMA_EP_ADDR    7
#define AM_ID_STOP_REQUEST   8
#define AM_ID_STOP_RESPOND   9
#define AM_ID_STAT_REQUEST   12
#define AM_ID_STAT_RESPOND   13

#define AM_ID_ACQUIRE_LOCK_REQUEST      20
#define AM_ID_ACQUIRE_LOCK_RESPOND      21
#define AM_ID_RELEASE_LOCK_REQUEST      22
#define AM_ID_RELEASE_LOCK_RESPOND      23
#define AM_ID_RELEASE_ALL_LOCK_REQUEST  24
#define AM_ID_RELEASE_ALL_LOCK_RESPOND  25
#define AM_ID_REVOKE_LOCK_REQUEST       26
#define AM_ID_REVOKE_LOCK_RESPOND       27


// Server
void tangram_ucx_server_init(tfs_info_t* tfs_info);
void tangram_ucx_server_register_rpc(void* (*user_handler)(int8_t, tangram_uct_addr_t*, void*, uint8_t*, size_t*));
void tangram_ucx_server_start();
void tangram_ucx_revoke_lock(tangram_uct_addr_t* client, void* data, size_t length);

// Client
void tangram_ucx_sendrecv_server(uint8_t id, void* data, size_t length, void** respond_ptr);
void tangram_ucx_sendrecv_client(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr);
void tangram_ucx_send_ep_addr(uint8_t id, tangram_uct_addr_t* dest, void* data, size_t length);
void tangram_ucx_stop_server();
void tangram_ucx_rpc_service_start(tfs_info_t* tfs_info, void (*revoke_lock_cb)(void*));
void tangram_ucx_rpc_service_stop();
tangram_uct_addr_t* tangram_ucx_get_client_addr();

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
