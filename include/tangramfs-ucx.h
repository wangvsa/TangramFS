#ifndef _TANGRAMFS_UCX_H_
#define _TANGRAMFS_UCX_H_
#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
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
#define AM_ID_CLIENT_ADDR    10
#define AM_ID_MPI_SIZE       11


// Server
void tangram_ucx_server_init(TFS_Info* tfs_info);
void tangram_ucx_server_register_rpc(void* (*user_handler)(int8_t, void*, size_t*));
void tangram_ucx_server_start();

// Client
void tangram_ucx_sendrecv_server(uint8_t id, void* data, size_t length, void* respond);
void tangram_ucx_sendrecv_peer(uint8_t id, int dest, void* data, size_t length, void* respond);
void tangram_ucx_send_ep_addr(uint8_t id, int dest, void* data, size_t length);
void tangram_ucx_stop_server();
void tangram_ucx_rpc_service_start(TFS_Info* tfs_info);
void tangram_ucx_rpc_service_stop();

// RMA
void tangram_ucx_rma_service_start(TFS_Info *tfs_info, void* (*serve_rma_data)(void*, size_t*));
void tangram_ucx_rma_service_stop();
void tangram_ucx_rma_request(int dest_rank, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size);
// RMA - used internally by client
void set_peer_ep_addr(uct_ep_addr_t* addr);
void* rma_respond(void* arg);

#endif
