#ifndef _TANGRAMFS_UCX_H_
#define _TANGRAMFS_UCX_H_
#include <ucp/api/ucp.h>

#define UCX_SERVER_PORT     13337

#define UCX_AM_ID_DATA          0
#define UCX_AM_ID_CMD           1
#define UCX_AM_ID_RMA           2

#define RPC_OP_POST       0
#define RPC_OP_QUERY      1
#define RPC_OP_STOP       2
#define RPC_OP_TRANSFER   3

#define OP_RMA_REQUEST          1001
#define OP_RMA_RESPOND          1002


// Server
void tangram_ucx_server_init();
void tangram_ucx_server_register_rpc(void* (*user_handler)(int, void*, size_t, size_t*));
void tangram_ucx_server_start();

// Client
void tangram_ucx_set_server_addr(const char* ip_addr);
void tangram_ucx_send(int op, void* data, size_t length);
void tangram_ucx_sendrecv(int op, void* data, size_t length, void* respond);
void tangram_ucx_stop_server();

void tangram_ucx_rma_service_start();
void tangram_ucx_rma_service_stop();

#endif
