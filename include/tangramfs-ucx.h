#ifndef _TANGRAMFS_UCX_H_
#define _TANGRAMFS_UCX_H_
#include <ucp/api/ucp.h>

#define UCX_SERVER_PORT     13337

#define UCX_AM_ID_DATA          0
#define UCX_AM_ID_CMD           1
#define UCX_AM_ID_RMA           2

#define OP_RPC_POST             1001
#define OP_RPC_QUERY            1002
#define OP_RPC_STOP             1003
#define OP_RMA_REQUEST          2001
#define OP_RMA_RESPOND          2002


// Server
void tangram_ucx_server_init(const char* iface, char* ip_addr);
void tangram_ucx_server_register_rpc(void* (*user_handler)(int, void*, size_t, size_t*));
void tangram_ucx_server_start();

// Client
void tangram_ucx_set_iface_addr(const char* iface, const char* ip_addr);
void tangram_ucx_send(int op, void* data, size_t length);
void tangram_ucx_sendrecv(int op, void* data, size_t length, void* respond);
void tangram_ucx_stop_server();

void tangram_ucx_rma_service_start(void* (*serve_rma_data)(void*, size_t*));
void tangram_ucx_rma_service_stop();
void tangram_ucx_rma_request(int dest_rank, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size);

#endif
