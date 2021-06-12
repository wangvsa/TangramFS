#ifndef _TANGRAMFS_UCX_H_
#define _TANGRAMFS_UCX_H_
#include <ucp/api/ucp.h>

#define UCX_SERVER_PORT     13337
#define UCX_AM_ID_DATA          0
#define UCX_AM_ID_CMD           1


typedef struct tangram_ucx_context {
    ucp_context_h ucp_context;
    ucp_worker_h  ucp_worker;       // for listen connections, server only
    ucp_worker_h  am_data_worker;   // for actual data communication, server only
    ucp_ep_h      server_ep;        // server only

    char          server_addr[128]; // client only
    ucp_ep_h      client_ep;        // client only
} tangram_ucx_context_t;


// Server
void tangram_ucx_server_init(tangram_ucx_context_t *context);
void tangram_ucx_server_register_rpc(tangram_ucx_context_t *context, void* (*user_handler)(int, void*, size_t, size_t*));
void tangram_ucx_server_start(tangram_ucx_context_t *context);

void tangram_mmap_recv_rkey(tangram_ucx_context_t *context, const void* rkey_buf);

// Client
void tangram_ucx_send(tangram_ucx_context_t *context, int op, void* data, size_t length);
void tangram_ucx_sendrecv(tangram_ucx_context_t *context, int op, void* data, size_t length, void* respond);
void tangram_ucx_stop_server(tangram_ucx_context_t *context);

void tangram_mmap_send_rkey(tangram_ucx_context_t *context, size_t length, ucp_mem_h* memh);

#endif
