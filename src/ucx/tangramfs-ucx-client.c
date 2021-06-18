#define _POSIX_C_SOURCE 200112L
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <arpa/inet.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

static ucp_context_h g_ucp_context;
static ucp_worker_h g_ucp_worker;
static char g_server_addr[128];
static ucp_ep_h g_client_ep;


// To return resopnd to user
void* g_server_respond;
volatile static int received_respond = 0;


static ucs_status_t client_am_recv_cb(void *arg, const void *header, size_t header_length,
                              void *data, size_t length,
                              const ucp_am_recv_param_t *param) {
    int rendezvous = 0;
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)
        rendezvous = 1;

    // Small messages will use eager protocol.
    // The exact size is defined by env variable UCX_RNDV_THRESH
    // So we should have received the data already.
    assert(rendezvous == 0);

    received_respond = 1;
    memcpy(g_server_respond, data, length);

    return UCS_OK;
}


void connect_to_server() {
    ucp_ep_params_t ep_params;
    ucs_status_t    status;

    // Set socket addr
    struct sockaddr_in connect_addr;
    memset(&connect_addr, 0, sizeof(struct sockaddr_in));
    connect_addr.sin_family      = AF_INET;
    connect_addr.sin_addr.s_addr = inet_addr(g_server_addr);
    connect_addr.sin_port        = htons(UCX_SERVER_PORT);

    // Set am callback to receive respond from server
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_DATA;
    am_param.cb         = client_am_recv_cb;
    status              = ucp_worker_set_am_recv_handler(g_ucp_worker, &am_param);
    assert(status == UCS_OK);

    // Create EP to connect
    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb   = err_cb;
    ep_params.err_handler.arg  = NULL;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr    = (struct sockaddr*)&connect_addr;
    ep_params.sockaddr.addrlen = sizeof(connect_addr);

    status = ucp_ep_create(g_ucp_worker, &ep_params, &g_client_ep);
    assert(status == UCS_OK);
    //printf("Client: connected with server\n");
}

void init_and_connect() {
    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_ucp_worker, true);
    connect_to_server();
}


void tangram_ucx_send(int op, void* data, size_t length) {
    init_and_connect();

    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(g_client_ep, UCX_AM_ID_DATA, &op, sizeof(int), data, length, &am_params);
    request_finalize(g_ucp_worker, request);

    ep_close(g_ucp_worker, g_client_ep);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
}

// Send and wait for the respond from server
void tangram_ucx_sendrecv(int op, void* data, size_t length, void* respond) {
    init_and_connect();

    //printf("Client: start sendrecv, op: %d, size: %lu\n", op, length);

    // Active Message send
    ucp_request_param_t params;
    params.op_attr_mask   = 0;
    void *request = ucp_am_send_nbx(g_client_ep, UCX_AM_ID_DATA, &op, sizeof(int), data, length, &params);
    request_finalize(g_ucp_worker, request);

    // Wait the respond from server
    g_server_respond = respond;
    while(received_respond==0) {
        ucp_worker_progress(g_ucp_worker);
    }
    received_respond = 0;
    //printf("Client: finished sendrecv, op: %d, size: %lu\n", op, length);

    ep_close(g_ucp_worker, g_client_ep);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_stop_server() {
    init_and_connect();

    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(g_client_ep, UCX_AM_ID_CMD, NULL, 0, NULL, 0, &am_params);
    request_finalize(g_ucp_worker, request);

    ep_close(g_ucp_worker, g_client_ep);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_set_iface_addr(const char* iface, const char* ip_addr) {
    setenv("UCX_NET_DEVICES", iface, 0);
    strcpy(g_server_addr, ip_addr);
}
