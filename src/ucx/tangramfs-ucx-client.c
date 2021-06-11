#include <arpa/inet.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

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


void connect_to_server(tangram_ucx_context_t *context) {

    ucp_ep_params_t ep_params;
    ucs_status_t    status;

    // Set socket addr
    struct sockaddr_in connect_addr;
    memset(&connect_addr, 0, sizeof(struct sockaddr_in));
    connect_addr.sin_family      = AF_INET;
    connect_addr.sin_addr.s_addr = inet_addr(context->server_addr);
    connect_addr.sin_port        = htons(UCX_SERVER_PORT);

    // Set am callback to receive respond from server
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_DATA;
    am_param.cb         = client_am_recv_cb;
    status              = ucp_worker_set_am_recv_handler(context->ucp_worker, &am_param);
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

    status = ucp_ep_create(context->ucp_worker, &ep_params, &context->client_ep);
    assert(status == UCS_OK);
    printf("Client: connected with server\n");
}

void tangram_ucx_send(tangram_ucx_context_t *context, int op, void* data, size_t length) {
    init_context(&context->ucp_context);
    init_worker(context->ucp_context, &context->ucp_worker);

    connect_to_server(context);

    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(context->client_ep, UCX_AM_ID_DATA, &op, sizeof(int), data, length, &am_params);
    request_finalize(context->ucp_worker, request);

    ep_close(context->ucp_worker, context->client_ep);
    ucp_worker_destroy(context->ucp_worker);
    ucp_cleanup(context->ucp_context);
}

// Send and wait for the respond from server
void tangram_ucx_sendrecv(tangram_ucx_context_t *context, int op, void* data, size_t length, void* respond) {
    init_context(&context->ucp_context);
    init_worker(context->ucp_context, &context->ucp_worker);

    connect_to_server(context);

    printf("Client: start sendrecv, op: %d, size: %lu\n", op, length);

    // Active Message send
    ucp_request_param_t params;
    params.op_attr_mask   = 0;
    void *request = ucp_am_send_nbx(context->client_ep, UCX_AM_ID_DATA, &op, sizeof(int), data, length, &params);
    request_finalize(context->ucp_worker, request);

    // Wait the respond from server
    g_server_respond = respond;
    while(received_respond==0) {
        ucp_worker_progress(context->ucp_worker);
    }
    received_respond = 0;
    printf("Client: finished sendrecv, op: %d, size: %lu\n", op, length);

    ep_close(context->ucp_worker, context->client_ep);
    ucp_worker_destroy(context->ucp_worker);
    ucp_cleanup(context->ucp_context);
}

void tangram_ucx_stop_server(tangram_ucx_context_t *context) {
    init_context(&context->ucp_context);
    init_worker(context->ucp_context, &context->ucp_worker);

    connect_to_server(context);

    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(context->client_ep, UCX_AM_ID_CMD, NULL, 0, NULL, 0, &am_params);
    request_finalize(context->ucp_worker, request);

    ep_close(context->ucp_worker, context->client_ep);
    ucp_worker_destroy(context->ucp_worker);
    ucp_cleanup(context->ucp_context);
}

