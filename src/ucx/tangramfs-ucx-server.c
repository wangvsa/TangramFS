#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ucp/api/ucp.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

volatile static ucp_conn_request_h g_conn_request = NULL;    // connection between client-server

volatile static int complete = 0;
volatile static int server_running = 1;
volatile static int need_respond = 0;

void (*user_am_data_handler)(int op, void* data, size_t length);


static void server_am_send_cb(void *request, ucs_status_t status, void *user_data)
{
    printf("server am send cb\n");
}

static void server_conn_cb(ucp_conn_request_h conn_request, void *arg)
{
    ucp_conn_request_attr_t attr;
    ucs_status_t status;
    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    status = ucp_conn_request_query(conn_request, &attr);
    assert(status == UCS_OK);

    g_conn_request = conn_request;
}

ucs_status_t server_am_cb_data(void *arg, const void *header, size_t header_length,
                               void *data, size_t length,
                               const ucp_am_recv_param_t *param) {
    int rendezvous = 0;
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)
        rendezvous = 1;

    // Small messages will use eager protocol.
    // The exact size is defined by env variable UCX_RNDV_THRESH
    // So we should have received the data already.
    assert(rendezvous == 0);

    printf("Server: at server_am_cb_data()\n");
    int op = *(int*)header;

    (*user_am_data_handler)(op, data, length);

    // TODO
    if(op == 1)
        need_respond = 1;

    complete = 1;
    return UCS_OK;
}

ucs_status_t server_am_cb_cmd(void *arg, const void *header, size_t header_length,
                               void *data, size_t length,
                               const ucp_am_recv_param_t *param) {
    printf("Server: at server_am_cb_cmd()\n");
    server_running = 0;
    return UCS_OK;
}

void run_server(tangram_ucx_context_t *context) {
    ucp_listener_h   listener;
    ucp_listener_params_t params;
    ucp_listener_attr_t attr;

    ucs_status_t     status;

    // setup socket listen address
    struct sockaddr_in listen_addr;
    memset(&listen_addr, 0, sizeof(struct sockaddr_in));
    listen_addr.sin_family      = AF_INET;
    listen_addr.sin_addr.s_addr = INADDR_ANY;
    listen_addr.sin_port        = htons(UCX_SERVER_PORT);


    params.field_mask         = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR |
                                UCP_LISTENER_PARAM_FIELD_CONN_HANDLER;
    params.sockaddr.addr      = (const struct sockaddr*)&listen_addr;
    params.sockaddr.addrlen   = sizeof(listen_addr);
    params.conn_handler.cb    = server_conn_cb;
    params.conn_handler.arg   = NULL;   // can pass in some data

    // Create listener
    status = ucp_listener_create(context->ucp_worker, &params, &listener);
    assert(status == UCS_OK);
    printf("Server: listener created\n");

    // attr can be used to retrive the source adddres, port, etc.
    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    status = ucp_listener_query(listener, &attr);
    assert(status == UCS_OK);
    printf("Server: wait for connection\n");

    // Serve one client at a time
    while(server_running) {

        // Wait for connection
        while(g_conn_request == NULL) {
            ucp_worker_progress(context->ucp_worker);
        }
        printf("Server: connected with client\n");

        ucp_ep_params_t ep_params;
        ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                    UCP_EP_PARAM_FIELD_CONN_REQUEST;
        ep_params.conn_request    = g_conn_request;
        ep_params.err_handler.cb  = err_cb;
        ep_params.err_handler.arg = NULL;
        status = ucp_ep_create(context->am_data_worker, &ep_params, &context->server_ep);
        assert(status == UCS_OK);

        // Wait for receive one AM message from client
        // and end this session
        while(complete==0 && server_running) {
            ucp_worker_progress(context->am_data_worker);
        }
        if(need_respond)
            tangram_ucx_server_respond(context);

        printf("Server: end one session\n\n\n");
        complete = 0;
        need_respond = 0;
        g_conn_request = NULL;
        ep_close(context->am_data_worker, context->server_ep);
    }

    // Clean up
    ucp_listener_destroy(listener);
    ucp_worker_destroy(context->am_data_worker);
    ucp_worker_destroy(context->ucp_worker);
    ucp_cleanup(context->ucp_context);
}

void tangram_ucx_server_init(tangram_ucx_context_t *context) {
    init_context(&(context->ucp_context));
    init_worker(context->ucp_context, &(context->ucp_worker));
}

void tangram_ucx_server_register_rpc(tangram_ucx_context_t *context, void (*user_handler)(int, void*, size_t)) {

    user_am_data_handler = user_handler;

    // Set up Active Message data handler
    ucs_status_t status;
    init_worker(context->ucp_context, &context->am_data_worker);
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_DATA;   // This id should match the one in am_send_nbx()
    am_param.cb         = server_am_cb_data;
    status              = ucp_worker_set_am_recv_handler(context->am_data_worker, &am_param);
    assert(status == UCS_OK);

    ucp_am_handler_param_t am_param2;
    am_param2.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param2.id         = UCX_AM_ID_CMD;   // This id should match the one in am_send_nbx()
    am_param2.cb         = server_am_cb_cmd;
    status              = ucp_worker_set_am_recv_handler(context->am_data_worker, &am_param2);
    assert(status == UCS_OK);
}

void tangram_ucx_server_respond(tangram_ucx_context_t *context) {
    ucp_request_param_t am_params;
    am_params.op_attr_mask   = UCP_OP_ATTR_FIELD_CALLBACK;
    am_params.cb.send = (ucp_send_nbx_callback_t) server_am_send_cb;
    void *request = ucp_am_send_nbx(context->server_ep, UCX_AM_ID_DATA, NULL, 0, NULL, 0, &am_params);
    request_finalize(context->am_data_worker, request);
}

void tangram_ucx_server_start(tangram_ucx_context_t *context) {
    run_server(context);
}
