#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include "utlist.h"
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

volatile static bool g_server_running = true;
static ucp_context_h g_ucp_context;
static ucp_worker_h  g_ucp_worker;              // for listening connections
static ucp_worker_h  g_am_data_worker;          // handle active messages
static ucp_ep_h      g_server_ep;               // connected ep with client


typedef struct my_session {
    ucp_conn_request_h conn_request;
    void* respond;
    size_t respond_len;
    volatile bool complete;
    struct my_session *next;
} my_session_t;

// TODO thread safe?
static my_session_t *g_sessions;
static my_session_t *current_session;


void* (*user_am_data_handler)(int op, void* data, size_t length, size_t *respond_len);


static void server_conn_cb(ucp_conn_request_h conn_request, void *arg)
{
    ucp_conn_request_attr_t attr;
    ucs_status_t status;
    attr.field_mask = UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR;
    status = ucp_conn_request_query(conn_request, &attr);
    assert(status == UCS_OK);

    my_session_t *session = malloc(sizeof(my_session_t));
    session->conn_request = conn_request;
    session->complete = false;
    session->respond = NULL;
    session->respond_len = 0;
    LL_PREPEND(g_sessions, session);
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
    assert(header);

    printf("Server: at server_am_cb_data()\n");
    int op = *(int*)header;

    current_session->complete = true;
    current_session->respond = (*user_am_data_handler)(op, data, length, &current_session->respond_len);
    printf("\tOp: %d, send respond: %d, len: %lu\n", op, (current_session->respond!=NULL), current_session->respond_len);

    return UCS_OK;
}

ucs_status_t server_am_cb_cmd(void *arg, const void *header, size_t header_length,
                               void *data, size_t length,
                               const ucp_am_recv_param_t *param) {
    printf("Server: at server_am_cb_cmd()\n");
    g_server_running = false;
    return UCS_OK;
}

void tangram_ucx_server_respond(void* respond, size_t len) {
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(g_server_ep, UCX_AM_ID_DATA, NULL, 0, respond, len, &am_params);
    request_finalize(g_am_data_worker, request);
}

void run_server() {
    ucp_listener_h listener;
    ucp_listener_params_t params;
    ucp_listener_attr_t attr;
    ucs_status_t status;

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
    status = ucp_listener_create(g_ucp_worker, &params, &listener);
    assert(status == UCS_OK);
    printf("Server: listener created\n");

    // attr can be used to retrive the source adddres, port, etc.
    attr.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR;
    status = ucp_listener_query(listener, &attr);
    assert(status == UCS_OK);

    // Serve one client at a time
    while(g_server_running) {

        // Check for connection
        ucp_worker_progress(g_ucp_worker);

        if(g_sessions != NULL) {
            current_session = g_sessions;
            printf("Server: connected with client\n");

            ucp_ep_params_t ep_params;
            ep_params.field_mask      = UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                        UCP_EP_PARAM_FIELD_CONN_REQUEST;
            ep_params.conn_request    = current_session->conn_request;
            ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
            ep_params.err_handler.cb  = err_cb;
            ep_params.err_handler.arg = NULL;
            status = ucp_ep_create(g_am_data_worker, &ep_params, &g_server_ep);
            assert(status == UCS_OK);

            // Wait for receive one AM message from client
            // and end this session
            while(g_server_running && !current_session->complete) {
                ucp_worker_progress(g_am_data_worker);
            }

            // If client required a resopnd
            if(current_session->respond) {
                tangram_ucx_server_respond(current_session->respond, current_session->respond_len);
                free(current_session->respond);
            }

            printf("Server: end one session\n\n\n");
            ep_close(g_am_data_worker, g_server_ep);
            LL_DELETE(g_sessions, current_session);
            free(current_session);
        }
    }

    // Clean up
    ucp_listener_destroy(listener);
    ucp_worker_destroy(g_am_data_worker);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_server_init(const char* interface, char* server_ip_addr) {
    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_ucp_worker, true);
    get_interface_ip_addr(interface, server_ip_addr);
}


void tangram_ucx_server_register_rpc(void* (*user_handler)(int, void*, size_t, size_t*)) {

    user_am_data_handler = user_handler;

    // Set up Active Message data handler
    ucs_status_t status;
    init_worker(g_ucp_context, &g_am_data_worker, true);
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_DATA;   // This id should match the one in am_send_nbx()
    am_param.cb         = server_am_cb_data;
    status              = ucp_worker_set_am_recv_handler(g_am_data_worker, &am_param);
    assert(status == UCS_OK);

    ucp_am_handler_param_t am_param2;
    am_param2.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param2.id         = UCX_AM_ID_CMD;   // This id should match the one in am_send_nbx()
    am_param2.cb         = server_am_cb_cmd;
    status              = ucp_worker_set_am_recv_handler(g_am_data_worker, &am_param2);
    assert(status == UCS_OK);
}

void tangram_ucx_server_start() {
    run_server();
}
