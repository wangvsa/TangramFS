#define _POSIX_C_SOURCE 200112L
#include <stdlib.h>
#include <string.h>
#include <alloca.h>
#include <assert.h>
#include <arpa/inet.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"
#define TAG                    0xCAFE
#define TTAG                    0xCAFF

static ucp_context_h g_ucp_context;
static char g_server_addr[128];

typedef struct my_session {
    ucp_worker_h ucp_worker;
    ucp_ep_h     client_ep;
    volatile bool send_complete;
    volatile bool received_respond;
    void* server_respond;
} my_session_t;

static char message[] = "I am client";



static ucs_status_t client_am_recv_cb(void *arg, const void *header, size_t header_length,
                              void *data, size_t length,
                              const ucp_am_recv_param_t *param) {
    my_session_t* session = (my_session_t*) arg;

    int rendezvous = 0;
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)
        rendezvous = 1;

    // Small messages will use eager protocol.
    // The exact size is defined by env variable UCX_RNDV_THRESH
    // So we should have received the data already.
    assert(rendezvous == 0);

    session->received_respond = true;
    //memcpy(session->server_respond, data, length);

    // return UCS_OK, ucp will free *data
    return UCS_OK;
}

void client_err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
    my_session_t *session = (my_session_t*) arg;
    printf("client_err_cb: received respond: %d, %s\n",
            session->received_respond, ucs_status_string(status));
    session->received_respond = true;
}



void connect_to_server(my_session_t *session) {
    ucp_ep_params_t ep_params;
    ucs_status_t    status;

    // Set socket addr
    struct sockaddr_in connect_addr;
    memset(&connect_addr, 0, sizeof(struct sockaddr_in));
    connect_addr.sin_family      = AF_INET;
    connect_addr.sin_addr.s_addr = inet_addr(g_server_addr);
    connect_addr.sin_port        = htons(UCX_SERVER_PORT);

    /*
    // Set am callback to receive respond from server
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB |
                          UCP_AM_HANDLER_PARAM_FIELD_ARG;
    am_param.id         = UCX_AM_ID_DATA;
    am_param.cb         = client_am_recv_cb;
    am_param.arg        = session;
    status              = ucp_worker_set_am_recv_handler(session->ucp_worker, &am_param);
    assert(status == UCS_OK);
    */

    // Create EP to connect
    ep_params.field_mask       = UCP_EP_PARAM_FIELD_FLAGS       |
                                 UCP_EP_PARAM_FIELD_SOCK_ADDR   |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLER |
                                 UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.err_mode         = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb   = client_err_cb;
    ep_params.err_handler.arg  = session;
    ep_params.flags            = UCP_EP_PARAMS_FLAGS_CLIENT_SERVER;
    ep_params.sockaddr.addr    = (struct sockaddr*)&connect_addr;
    ep_params.sockaddr.addrlen = sizeof(connect_addr);

    status = ucp_ep_create(session->ucp_worker, &ep_params, &session->client_ep);
    assert(status == UCS_OK);
}

void init_and_connect(my_session_t* session) {
    init_context(&g_ucp_context);
    // TODO thread mode here
    init_worker(g_ucp_context, &session->ucp_worker, false);
    connect_to_server(session);
}

static void client_tag_send_cb(void *request, ucs_status_t status, void *user_data) {
    my_session_t* session = (my_session_t*) user_data;
    session->send_complete = true;
}
static void client_tag_recv_cb(void *request, ucs_status_t status, const ucp_tag_recv_info_t *info, void *user_data) {
    my_session_t *session = (my_session_t*) user_data;
    session->received_respond = true;
}


// Send and wait for the respond from server
void tangram_ucx_sendrecv(int op, void* data, size_t length, void* respond) {
    my_session_t session;
    session.server_respond = NULL;
    session.received_respond = false;
    session.send_complete = false;

    init_and_connect(&session);

    //printf("Client: start sendrecv, op: %d, size: %lu\n", op, length);

    /*
    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask   = UCP_OP_ATTR_FIELD_FLAGS;
    am_params.flags = UCP_AM_SEND_FLAG_EAGER;
    void *request = ucp_am_send_nbx(session.client_ep, UCX_AM_ID_DATA, &op, sizeof(int), data, length, &am_params);
    request_finalize(session.ucp_worker, request);
    */

    int msg = 999;
    ucp_request_param_t tag_param;
    tag_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_USER_DATA;
    tag_param.user_data    = &session;
    tag_param.cb.send      = client_tag_send_cb;
    void* request = ucp_tag_send_nbx(session.client_ep, &msg, sizeof(int), TAG, &tag_param);
    request_finalize(session.ucp_worker, request);
    while(!session.send_complete) {
        ucp_worker_progress(session.ucp_worker);
    }

    // Wait for the respond from server
    session.server_respond = respond;

    int ack;
    ucp_request_param_t tag_param2;
    tag_param2.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                              UCP_OP_ATTR_FIELD_USER_DATA;
    tag_param2.user_data    = &session;
    tag_param2.cb.recv      = client_tag_recv_cb;
    void* request2 = ucp_tag_recv_nbx(session.ucp_worker, &ack, sizeof(int), TTAG, 0, &tag_param2);
    request_finalize(session.ucp_worker, request2);
    if(!session.received_respond)
        printf("not received yet!\n");
    while(!session.received_respond) {
        ucp_worker_progress(session.ucp_worker);
    }

    sleep(2);
    ep_close(session.ucp_worker, session.client_ep);
    ucp_worker_destroy(session.ucp_worker);
    sleep(2);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_stop_server() {
    my_session_t session;
    init_and_connect(&session);

    /*
    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(session.client_ep, UCX_AM_ID_CMD, NULL, 0, NULL, 0, &am_params);
    request_finalize(session.ucp_worker, request);
    */

    int msg = 888;
    ucp_request_param_t tag_param;
    tag_param.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                             UCP_OP_ATTR_FIELD_USER_DATA;
    tag_param.user_data    = &session;
    tag_param.cb.send      = client_tag_send_cb;
    void* request = ucp_tag_send_nbx(session.client_ep, &msg, sizeof(int), TAG, &tag_param);
    request_finalize(session.ucp_worker, request);
    while(!session.send_complete) {
        ucp_worker_progress(session.ucp_worker);
    }

    ep_close(session.ucp_worker, session.client_ep);
    ucp_worker_destroy(session.ucp_worker);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_set_iface_addr(const char* iface, const char* ip_addr) {
    setenv("UCX_NET_DEVICES", iface, 0);
    strcpy(g_server_addr, ip_addr);
}
