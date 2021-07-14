#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <alloca.h>
#include <arpa/inet.h>
#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include "utlist.h"
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

#define NUM_THREADS             8

volatile static bool g_server_running = true;
static ucp_context_h g_ucp_context;
static ucp_worker_h  g_ucp_worker;              // for listening connections

typedef struct my_session {
    ucp_conn_request_h conn_request;
    void* respond;
    size_t respond_len;
    volatile bool complete;

    ucp_worker_h  am_data_worker;          // handle active messages
    ucp_ep_h      server_ep;               // connected ep with client

    struct my_session *next, *prev;
} my_session_t;

struct my_worker {
    int tid;
    pthread_t thread;
    pthread_mutex_t lock;
    my_session_t *sessions;
};

// A thread pool, where each worker handles a list of my_session_t*
static struct my_worker g_my_workers[NUM_THREADS];
static int who = 0;

void* (*user_am_data_handler)(int op, void* data, size_t length, size_t *respond_len);


ucs_status_t server_am_cb_data(void *arg, const void *header, size_t header_length,
                               void *data, size_t length,
                               const ucp_am_recv_param_t *param) {
    my_session_t *session = (my_session_t*) arg;
    int rendezvous = 0;
    if (param->recv_attr & UCP_AM_RECV_ATTR_FLAG_RNDV)
        rendezvous = 1;

    // Small messages will use eager protocol.
    // The exact size is defined by env variable UCX_RNDV_THRESH
    // So we should have received the data already.
    assert(rendezvous == 0);
    assert(header);

    int op = *(int*)header;

    session->complete = true;
    session->respond = (*user_am_data_handler)(op, data, length, &session->respond_len);
    //printf("Server: op: %d, send respond: %d, len: %lu\n", op, (session->respond!=NULL), session->respond_len);

    // return UCS_OK, data will not be persisted. i.e., freed by UCP
    return UCS_OK;
}

ucs_status_t server_am_cb_cmd(void *arg, const void *header, size_t header_length,
                               void *data, size_t length,
                               const ucp_am_recv_param_t *param) {
    printf("Server: received stop server command!\n");
    g_server_running = false;

    return UCS_OK;
}


void tangram_ucx_server_respond(my_session_t *session) {
    ucp_request_param_t am_params;
    am_params.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    am_params.flags        = UCP_AM_SEND_FLAG_EAGER;

    void *request = ucp_am_send_nbx(session->server_ep, UCX_AM_ID_DATA, NULL, 0, session->respond, session->respond_len, &am_params);
    request_finalize(session->am_data_worker, request);
}


void server_err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
    my_session_t *session = (my_session_t*) arg;
    printf("server_err_cb: complete: %d, %s\n",
                session->complete, ucs_status_string(status));
    session->complete = true;
}


void handle_one_session(my_session_t* session) {
    ucs_status_t status;
    init_worker(g_ucp_context, &session->am_data_worker, true);

    // Set up Active Message data handler
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB |
                          UCP_AM_HANDLER_PARAM_FIELD_ARG;
    am_param.id         = UCX_AM_ID_DATA;   // This id should match the one in am_send_nbx()
    am_param.cb         = server_am_cb_data;
    am_param.arg        = session;
    status              = ucp_worker_set_am_recv_handler(session->am_data_worker, &am_param);
    assert(status == UCS_OK);

    ucp_am_handler_param_t am_param2;
    am_param2.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param2.id         = UCX_AM_ID_CMD;   // This id should match the one in am_send_nbx()
    am_param2.cb         = server_am_cb_cmd;
    status               = ucp_worker_set_am_recv_handler(session->am_data_worker, &am_param2);
    assert(status == UCS_OK);

    // Create EP for data communication
    ucp_ep_params_t ep_params;
    ep_params.field_mask      = UCP_EP_PARAM_FIELD_CONN_REQUEST |
                                UCP_EP_PARAM_FIELD_ERR_HANDLER  |
                                UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE;
    ep_params.conn_request    = session->conn_request;
    ep_params.err_mode        = UCP_ERR_HANDLING_MODE_PEER;
    ep_params.err_handler.cb  = server_err_cb;
    ep_params.err_handler.arg = session;
    status = ucp_ep_create(session->am_data_worker, &ep_params, &session->server_ep);
    assert(status == UCS_OK);

    // Wait for receive one AM message from client
    // and end this session
    while(!session->complete) {
        ucp_worker_progress(session->am_data_worker);
    }

    if(session->respond) {
        tangram_ucx_server_respond(session);
        free(session->respond);
    }

    ep_close(session->am_data_worker, session->server_ep);
    ucp_worker_destroy(session->am_data_worker);
}

void* my_worker_func(void* arg) {
    int tid = *((int*)arg);
    my_session_t *session = NULL;
    while(g_server_running) {

        pthread_mutex_lock(&g_my_workers[tid].lock);
        session = g_my_workers[tid].sessions;
        if(session)
            DL_DELETE(g_my_workers[tid].sessions, session);
        pthread_mutex_unlock(&g_my_workers[tid].lock);

        if(session) {
            handle_one_session(session);
            free(session);
        }
    }

    // At this point, we should handled all sessions.
    // i.e., g_my_workers[tid].sessions should be empty.
    //
    // But it is possible that client stoped the server
    // before all their requests have been finished.
    int count;
    DL_COUNT(g_my_workers[tid].sessions, session, count);
    assert(count == 0);
}

static void server_conn_cb(ucp_conn_request_h conn_request, void *arg)
{
    my_session_t *session = malloc(sizeof(my_session_t));
    session->conn_request = conn_request;
    session->complete = false;
    session->respond = NULL;
    session->respond_len = 0;

    pthread_mutex_lock(&g_my_workers[who].lock);
    DL_APPEND(g_my_workers[who].sessions, session);
    assert(g_my_workers[who].sessions != NULL);
    pthread_mutex_unlock(&g_my_workers[who].lock);
    who = (who + 1) % NUM_THREADS;
}

void run_server() {
    ucp_listener_h listener;
    ucp_listener_params_t params;
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

    /*
     * Every time a connection request comes, the conn_cb()
     * function will spawn a new pthread to handle the request.
     */
    while(g_server_running) {
        ucp_worker_progress(g_ucp_worker);
    }

    // Clean up
    // TODO: pthread_join();
    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(g_my_workers[i].thread, NULL);
    }
    ucp_listener_destroy(listener);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_server_init(const char* interface, char* server_ip_addr) {
    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_ucp_worker, false);
    get_interface_ip_addr(interface, server_ip_addr);

    for(int i = 0; i < NUM_THREADS; i++) {
        g_my_workers[i].tid = i;
        g_my_workers[i].sessions = NULL;
        int err = pthread_mutex_init(&g_my_workers[i].lock, NULL);
        assert(err == 0);
        pthread_create(&(g_my_workers[i].thread), NULL, my_worker_func, &g_my_workers[i].tid);
    }
}

void tangram_ucx_server_register_rpc(void* (*user_handler)(int, void*, size_t, size_t*)) {
    user_am_data_handler = user_handler;
}

void tangram_ucx_server_start() {
    run_server();
}
