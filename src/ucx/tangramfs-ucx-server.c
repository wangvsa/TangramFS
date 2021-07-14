#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <unistd.h>
#include <pthread.h>
#include <ucp/api/ucp.h>
#include "utlist.h"
#include "tangramfs-utils.h"
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

#define NUM_THREADS 8

volatile static bool  g_server_running = true;
static ucp_context_h  g_ucp_context;
static ucp_worker_h   g_ucp_worker;              // for listening connections
static ucp_address_t* g_server_addr;             // server worker address


typedef struct my_session {
    void* respond;
    size_t respond_len;

    int op;
    void* data;

    ucp_worker_h  ucp_worker;
    ucp_address_t *client_addr;
    ucp_ep_h      client_ep;              // connected ep with client
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

void* (*user_am_data_handler)(int op, void* data, size_t *respond_len);




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

    my_session_t *session = malloc(sizeof(my_session_t));

    session->op = *(int*)header;
    session->client_addr = malloc(header_length - sizeof(int));
    memcpy(session->client_addr, header+sizeof(int), header_length - sizeof(int));
    session->data = malloc(length);
    memcpy(session->data, data, length);

    session->respond = NULL;
    session->respond_len = 0;

    pthread_mutex_lock(&g_my_workers[who].lock);
    DL_APPEND(g_my_workers[who].sessions, session);
    assert(g_my_workers[who].sessions != NULL);
    pthread_mutex_unlock(&g_my_workers[who].lock);
    who = (who + 1) % NUM_THREADS;

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
}


void server_err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
    my_session_t *session = (my_session_t*) arg;
    printf("server_err_cb: complete: %s\n", ucs_status_string(status));
}


void handle_one_session(my_session_t* session) {
    ucs_status_t status;
    init_worker(g_ucp_context, &session->ucp_worker, true);

    // Connect to client ep
    ep_connect(session->client_addr, session->ucp_worker, &session->client_ep);

    session->respond = (*user_am_data_handler)(session->op, session->data, &session->respond_len);

    // Send respond to client
    if(session->respond) {
        ucp_request_param_t am_params;
        am_params.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        am_params.flags        = UCP_AM_SEND_FLAG_EAGER;

        void *request = ucp_am_send_nbx(session->client_ep, UCX_AM_ID_DATA, NULL, 0, session->respond, session->respond_len, &am_params);
        request_finalize(session->ucp_worker, request);

        free(session->respond);
    }

    ep_close(session->ucp_worker, session->client_ep);
    ucp_worker_destroy(session->ucp_worker);
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

    // At this point, we should have handled all sessions.
    // i.e., g_my_workers[tid].sessions should be empty.
    //
    // But it is possible that client stoped the server
    // before all their requests have been finished.
    int count;
    DL_COUNT(g_my_workers[tid].sessions, session, count);
    assert(count == 0);
}

void run_server() {
    ucs_status_t status;

    /*
     * Every time a connection request comes, the conn_cb()
     * function will spawn a new pthread to handle the request.
     */
    while(g_server_running) {
        ucp_worker_progress(g_ucp_worker);
    }

    // Clean up
    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(g_my_workers[i].thread, NULL);
    }
    ucp_worker_release_address(g_ucp_worker, g_server_addr);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
}

void tangram_ucx_server_init(const char* persist_dir) {
    ucs_status_t status;
    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_ucp_worker, false);

    // Set up Active Message data handler
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_DATA;   // This id should match the one in am_send_nbx()
    am_param.cb         = server_am_cb_data;
    status              = ucp_worker_set_am_recv_handler(g_ucp_worker, &am_param);
    assert(status == UCS_OK);

    ucp_am_handler_param_t am_param2;
    am_param2.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                           UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param2.id         = UCX_AM_ID_CMD;   // This id should match the one in am_send_nbx()
    am_param2.cb         = server_am_cb_cmd;
    status               = ucp_worker_set_am_recv_handler(g_ucp_worker, &am_param2);
    assert(status == UCS_OK);

    // get server worker address
    size_t addr_len;
    status = ucp_worker_get_address(g_ucp_worker, &g_server_addr, &addr_len);
    assert(status == UCS_OK);
    tangram_write_server_addr(persist_dir, g_server_addr, addr_len);

    for(int i = 0; i < NUM_THREADS; i++) {
        g_my_workers[i].tid = i;
        g_my_workers[i].sessions = NULL;
        int err = pthread_mutex_init(&g_my_workers[i].lock, NULL);
        assert(err == 0);
        pthread_create(&(g_my_workers[i].thread), NULL, my_worker_func, &g_my_workers[i].tid);
    }
}

void tangram_ucx_server_register_rpc(void* (*user_handler)(int, void*, size_t*)) {
    user_am_data_handler = user_handler;
}

void tangram_ucx_server_start() {
    run_server();
}
