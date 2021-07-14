#define _POSIX_C_SOURCE 200112L
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "tangramfs-utils.h"
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"


static ucp_context_h  g_ucp_context;
static ucp_worker_h   g_ucp_worker;
static ucp_ep_h       g_client_ep;
static ucp_address_t* g_worker_addr;
static void*          g_server_addr;
static void*          g_am_header;
static size_t         g_am_header_len;

static void*          g_server_respond;
static volatile bool  g_received_respond;


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

    g_received_respond = true;
    if(g_server_respond)
        memcpy(g_server_respond, data, length);

    // return UCS_OK, ucp will free *data
    return UCS_OK;
}

void client_err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
    printf("client_err_cb: received respond: %d, %s\n",
            g_received_respond, ucs_status_string(status));
    g_received_respond = true;
}



// Send and wait for the respond from server
void tangram_ucx_sendrecv(int op, void* data, size_t length, void* respond) {
    g_server_respond = respond;
    g_received_respond = false;
    ep_connect(g_server_addr, g_ucp_worker, &g_client_ep);

    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    am_params.flags = UCP_AM_SEND_FLAG_EAGER;

    memcpy(g_am_header, &op, sizeof(int));
    void *request = ucp_am_send_nbx(g_client_ep, UCX_AM_ID_DATA, g_am_header, g_am_header_len, data, length, &am_params);
    request_finalize(g_ucp_worker, request);

    // Wait for the respond from server
    while(!g_received_respond) {
        ucp_worker_progress(g_ucp_worker);
    }

    ep_close(g_ucp_worker, g_client_ep);
}

void tangram_ucx_stop_server() {
    ep_connect(g_server_addr, g_ucp_worker, &g_client_ep);

    // Active Message send
    ucp_request_param_t am_params;
    am_params.op_attr_mask = 0;
    void *request = ucp_am_send_nbx(g_client_ep, UCX_AM_ID_CMD, NULL, 0, NULL, 0, &am_params);
    request_finalize(g_ucp_worker, request);
}

void tangram_ucx_rpc_service_start(const char* persist_dir) {
    size_t addr_len;
    tangram_read_server_addr(persist_dir, &g_server_addr, &addr_len);
    init_context(&g_ucp_context);
    init_worker(g_ucp_context, &g_ucp_worker, true);

    // Set am callback to receive respond from server
    ucp_am_handler_param_t am_param;
    am_param.field_mask = UCP_AM_HANDLER_PARAM_FIELD_ID |
                          UCP_AM_HANDLER_PARAM_FIELD_CB;
    am_param.id         = UCX_AM_ID_DATA;
    am_param.cb         = client_am_recv_cb;
    ucs_status_t status = ucp_worker_set_am_recv_handler(g_ucp_worker, &am_param);
    assert(status == UCS_OK);

    status = ucp_worker_get_address(g_ucp_worker, &g_worker_addr, &addr_len);
    assert(status == UCS_OK);
    // am header: [op(int) client_worker_addr]
    g_am_header_len = sizeof(int) + addr_len;
    g_am_header = malloc(g_am_header_len);
    memcpy(g_am_header+sizeof(int), g_worker_addr, addr_len);
}

void tangram_ucx_rpc_service_stop() {
    ucp_worker_release_address(g_ucp_worker, g_worker_addr);
    ucp_worker_destroy(g_ucp_worker);
    ucp_cleanup(g_ucp_context);
    free(g_server_addr);
    free(g_am_header);
}
