#define _POSIX_C_SOURCE 200112L
#define NI_MAXHOST      1025

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ucp/api/ucp.h>
#include "tangramfs-ucx-comm.h"


void init_context(ucp_context_h *ucp_context) {
    ucp_params_t ucp_params;
    ucs_status_t status;

    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features = UCP_FEATURE_AM | UCP_FEATURE_RMA;
    status = ucp_init(&ucp_params, NULL, ucp_context);
    assert(status == UCS_OK);
}

void init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker, bool single_thread) {
    ucp_worker_params_t worker_params;
    ucs_status_t status;

    memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    if(single_thread)
        worker_params.thread_mode = UCS_THREAD_MODE_SINGLE;
    else
        worker_params.thread_mode = UCS_THREAD_MODE_MULTI;

    status = ucp_worker_create(ucp_context, &worker_params, ucp_worker);
    assert(status == UCS_OK);
}

void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status) {
    char* message = (char*) arg;
    printf("%s, at err_cb(%d): %s\n", message, status, ucs_status_string(status));
}


void ep_connect(ucp_address_t* addr, ucp_worker_h worker, ucp_ep_h *ep) {
    ucp_ep_params_t ep_params;
    ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
    ep_params.address = addr;
    ucs_status_t status = ucp_ep_create(worker, &ep_params, ep);
    assert(status == UCS_OK);
}

void ep_close(ucp_worker_h worker, ucp_ep_h ep)
{
    ucp_request_param_t param;
    ucs_status_t status;
    void *close_req;
    param.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
    param.flags        = UCP_EP_CLOSE_FLAG_FORCE;
    close_req          = ucp_ep_close_nbx(ep, &param);
    //close_req          = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
    //request_finalize(worker, close_req);
    if (UCS_PTR_IS_PTR(close_req)) {
        do {
            ucp_worker_progress(worker);
            status = ucp_request_check_status(close_req);
        } while (status == UCS_INPROGRESS);
        // TODO confirm this
        // Do not assert astatus == UCS_OK here,
        // as the endponit maybe closed by the other already
        // so the status will not be UCS_OK
        ucp_request_free(close_req);
    } else if (UCS_PTR_STATUS(close_req) != UCS_OK) {
        fprintf(stderr, "failed to close ep, %s\n", ucs_status_string(UCS_PTR_STATUS(close_req)));
    }
}


void request_finalize(ucp_worker_h worker, void *request)
{
    ucs_status_t status;
    if(request == NULL)
        return;

    if(UCS_PTR_IS_ERR(request)) {
        status = UCS_PTR_STATUS(request);
        fprintf(stderr, "Erro at requeset_finalize(): %s\n", ucs_status_string(status));
        return;
    }

    do {
        ucp_worker_progress(worker);
        status = ucp_request_check_status(request);
    } while (status == UCS_INPROGRESS);

    status = ucp_request_check_status(request);
    assert(status == UCS_OK);

    ucp_request_free(request);
}


void empty_callback(void *request, ucs_status_t status) {
}

void worker_flush(ucp_worker_h worker)
{
    void *request = ucp_worker_flush_nb(worker, 0, empty_callback);
    request_finalize(worker, request);
}

// interface: e.g., "wlan0", "eno0"
void get_interface_ip_addr(const char* interface, char *ip_addr) {
    struct ifaddrs *ifaddr, *ifa;

    int res = getifaddrs(&ifaddr);
    assert(res != -1);

    for(ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
        if (ifa->ifa_addr == NULL)
            continue;

        if((strcmp(ifa->ifa_name, interface)==0) && (ifa->ifa_addr->sa_family==AF_INET)) {
            res = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), ip_addr, NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
            assert(res == 0);
            break;
        }
    }

    freeifaddrs(ifaddr);
}
