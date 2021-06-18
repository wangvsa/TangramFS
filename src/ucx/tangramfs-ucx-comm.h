#ifndef _TANGRAMFS_UCX_COMM_H_
#define _TANGRAMFS_UCX_COMM_H_
#include <stdbool.h>
#include <ucp/api/ucp.h>

void init_context(ucp_context_h *ucp_context);
void init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker, bool single_thread);
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);
void ep_close(ucp_worker_h worker, ucp_ep_h ep);

void request_finalize(ucp_worker_h worker, void *request);
void empty_callback(void *request, ucs_status_t status);
void worker_flush(ucp_worker_h worker);

void get_interface_ip_addr(const char* interface, char *ip_addr);

#endif
