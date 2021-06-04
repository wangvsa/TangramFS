#ifndef _TANGRAMFS_UCX_COMM_H_
#define _TANGRAMFS_UCX_COMM_H_
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <ucp/api/ucp.h>

void init_context(ucp_context_h *ucp_context);
void init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker);
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);
void ep_close(ucp_worker_h worker, ucp_ep_h ep);
void request_finalize(ucp_worker_h worker, void *request);

#endif
