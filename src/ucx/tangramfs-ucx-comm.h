#ifndef _TANGRAMFS_UCX_COMM_H_
#define _TANGRAMFS_UCX_COMM_H_
#include <stdbool.h>
#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include <pthread.h>


void init_context(ucp_context_h *ucp_context);
void init_worker(ucp_context_h ucp_context, ucp_worker_h *ucp_worker, bool single_thread);
void err_cb(void *arg, ucp_ep_h ep, ucs_status_t status);
void ep_connect(ucp_address_t* addr, ucp_worker_h worker, ucp_ep_h *ep);
void ep_close(ucp_worker_h worker, ucp_ep_h ep);

void request_finalize(ucp_worker_h worker, void *request);
void empty_callback(void *request, ucs_status_t status);
void worker_flush(ucp_worker_h worker);

void get_interface_ip_addr(const char* interface, char *ip_addr);

/**
 * For UCT
 */

typedef struct uct_listener_info {
    uct_component_h  component;
    uct_worker_h     worker;
    uct_iface_h      iface;
    uct_iface_attr_t iface_attr;
    uct_md_h         md;
    uct_md_attr_t    md_attr;

    uct_device_addr_t *server_dev_addr;
    uct_iface_addr_t  *server_iface_addr;

    uct_device_addr_t **client_dev_addrs;
    uct_iface_addr_t  **client_iface_addrs;

} uct_listener_info_t;


void init_uct_listener_info(ucs_async_context_t* context, char* dev_name, char* tl_name, bool server, uct_listener_info_t *info);
void destroy_uct_listener_info(uct_listener_info_t* info);

void uct_ep_create_connect(uct_iface_h iface, uct_device_addr_t* dev_addr, uct_iface_addr_t* iface_addr, uct_ep_h* ep);

void do_uct_am_short(pthread_mutex_t *lock, uct_ep_h ep, uint8_t id, int header, void* data, size_t length);
void do_uct_am_short_progress(uct_worker_h worker, uct_ep_h ep, uint8_t id, int header, void* data, size_t length);

#endif
