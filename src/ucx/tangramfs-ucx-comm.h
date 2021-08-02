#ifndef _TANGRAMFS_UCX_COMM_H_
#define _TANGRAMFS_UCX_COMM_H_
#include <stdbool.h>
#include <uct/api/uct.h>
#include <pthread.h>

typedef struct tangram_uct_context {
    uct_component_h    component;
    uct_worker_h       worker;
    uct_iface_h        iface;
    uct_iface_attr_t   iface_attr;
    uct_md_h           md;
    uct_md_attr_t      md_attr;

    uct_device_addr_t* dev_addr;
    uct_iface_addr_t*  iface_addr;
    uct_device_addr_t* server_dev_addr;
    uct_iface_addr_t*  server_iface_addr;
} tangram_uct_context_t;


void tangram_uct_context_init(ucs_async_context_t* async, char* dev_name, char* tl_name, bool server, tangram_uct_context_t* context);
void tangram_uct_context_destroy(tangram_uct_context_t* context);
void exchange_dev_iface_addr(tangram_uct_context_t* context, uct_device_addr_t** dev_addrs, uct_iface_addr_t** iface_addrs);

void uct_ep_create_connect(uct_iface_h iface, uct_device_addr_t* dev_addr, uct_iface_addr_t* iface_addr, uct_ep_h* ep);

void do_uct_am_short(pthread_mutex_t *lock, uct_ep_h ep, uint8_t id, int header, void* data, size_t length);
void do_uct_am_short_progress(uct_worker_h worker, uct_ep_h ep, uint8_t id, int header, void* data, size_t length);

#endif
