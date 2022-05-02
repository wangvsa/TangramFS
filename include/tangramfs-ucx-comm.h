#ifndef _TANGRAMFS_UCX_COMM_H_
#define _TANGRAMFS_UCX_COMM_H_
#include <stdbool.h>
#include <uct/api/uct.h>
#include <pthread.h>
#include "tangramfs-utils.h"

#define TANGRAM_UCX_ROLE_RPC_CLIENT     0
#define TANGRAM_UCX_ROLE_RMA_CLIENT     1
#define TANGRAM_UCX_ROLE_LOCAL_SERVER   2
#define TANGRAM_UCX_ROLE_GLOBAL_SERVER  3

#define TANGRAM_UCT_ADDR_IGNORE         NULL

typedef struct tangram_uct_addr {
    uct_device_addr_t* dev;
    uct_iface_addr_t*  iface;
    size_t             dev_len;
    size_t             iface_len;
} tangram_uct_addr_t;


typedef struct tangram_uct_context {
    uct_component_h    component;
    uct_worker_h       worker;
    uct_iface_h        iface;
    uct_iface_attr_t   iface_attr;
    uct_md_h           md;
    uct_md_attr_t      md_attr;

    tangram_uct_addr_t self_addr;
    tangram_uct_addr_t local_server_addr;
    tangram_uct_addr_t global_server_addr;

    // Make sure a context is only used by one thread at a time
    pthread_mutex_t    mutex;
    pthread_mutex_t    cond_mutex;
    pthread_cond_t     cond;

    // Used to recieve respond from server or peers.
    void**             respond_ptr;
    volatile bool      respond_flag;
} tangram_uct_context_t;

void tangram_uct_context_init(ucs_async_context_t* async, tfs_info_t* tfs_info, tangram_uct_context_t* context);
void tangram_uct_context_destroy(tangram_uct_context_t* context);
void exchange_dev_iface_addr(tangram_uct_context_t* context, tangram_uct_addr_t* peer_addrs);

void uct_ep_create_connect(uct_iface_h iface, tangram_uct_addr_t* dest, uct_ep_h* ep);


void unpack_rpc_buffer(void* buf, size_t buf_len, tangram_uct_addr_t *sender, void** data_ptr);

void do_uct_am_short(pthread_mutex_t *lock, uct_ep_h ep, uint8_t id, tangram_uct_addr_t* my_addr, void* data, size_t length);
void do_uct_am_short_progress(uct_worker_h worker, uct_ep_h ep, uint8_t id, tangram_uct_addr_t* my_addr, void* data, size_t length);

void* tangram_uct_addr_serialize(tangram_uct_addr_t* addr, size_t* len);
void  tangram_uct_addr_deserialize(void* buf, tangram_uct_addr_t* addr);
tangram_uct_addr_t* tangram_uct_addr_duplicate(tangram_uct_addr_t* in);
void  tangram_uct_addr_free(tangram_uct_addr_t* addr);
int   tangram_uct_addr_compare(tangram_uct_addr_t* a, tangram_uct_addr_t* b);


#endif
