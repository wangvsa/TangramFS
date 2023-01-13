#ifndef _TANGRAMFS_UCX_COMM_H_
#define _TANGRAMFS_UCX_COMM_H_
#include <stdbool.h>
#include <pthread.h>
#include <ucp/api/ucp.h>
#include <uct/api/uct.h>
#include "tangramfs-utils.h"

#define UCX_SERVER_PORT                     13432

#define AM_ID_QUERY_REQUEST                 1
#define AM_ID_QUERY_RESPOND                 2
#define AM_ID_POST_REQUEST                  3
#define AM_ID_POST_RESPOND                  4
#define AM_ID_RMA_REQUEST                   5
#define AM_ID_RMA_RESPOND                   6
#define AM_ID_RMA_EP_ADDR                   7
#define AM_ID_STOP_REQUEST                  8
#define AM_ID_STOP_RESPOND                  9
#define AM_ID_STAT_REQUEST                  12
#define AM_ID_STAT_RESPOND                  13
#define AM_ID_UNPOST_FILE_REQUEST           14
#define AM_ID_UNPOST_FILE_RESPOND           15
#define AM_ID_UNPOST_CLIENT_REQUEST         16
#define AM_ID_UNPOST_CLIENT_RESPOND         17


#define AM_ID_ACQUIRE_LOCK_REQUEST          20
#define AM_ID_ACQUIRE_LOCK_RESPOND          21
#define AM_ID_RELEASE_LOCK_REQUEST          22
#define AM_ID_RELEASE_LOCK_RESPOND          23
#define AM_ID_RELEASE_LOCK_FILE_REQUEST     24
#define AM_ID_RELEASE_LOCK_FILE_RESPOND     25
#define AM_ID_RELEASE_LOCK_CLIENT_REQUEST   26
#define AM_ID_RELEASE_LOCK_CLIENT_RESPOND   27
#define AM_ID_SPLIT_LOCK_REQUEST            28
#define AM_ID_SPLIT_LOCK_RESPOND            29

#define TANGRAM_UCX_ROLE_CLIENT             0
#define TANGRAM_UCX_ROLE_SERVER             1

#define TANGRAM_UCT_ADDR_IGNORE             NULL


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
    tangram_uct_addr_t delegator_addr;
    tangram_uct_addr_t server_addr;

    // Make sure a context is only used by one thread at a time
    pthread_mutex_t    mutex;
    pthread_mutex_t    cond_mutex;
    pthread_cond_t     cond;

    // Used to recieve respond from server or peers.
    void**             respond_ptr;
    volatile bool      respond_flag;
} tangram_uct_context_t;

void tangram_uct_context_init(ucs_async_context_t* async, tfs_info_t* tfs_info, bool intra_comm, tangram_uct_context_t* context);
void tangram_uct_context_destroy(tangram_uct_context_t* context);
void exchange_dev_iface_addr(tangram_uct_context_t* context, tangram_uct_addr_t* peer_addrs);

void uct_ep_create_connect(uct_iface_h iface, tangram_uct_addr_t* dest, uct_ep_h* ep);


void unpack_rpc_buffer(void* buf, size_t buf_len, uint64_t* seq_id, tangram_uct_addr_t* sender, void** data_ptr);

void do_uct_am_short_lock(pthread_mutex_t *lock, uct_ep_h ep, uint8_t id, uint64_t seq_id, tangram_uct_addr_t* my_addr, void* data, size_t length);
void do_uct_am_short_progress(uct_worker_h worker, uct_ep_h ep, uint8_t id, uint64_t seq_id, tangram_uct_addr_t* my_addr, void* data, size_t length);

void* tangram_uct_addr_serialize(tangram_uct_addr_t* addr, size_t* len);
void  tangram_uct_addr_deserialize(void* buf, tangram_uct_addr_t* addr);
tangram_uct_addr_t* tangram_uct_addr_duplicate(tangram_uct_addr_t* in);
void  tangram_uct_addr_free(tangram_uct_addr_t* addr);
int   tangram_uct_addr_compare(tangram_uct_addr_t* a, tangram_uct_addr_t* b);


#endif
