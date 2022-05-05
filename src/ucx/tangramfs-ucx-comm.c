#define _POSIX_C_SOURCE 200112L
#define NI_MAXHOST      1025
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <alloca.h>
#include <mpi.h>
#include "tangramfs-utils.h"
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"


void init_iface(char* dev_name, char* tl_name, tangram_uct_context_t *context) {
    ucs_status_t        status;
    uct_iface_config_t  *config; /* Defines interface configuration options */
    uct_iface_params_t  params;
    params.field_mask           = UCT_IFACE_PARAM_FIELD_OPEN_MODE   |
                                  UCT_IFACE_PARAM_FIELD_DEVICE      |
                                  UCT_IFACE_PARAM_FIELD_STATS_ROOT  |
                                  UCT_IFACE_PARAM_FIELD_RX_HEADROOM |
                                  UCT_IFACE_PARAM_FIELD_CPU_MASK;
    params.open_mode            = UCT_IFACE_OPEN_MODE_DEVICE;
    params.mode.device.dev_name = dev_name;
    params.mode.device.tl_name  = tl_name;
    params.stats_root           = NULL;
    params.rx_headroom          = 0;

    // TODO??
    UCS_CPU_ZERO(&params.cpu_mask);

    uct_md_iface_config_read(context->md, tl_name, NULL, NULL, &config);
    status = uct_iface_open(context->md, context->worker, &params, config, &context->iface);
    uct_config_release(config);

    // enable progress
    uct_iface_progress_enable(context->iface, UCT_PROGRESS_SEND | UCT_PROGRESS_RECV);

    // get attr
    uct_iface_query(context->iface, &context->iface_attr);
}

void dev_tl_lookup(char* dev_name, char* tl_name, tangram_uct_context_t *context) {

    uct_component_h* components;
    unsigned num_components;
    uct_query_components(&components, &num_components);

    // Iterate through components
    for(int i = 0; i < num_components; i++) {
        uct_component_attr_t   component_attr;
        component_attr.field_mask = UCT_COMPONENT_ATTR_FIELD_MD_RESOURCE_COUNT;
        uct_component_query(components[i], &component_attr);

        component_attr.field_mask = UCT_COMPONENT_ATTR_FIELD_MD_RESOURCES;
        component_attr.md_resources = alloca(sizeof(*component_attr.md_resources) *
                                             component_attr.md_resource_count);
        uct_component_query(components[i], &component_attr);

        //iface_p->iface = NULL;
        // Iterate through memory domain
        for(int j= 0; j < component_attr.md_resource_count; j++) {
            uct_md_h md;
            uct_md_config_t *md_config;
            uct_md_config_read(components[i], NULL, NULL, &md_config);
            uct_md_open(components[i], component_attr.md_resources[j].md_name,
                        md_config, &md);
            uct_config_release(md_config);

            uct_tl_resource_desc_t *tl_resources = NULL;
            unsigned num_tl_resources;
            uct_md_query_tl_resources(md, &tl_resources, &num_tl_resources);


            int found = 0;
            // Iterate through transport
            for(int k = 0; k < num_tl_resources; k++) {
                char* tln = tl_resources[k].tl_name;
                char* devn = tl_resources[k].dev_name;
                if(0==strcmp(tln, tl_name) && 0==strcmp(devn, dev_name)) {
                    found = 1;
                    context->md = md;
                    context->component = components[i];
                    uct_md_query(md, &context->md_attr);
                }
            }

            uct_release_tl_resource_list(tl_resources);
            if(!found)
                uct_md_close(md);
        }
    }
    uct_release_component_list(components);
}

void exchange_dev_iface_addr(tangram_uct_context_t* context, tangram_uct_addr_t* peer_addrs) {
    int mpi_size;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    size_t addr_len;
    void*  all_addrs;

    // exchange device addrs
    addr_len = context->self_addr.dev_len;
    all_addrs = malloc(addr_len * mpi_size);
    MPI_Allgather(context->self_addr.dev, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    for(int rank = 0; rank < mpi_size; rank++) {
        peer_addrs[rank].dev     = malloc(addr_len);
        peer_addrs[rank].dev_len = addr_len;
        memcpy(peer_addrs[rank].dev, all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);

    // exchange iface addrs
    addr_len = context->self_addr.iface_len;
    all_addrs = malloc(addr_len*mpi_size);
    MPI_Allgather(context->self_addr.iface, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    for(int rank = 0; rank < mpi_size; rank++) {
        peer_addrs[rank].iface     = malloc(addr_len);
        peer_addrs[rank].iface_len = addr_len;
        memcpy(peer_addrs[rank].iface, all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);
}

void tangram_uct_context_init(ucs_async_context_t* async, tfs_info_t* tfs_info, tangram_uct_context_t *context) {
    uct_worker_create(async, UCS_THREAD_MODE_SERIALIZED, &context->worker);

    // search for dev and tl
    // This will open context->md and set context->md_attr
    dev_tl_lookup(tfs_info->rpc_dev_name, tfs_info->rpc_tl_name, context);

    // This will open the context->iface and set context->iface_attr
    init_iface(tfs_info->rpc_dev_name, tfs_info->rpc_tl_name, context);

    // Set up myself's addr
    context->self_addr.dev_len   = context->iface_attr.device_addr_len;
    context->self_addr.iface_len = context->iface_attr.iface_addr_len;
    context->self_addr.dev       = malloc(context->self_addr.dev_len);
    context->self_addr.iface     = malloc(context->self_addr.iface_len);
    uct_iface_get_device_address(context->iface, context->self_addr.dev);
    uct_iface_get_address(context->iface, context->self_addr.iface);

    // context->local_server will be filled by the calling client
    // context->global_server will be filled by read_uct_server_addr()
    context->local_server_addr.dev    = NULL;
    context->local_server_addr.iface  = NULL;
    context->global_server_addr.dev   = NULL;
    context->global_server_addr.iface = NULL;

    if (tfs_info->role == TANGRAM_UCX_ROLE_CLIENT) {
        tangram_read_uct_server_addr((void**)&context->global_server_addr.dev, &context->global_server_addr.dev_len,
                                           (void**)&context->global_server_addr.iface, &context->global_server_addr.iface_len);
    }

    if(tfs_info->role == TANGRAM_UCX_ROLE_SERVER) {
        // write out my address
        tangram_write_uct_server_addr(context->self_addr.dev, context->self_addr.dev_len,
                                            context->self_addr.iface, context->self_addr.iface_len);
    }

    pthread_mutex_init(&context->mutex, NULL);
    pthread_mutex_init(&context->cond_mutex, NULL);
    pthread_cond_init(&context->cond, NULL);
}

void tangram_uct_context_destroy(tangram_uct_context_t *context) {
    uct_iface_close(context->iface);
    uct_md_close(context->md);

    tangram_uct_addr_free(&context->self_addr);
    tangram_uct_addr_free(&context->local_server_addr);
    tangram_uct_addr_free(&context->global_server_addr);

    uct_worker_destroy(context->worker);


    pthread_mutex_destroy(&context->mutex);
    pthread_mutex_destroy(&context->cond_mutex);
    pthread_cond_destroy(&context->cond);
}


// Create and connect to the remote iface
void uct_ep_create_connect(uct_iface_h iface, tangram_uct_addr_t* addr, uct_ep_h* ep) {
    //assert(context->iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE);
    ucs_status_t status;

    uct_ep_params_t ep_params;
    ep_params.field_mask = UCT_EP_PARAM_FIELD_IFACE     |
                           UCT_EP_PARAM_FIELD_DEV_ADDR  |
                           UCT_EP_PARAM_FIELD_IFACE_ADDR;
    ep_params.iface      = iface;
    ep_params.dev_addr   = addr->dev;
    ep_params.iface_addr = addr->iface;

    status = uct_ep_create(&ep_params, ep);
    assert(status == UCS_OK);
}

void* pack_rpc_buffer(tangram_uct_addr_t* addr, void* data, size_t inlen, size_t* outlen) {

    *outlen = 2 * sizeof(size_t) + addr->dev_len + addr->iface_len + inlen;

    void* out = malloc(*outlen);

    void* ptr = out;
    memcpy(ptr, &addr->dev_len, sizeof(size_t));

    ptr += sizeof(size_t);
    memcpy(ptr, addr->dev, addr->dev_len);

    ptr += addr->dev_len;
    memcpy(ptr, &addr->iface_len, sizeof(size_t));

    ptr += sizeof(size_t);
    memcpy(ptr, addr->iface, addr->iface_len);

    ptr += addr->iface_len;
    if(data && inlen > 0)
        memcpy(ptr, data, inlen);

    return out;
}

void unpack_rpc_buffer(void* buf, size_t buf_len, tangram_uct_addr_t *sender, void** data_ptr) {
    buf_len = buf_len - sizeof(uint64_t);   // skip header;
    void* ptr = buf + sizeof(uint64_t);

    size_t dev_len, iface_len;
    memcpy(&dev_len, ptr, sizeof(size_t));
    ptr += sizeof(size_t);

    if(sender != TANGRAM_UCT_ADDR_IGNORE) {
        sender->dev_len = dev_len;
        sender->dev = malloc(dev_len);
        memcpy(sender->dev, ptr, dev_len);
    }

    ptr += dev_len;
    memcpy(&iface_len, ptr, sizeof(size_t));
    ptr += sizeof(size_t);

    if(sender != TANGRAM_UCT_ADDR_IGNORE) {
        sender->iface_len = iface_len;
        sender->iface = malloc(iface_len);
        memcpy(sender->iface, ptr, iface_len);
    }

    ptr += iface_len;
    size_t data_len = buf_len - 2 * sizeof(size_t) - dev_len - iface_len;

    if(data_len > 0 && data_ptr != NULL) {
        *data_ptr = malloc(data_len);
        memcpy(*data_ptr, ptr, data_len);
    }
}

void do_uct_am_short(pthread_mutex_t *lock, uct_ep_h ep, uint8_t id, tangram_uct_addr_t* my_addr, void* data, size_t data_len) {
    size_t buf_len;
    void* buf = pack_rpc_buffer(my_addr, data, data_len, &buf_len);

    ucs_status_t status = UCS_OK;
    do {
        pthread_mutex_lock(lock);
        status = uct_ep_am_short(ep, id, 0, buf, buf_len);
        pthread_mutex_unlock(lock);
    } while (status == UCS_ERR_NO_RESOURCE);

    free(buf);
    assert(status == UCS_OK);
}

void do_uct_am_short_progress(uct_worker_h worker, uct_ep_h ep, uint8_t id, tangram_uct_addr_t* my_addr, void* data, size_t data_len) {
    size_t buf_len;
    void* buf = pack_rpc_buffer(my_addr, data, data_len, &buf_len);

    ucs_status_t status = UCS_OK;
    do {
        status = uct_ep_am_short(ep, id, 0, buf, buf_len);
        uct_worker_progress(worker);
    } while (status == UCS_ERR_NO_RESOURCE);

    free(buf);
    assert(status == UCS_OK);
}


void* tangram_uct_addr_serialize(tangram_uct_addr_t* addr, size_t *len) {
    *len = 0;
    if(!addr) return NULL;

    *len = sizeof(size_t)*2 + addr->dev_len + addr->iface_len;
    void* buf = malloc(*len);

    void* ptr = buf;
    memcpy(ptr, &addr->dev_len, sizeof(size_t));

    ptr += sizeof(size_t);
    memcpy(ptr, addr->dev, addr->dev_len);

    ptr += addr->dev_len;
    memcpy(ptr, &addr->iface_len, sizeof(size_t));

    ptr += sizeof(size_t);
    memcpy(ptr, addr->iface, addr->iface_len);

    return buf;
}

void tangram_uct_addr_deserialize(void* buf, tangram_uct_addr_t* addr) {
    void* ptr = buf;
    memcpy(&addr->dev_len, ptr, sizeof(size_t));

    ptr += sizeof(size_t);
    addr->dev = malloc(addr->dev_len);
    memcpy(addr->dev, ptr, addr->dev_len);

    ptr += addr->dev_len;
    memcpy(&addr->iface_len, ptr, sizeof(size_t));

    ptr += sizeof(size_t);
    addr->iface = malloc(addr->iface_len);
    memcpy(addr->iface, ptr, addr->iface_len);
}

tangram_uct_addr_t* tangram_uct_addr_duplicate(tangram_uct_addr_t* in) {
    if(in == TANGRAM_UCT_ADDR_IGNORE)
        return TANGRAM_UCT_ADDR_IGNORE;
    tangram_uct_addr_t* out = malloc(sizeof(tangram_uct_addr_t));
    out->dev_len = in->dev_len;
    out->iface_len = in->iface_len;
    out->dev = malloc(out->dev_len);
    out->iface = malloc(out->iface_len);
    memcpy(out->dev, in->dev, out->dev_len);
    memcpy(out->iface, in->iface, out->iface_len);
    return out;
}

void tangram_uct_addr_free(tangram_uct_addr_t* addr) {
    if(addr == TANGRAM_UCT_ADDR_IGNORE) return;

    if(addr->dev != NULL)
        free(addr->dev);
    if(addr->iface != NULL)
        free(addr->iface);

    addr->dev = NULL;
    addr->iface = NULL;
    // TODO free(addr)?
}

int tangram_uct_addr_compare(tangram_uct_addr_t* a, tangram_uct_addr_t* b) {
    if(!a || !b) return -1;
    if(a == b) return 0;
    if(a->dev_len == b->dev_len && a->iface_len == b->iface_len) {
        int r1 = memcmp(a->dev, b->dev, a->dev_len);
        int r2 = memcmp(a->iface, b->iface, a->iface_len);
        if (r1 == 0 && r2 == 0)
            return 0;
    }
    return 1;
}

