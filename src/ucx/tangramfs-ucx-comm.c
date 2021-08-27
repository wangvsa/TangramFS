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

void exchange_dev_iface_addr(tangram_uct_context_t* context, uct_device_addr_t** dev_addrs, uct_iface_addr_t** iface_addrs) {
    int mpi_size;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    size_t addr_len;
    void*  all_addrs;

    // exchange device addrs
    addr_len = context->iface_attr.device_addr_len;
    all_addrs = malloc(addr_len*mpi_size);
    MPI_Allgather(context->dev_addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    for(int rank = 0; rank < mpi_size; rank++) {
        dev_addrs[rank] = malloc(addr_len);
        memcpy(dev_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);

    if(iface_addrs == NULL) return;
    // exchange iface addrs
    addr_len = context->iface_attr.iface_addr_len;
    all_addrs = malloc(addr_len*mpi_size);
    MPI_Allgather(context->iface_addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    for(int rank = 0; rank < mpi_size; rank++) {
        iface_addrs[rank] = malloc(addr_len);
        memcpy(iface_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);
}

void tangram_uct_context_init(ucs_async_context_t* async, char* dev_name, char* tl_name, bool server, tangram_uct_context_t *context) {
    uct_worker_create(async, UCS_THREAD_MODE_SERIALIZED, &context->worker);

    // search for dev and tl
    // This will open context->md and set context->md_attr
    dev_tl_lookup(dev_name, tl_name, context);

    // This will open the context->iface and set context->iface_attr
    init_iface(dev_name, tl_name, context);

    context->dev_addr = malloc(context->iface_attr.device_addr_len);
    context->iface_addr = malloc(context->iface_attr.iface_addr_len);
    context->server_dev_addr = malloc(context->iface_attr.device_addr_len);
    context->server_iface_addr = malloc(context->iface_attr.iface_addr_len);

    uct_iface_get_device_address(context->iface, context->dev_addr);
    uct_iface_get_address(context->iface, context->iface_addr);

    if(server) {
        tangram_write_uct_server_addr("./", context->dev_addr, context->iface_attr.device_addr_len,
                            context->iface_addr, context->iface_attr.iface_addr_len);
    } else {
        tangram_read_uct_server_addr("./", (void**)&context->server_dev_addr, (void**)&context->server_iface_addr);
    }

    pthread_mutex_init(&context->mutex, NULL);
    pthread_mutex_init(&context->cond_mutex, NULL);
    pthread_cond_init(&context->cond, NULL);
}


void tangram_uct_context_destroy(tangram_uct_context_t *context) {
    uct_iface_close(context->iface);
    uct_md_close(context->md);
    free(context->dev_addr);
    free(context->iface_addr);
    free(context->server_dev_addr);
    free(context->server_iface_addr);
    uct_worker_destroy(context->worker);

    pthread_mutex_destroy(&context->mutex);
    pthread_mutex_destroy(&context->cond_mutex);
    pthread_cond_destroy(&context->cond);
}


// Create and connect to the remote iface
void uct_ep_create_connect(uct_iface_h iface, uct_device_addr_t* dev_addr, uct_iface_addr_t* iface_addr, uct_ep_h* ep) {
    //assert(context->iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE);
    ucs_status_t status;

    uct_ep_params_t ep_params;
    ep_params.field_mask = UCT_EP_PARAM_FIELD_IFACE     |
                           UCT_EP_PARAM_FIELD_DEV_ADDR  |
                           UCT_EP_PARAM_FIELD_IFACE_ADDR;
    ep_params.iface      = iface;
    ep_params.dev_addr   = dev_addr;
    ep_params.iface_addr = iface_addr;

    status = uct_ep_create(&ep_params, ep);
    assert(status == UCS_OK);
}

void do_uct_am_short(pthread_mutex_t *lock, uct_ep_h ep, uint8_t id, int header, void* data, size_t length) {
    ucs_status_t status = UCS_OK;
    do {
        pthread_mutex_lock(lock);
        status = uct_ep_am_short(ep, id, header, data, length);
        pthread_mutex_unlock(lock);
    } while (status == UCS_ERR_NO_RESOURCE);
    assert(status == UCS_OK);
}

void do_uct_am_short_progress(uct_worker_h worker, uct_ep_h ep, uint8_t id, int header, void* data, size_t length) {
    ucs_status_t status = UCS_OK;
    do {
        status = uct_ep_am_short(ep, id, header, data, length);
        uct_worker_progress(worker);
    } while (status == UCS_ERR_NO_RESOURCE);
    assert(status == UCS_OK);
}


