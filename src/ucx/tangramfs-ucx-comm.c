#define _POSIX_C_SOURCE 200112L
#define NI_MAXHOST      1025

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <alloca.h>
#include <ifaddrs.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ucp/api/ucp.h>
#include <mpi.h>
#include "tangramfs-utils.h"
#include "tangramfs-ucx-comm.h"

#define MPI_SIZE 64


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
    //close_req          = ucp_ep_close_nbx(ep, &param);
    close_req          = ucp_ep_close_nb(ep, UCP_EP_CLOSE_MODE_FLUSH);
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


void init_iface(char* dev_name, char* tl_name, uct_listener_info_t *info) {
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

    uct_md_iface_config_read(info->md, tl_name, NULL, NULL, &config);
    status = uct_iface_open(info->md, info->worker, &params, config, &info->iface);
    uct_config_release(config);

    // enable progress
    uct_iface_progress_enable(info->iface, UCT_PROGRESS_SEND | UCT_PROGRESS_RECV);

    // get attr
    uct_iface_query(info->iface, &info->iface_attr);
}

void dev_tl_lookup(char* dev_name, char* tl_name, uct_listener_info_t *info) {

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
                    info->md = md;
                    info->component = components[i];
                    uct_md_query(md, &info->md_attr);
                }
            }

            uct_release_tl_resource_list(tl_resources);
            if(!found)
                uct_md_close(md);
        }
    }
    uct_release_component_list(components);
}

void exchange_dev_iface_addr(void* dev_addr, void* iface_addr, uct_listener_info_t* info) {
    int mpi_size;
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    size_t addr_len;
    void*  all_addrs;

    // exchange device addrs
    addr_len = info->iface_attr.device_addr_len;
    all_addrs = malloc(addr_len*mpi_size);
    MPI_Allgather(dev_addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    info->client_dev_addrs = malloc(mpi_size*sizeof(uct_device_addr_t*));
    for(int rank = 0; rank < mpi_size; rank++) {
        info->client_dev_addrs[rank] = malloc(addr_len);
        memcpy(info->client_dev_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);

    // exchange iface addrs
    addr_len = info->iface_attr.iface_addr_len;
    all_addrs = malloc(addr_len*mpi_size);
    MPI_Allgather(iface_addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    info->client_iface_addrs = malloc(mpi_size*sizeof(uct_iface_addr_t*));
    for(int rank = 0; rank < mpi_size; rank++) {
        info->client_iface_addrs[rank] = malloc(addr_len);
        memcpy(info->client_iface_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);
}

void init_uct_listener_info(ucs_async_context_t* context, char* dev_name, char* tl_name, bool server, uct_listener_info_t *info) {
    uct_worker_create(context, UCS_THREAD_MODE_SERIALIZED, &info->worker);

    // search for dev and tl
    // This will open info->md and set info->md_attr
    dev_tl_lookup(dev_name, tl_name, info);

    // This will open the info->iface and set info->iface_attr
    init_iface(dev_name, tl_name, info);

    if(server) {
        info->server_dev_addr = malloc(info->iface_attr.device_addr_len);
        uct_iface_get_device_address(info->iface, info->server_dev_addr);
        info->server_iface_addr = malloc(info->iface_attr.iface_addr_len);
        uct_iface_get_address(info->iface, info->server_iface_addr);
        tangram_write_uct_server_addr("./", info->server_dev_addr, info->iface_attr.device_addr_len,
                            info->server_iface_addr, info->iface_attr.iface_addr_len);

        int mpi_size = MPI_SIZE;
        info->client_dev_addrs = malloc(sizeof(uct_device_addr_t*) * mpi_size);
        info->client_iface_addrs = malloc(sizeof(uct_iface_addr_t*) * mpi_size);
        for(int i = 0; i < mpi_size; i++) {
            info->client_dev_addrs[i] = NULL;
            info->client_iface_addrs[i] = NULL;
        }

    } else {
        void* dev_addr = alloca(info->iface_attr.device_addr_len);
        uct_iface_get_device_address(info->iface, dev_addr);
        void* iface_addr = alloca(info->iface_attr.iface_addr_len);
        uct_iface_get_address(info->iface, iface_addr);
        exchange_dev_iface_addr(dev_addr, iface_addr, info);
        tangram_read_uct_server_addr("./", (void**)&info->server_dev_addr, (void**)&info->server_iface_addr);
    }
}

void destroy_uct_listener_info(uct_listener_info_t *info) {

    /*
    for(int rank = 0; rank < MPI_SIZE; rank++) {
        if(info->client_dev_addrs[rank])
            free(info->client_dev_addrs[rank]);
        if(info->client_iface_addrs[rank])
            free(info->client_iface_addrs[rank]);
    }
    */

    uct_iface_close(info->iface);
    uct_md_close(info->md);
    free(info->client_dev_addrs);
    free(info->client_iface_addrs);
    uct_worker_destroy(info->worker);
}


// Create and connect to the remote iface
void uct_ep_create_connect(uct_iface_h iface, uct_device_addr_t* dev_addr, uct_iface_addr_t* iface_addr, uct_ep_h* ep) {
    //assert(info->iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE);
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


