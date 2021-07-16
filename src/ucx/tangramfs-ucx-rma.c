#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <pthread.h>
#include <mpi.h>
#include <alloca.h>
#include <uct/api/uct.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"


#define AM_ID_RMA_REQUEST 1
#define AM_ID_RMA_RESPOND 2


static int          g_mpi_rank;
static int          g_mpi_size;

static volatile bool g_running = true;
static volatile int  g_current_reqs = 0;
static volatile bool g_received_respond = false;

ucs_async_context_t *g_context;

typedef struct my_info {
    uct_component_h  component;
    uct_worker_h     worker;
    uct_iface_h      iface;
    uct_iface_attr_t iface_attr;
    uct_md_h         md;
    uct_md_attr_t    md_attr;

    uct_ep_h          *eps;           // eps[i] is connected to rank i
    uct_device_addr_t **dev_addrs;
    uct_iface_addr_t  **iface_addrs;
} my_info_t;

my_info_t g_res_info;      // for listening request and sending respond
//my_info_t g_req_info;      // for sending requset


typedef struct zcopy_comp {
    uct_completion_t uct_comp;
    volatile bool    done;
    uct_md_h         md;
    uct_mem_h        memh;
} zcopy_comp_t;

// The user of RMA serice needs to provide
// this funciton to provide the actual data to
// send though RMA
void* (*g_serve_rma_data)(void*, size_t *size);

static pthread_t g_progress_thread;
pthread_mutex_t  g_lock = PTHREAD_MUTEX_INITIALIZER;

void* pack_request_arg(void* my_addr, void* rkey_buf, size_t rkey_buf_size,
                            void* user_arg, size_t user_arg_size, size_t *total_size) {
    // format:
    // | my_rank | my_addr | rkey_buf_size | rkey_buf | user_arg_size | user_arg |
    *total_size = sizeof(int) +  sizeof(uint64_t) + sizeof(size_t)*2 + rkey_buf_size + user_arg_size;

    int pos = 0;
    void* send_buf = malloc(*total_size);

    memcpy(send_buf+pos, &g_mpi_rank, sizeof(int));
    pos += sizeof(int);

    uint64_t tmp_addr = (uint64_t)my_addr;
    memcpy(send_buf+pos, &tmp_addr, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(send_buf+pos, &rkey_buf_size, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(send_buf+pos, rkey_buf, rkey_buf_size);
    pos += rkey_buf_size;

    memcpy(send_buf+pos, &user_arg_size, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(send_buf+pos, user_arg, user_arg_size);
    pos += user_arg_size;

    return send_buf;
}

void zcopy_completion_cb(uct_completion_t *self) {
    zcopy_comp_t *comp = (zcopy_comp_t *)self;
    if (comp->memh != UCT_MEM_HANDLE_NULL)
        uct_md_mem_dereg(comp->md, comp->memh);
    comp->done = true;
}

void build_zcopy_comp(zcopy_comp_t *comp) {
    comp->uct_comp.func   = zcopy_completion_cb;
    comp->uct_comp.count  = 1;
    comp->uct_comp.status = UCS_OK;
    comp->done            = false;
    comp->memh            = UCT_MEM_HANDLE_NULL;
}

void build_iov_and_zcopy_comp(uct_iov_t *iov, zcopy_comp_t *comp, my_info_t* info, void* buf, size_t buf_len) {
    uct_mem_h memh;
    if (info->md_attr.cap.flags & UCT_MD_FLAG_NEED_MEMH)
        uct_md_mem_reg(info->md, buf, buf_len, UCT_MD_MEM_ACCESS_RMA, &memh);
    else
        memh = UCT_MEM_HANDLE_NULL;

    iov->buffer = buf;
    iov->length = buf_len;
    iov->memh   = memh;
    iov->stride = 0;
    iov->count  = 1;

    build_zcopy_comp(comp);
    comp->md              = info->md;    // in case we need to free memh
    comp->memh            = iov->memh;
}


void do_am_zcopy(my_info_t *info, uint8_t id, int dest, void* buf, size_t buf_len) {

    uct_iov_t iov;
    zcopy_comp_t comp;
    build_iov_and_zcopy_comp(&iov, &comp, info, buf, buf_len);

    ucs_status_t status = UCS_OK;
    do {
        pthread_mutex_lock(&g_lock);
        status = uct_ep_am_zcopy(info->eps[dest], id, NULL, 0, &iov, 1, 0, (uct_completion_t *)&comp);
        pthread_mutex_unlock(&g_lock);
    } while (status == UCS_ERR_NO_RESOURCE);

    if (status == UCS_INPROGRESS) {
        while (!comp.done) {
        }
    }
}

void do_put_zcopy(my_info_t* info, int dest_rank, uint64_t remote_addr, uct_rkey_t rkey, void* buf, size_t buf_len) {
    uct_iov_t iov;
    zcopy_comp_t comp;
    build_iov_and_zcopy_comp(&iov, &comp, &g_res_info, buf, buf_len);


    ucs_status_t status = UCS_OK;
    do {
        pthread_mutex_lock(&g_lock);
        status = uct_ep_put_zcopy(info->eps[dest_rank], &iov, 1, remote_addr, rkey, (uct_completion_t *)&comp);
        pthread_mutex_unlock(&g_lock);
        // we have a dedicated pthread make progress
    } while (status == UCS_ERR_NO_RESOURCE);

    if (status == UCS_INPROGRESS) {
        while (!comp.done) {
            // we have a dedicated pthread make progress
        }
    }

}


void* rma_respond(void* data) {
    int dest_rank;
    uint64_t remote_addr;
    size_t rkey_buf_size, user_arg_size;
    void *rkey_buf, *user_arg;

    int pos = 0;
    memcpy(&dest_rank, data+pos, sizeof(int));
    pos += sizeof(int);

    memcpy(&remote_addr, data+pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(&rkey_buf_size, data+pos, sizeof(size_t));
    pos += sizeof(size_t);

    rkey_buf = malloc(rkey_buf_size);
    memcpy(rkey_buf, data+pos, rkey_buf_size);
    pos += rkey_buf_size;

    memcpy(&user_arg_size, data+pos, sizeof(size_t));
    pos += sizeof(size_t);
    user_arg = data+pos;

    // Get rkey
    uct_rkey_bundle_t rkey_ob;
    uct_rkey_unpack(g_res_info.component, rkey_buf, &rkey_ob);
    free(rkey_buf);

    // RMA
    size_t buf_len;
    void* buf = g_serve_rma_data(user_arg, &buf_len);
    do_put_zcopy(&g_res_info, dest_rank, remote_addr, rkey_ob.rkey, buf, buf_len);

    // Send ACK
    do_am_zcopy(&g_res_info, AM_ID_RMA_RESPOND, dest_rank, NULL, 0);

    uct_rkey_release(g_res_info.component, &rkey_ob);
    free(buf);

    return NULL;
}

static ucs_status_t rma_request_listener(void *arg, void *data, size_t length, unsigned flags) {
    printf("data received length: %lu\n", length);
    // Note this is called by pthread-progress funciton.
    // we can not do am_send here as no one will perform the progress
    // So we create a new thread to handle this rma request

    void* persist_data = malloc(length);
    memcpy(persist_data, data, length);
    pthread_t thread;
    pthread_create(&thread, NULL, rma_respond, persist_data);

    return UCS_OK;
}

static ucs_status_t rma_respond_listener(void *arg, void *data, size_t length, unsigned flags) {
    g_received_respond = true;
    return UCS_OK;
}



/** Send a RMA request
 * (1) connect
 * (2) register memory
 * (3) send my rkey
 * (4) wait for ack
 *
 * void* recv_buf is the user's buffer in tfs_read()
 * we will check if it is page-aligned, if so we
 * will use it directly for RMA.
 * if not we let UCX to allocate memory for RMA
 * and copy it to user's buffer.
 *
 * this function should be called by the main thread.
 */
void tangram_ucx_rma_request(int dest_rank, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size) {
    ucs_status_t status;
    // TODO: should directly put data to user's buffer
    g_received_respond = false;

    uct_mem_alloc_params_t params;
    params.field_mask = UCT_MEM_ALLOC_PARAM_FIELD_ADDRESS  |
                        UCT_MEM_ALLOC_PARAM_FIELD_MEM_TYPE;
    params.address    = NULL;
    params.mem_type   = UCS_MEMORY_TYPE_HOST;
    // TODO which one is the best?
    uct_alloc_method_t methods[] = {UCT_ALLOC_METHOD_MD, UCT_ALLOC_METHOD_HEAP};
    uct_allocated_memory_t mem;
    uct_mem_alloc(recv_size, methods, 2, &params, &mem);

    uct_mem_h memh;
    if(mem.method != UCT_ALLOC_METHOD_MD) {
        uct_md_mem_reg(g_res_info.md, mem.address, mem.length, UCT_MD_MEM_ACCESS_RMA, &memh);
    }

    // pack rkey buf
    void* rkey_buf = alloca(g_res_info.md_attr.rkey_packed_size);
    uct_md_mkey_pack(g_res_info.md, memh, rkey_buf);


    // send to peer
    size_t sendbuf_size;
    void* sendbuf = pack_request_arg(mem.address, rkey_buf, g_res_info.md_attr.rkey_packed_size,
                                    user_arg, user_arg_size, &sendbuf_size);

    do_am_zcopy(&g_res_info, AM_ID_RMA_REQUEST, dest_rank, sendbuf, sendbuf_size);


    // Wait for the respond
    while(!g_received_respond) {
    }


    uct_mem_free(&mem);

    /*
    // Now we should have the data ready
    memcpy(recv_buf, attr.address, recv_size);

    status = ucp_mem_unmap(g_ucp_context, memh);
    assert(status == UCS_OK);

    ep_close(g_request_worker, ep);
    */
}

void exchange_dev_iface_addr(void* dev_addr, void* iface_addr, my_info_t *info) {
    MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

    size_t addr_len;
    void*  all_addrs;

    // exchange device addrs
    addr_len = info->iface_attr.device_addr_len;
    all_addrs = malloc(addr_len*g_mpi_size);
    MPI_Allgather(dev_addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    info->dev_addrs = malloc(g_mpi_size*sizeof(uct_device_addr_t*));
    for(int rank = 0; rank < g_mpi_size; rank++) {
        info->dev_addrs[rank] = malloc(addr_len);
        memcpy(info->dev_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);

    // exchange iface addrs
    addr_len = info->iface_attr.iface_addr_len;
    all_addrs = malloc(addr_len*g_mpi_size);
    MPI_Allgather(iface_addr, addr_len, MPI_BYTE, all_addrs, addr_len, MPI_BYTE, MPI_COMM_WORLD);

    info->iface_addrs = malloc(g_mpi_size*sizeof(uct_ep_addr_t*));
    for(int rank = 0; rank < g_mpi_size; rank++) {
        info->iface_addrs[rank] = malloc(addr_len);
        memcpy(info->iface_addrs[rank], all_addrs+rank*addr_len, addr_len);
    }
    free(all_addrs);
}

void* progress_loop(void* arg) {
    while(g_running) {
        pthread_mutex_lock(&g_lock);
        uct_worker_progress(g_res_info.worker);
        pthread_mutex_unlock(&g_lock);
    }
}


void init_iface(char* dev_name, char* tl_name, my_info_t *info) {
    ucs_status_t        status;
    uct_iface_config_t  *config; /* Defines interface configuration options */
    uct_iface_params_t  params;
    params.field_mask           = UCT_IFACE_PARAM_FIELD_OPEN_MODE   |
                                  UCT_IFACE_PARAM_FIELD_DEVICE      |
                                  UCT_IFACE_PARAM_FIELD_STATS_ROOT  |
                                  UCT_IFACE_PARAM_FIELD_RX_HEADROOM |
                                  UCT_IFACE_PARAM_FIELD_CPU_MASK;
    params.open_mode            = UCT_IFACE_OPEN_MODE_DEVICE;
    params.mode.device.tl_name  = tl_name;
    params.mode.device.dev_name = dev_name;
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

void dev_tl_lookup(char* dev_name, char* tl_name, my_info_t *info) {

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

void init_my_info(char* dev_name, char* tl_name, my_info_t *info) {
    uct_worker_create(g_context, UCS_THREAD_MODE_SERIALIZED, &info->worker);

    // search for dev and tl
    // This will keep open info->md and set info->md_attr
    dev_tl_lookup(dev_name, tl_name, info);

    // This will open the info->iface and set info->iface_attr
    init_iface(dev_name, tl_name, info);

    // Exchange device and iface addresses
    uct_device_addr_t *dev_addr = alloca(info->iface_attr.device_addr_len);
    uct_iface_get_device_address(info->iface, dev_addr);
    uct_iface_addr_t *iface_addr = alloca(info->iface_attr.iface_addr_len);
    uct_iface_get_address(info->iface, iface_addr);
    exchange_dev_iface_addr(dev_addr, iface_addr, info);

    // Create and connect to all other rank's endpoint
    assert(info->iface_attr.cap.flags & UCT_IFACE_FLAG_CONNECT_TO_IFACE);
    info->eps = malloc(sizeof(uct_ep_h) * g_mpi_size);
    ucs_status_t status;
    for(int rank = 0; rank < g_mpi_size; rank++) {
        uct_ep_h ep;
        uct_ep_params_t ep_params;
        ep_params.field_mask = UCT_EP_PARAM_FIELD_IFACE     |
                               UCT_EP_PARAM_FIELD_DEV_ADDR  |
                               UCT_EP_PARAM_FIELD_IFACE_ADDR;
        ep_params.iface      = info->iface;
        ep_params.dev_addr   = info->dev_addrs[rank];
        ep_params.iface_addr = info->iface_addrs[rank];

        status = uct_ep_create(&ep_params, &info->eps[rank]);
        assert(status == UCS_OK);
    }

}

void destroy_my_info(my_info_t *info) {
    for(int rank = 0; rank < g_mpi_size; rank++) {
        free(info->dev_addrs[rank]);
        free(info->iface_addrs[rank]);
    }

    for(int rank = 0; rank < g_mpi_rank; rank++)
        uct_ep_destroy(info->eps[rank]);
    uct_iface_close(info->iface);
    uct_md_close(info->md);
    free(info->eps);
    free(info->dev_addrs);
    free(info->iface_addrs);
    uct_worker_destroy(info->worker);
}


void tangram_ucx_rma_service_start(void* (serve_rma_data)(void*, size_t*)) {

    g_serve_rma_data = serve_rma_data;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_context);

    init_my_info("enp6s0", "tcp", &g_res_info);

    status = uct_iface_set_am_handler(g_res_info.iface, AM_ID_RMA_REQUEST, rma_request_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_res_info.iface, AM_ID_RMA_RESPOND, rma_respond_listener, NULL, 0);
    assert(status == UCS_OK);

    pthread_create(&g_progress_thread, NULL, progress_loop, NULL);
    MPI_Barrier(MPI_COMM_WORLD);
}

void tangram_ucx_rma_service_stop() {

    MPI_Barrier(MPI_COMM_WORLD);

    g_running = false;
    pthread_join(g_progress_thread, NULL);

    destroy_my_info(&g_res_info);
    ucs_async_context_destroy(g_context);
}
