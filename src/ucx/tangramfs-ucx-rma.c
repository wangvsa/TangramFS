#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <alloca.h>
#include <uct/api/uct.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"


static ucs_async_context_t* g_rma_async;
TFS_Info*                   g_tfs_info;

/**
 * Request context is for sending RMA request
 * this is used by the main thread
 *
 * Respond context is for perforimng the acutal RMA put
 * this is done by a pthread spawned upon receiving the request.
 *
 * We only allow one outgoing request or respond at a time
 * so we use pthread mutex to protect the rma_request() and
 * rma_respond() function.
 *
 */
static tangram_uct_context_t  g_request_context;
static tangram_uct_context_t  g_respond_context;

uct_device_addr_t**           g_respond_dev_addrs;
uct_device_addr_t**           g_request_dev_addrs;



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


void* pack_request_arg(void* ep_addr, void* my_addr, void* rkey_buf, size_t rkey_buf_size,
                            void* user_arg, size_t user_arg_size, size_t *total_size) {
    // format:
    // | my rank | ep_addr_len | ep_addr | my_addr | rkey_buf_size | rkey_buf | user_arg_size | user_arg |
    *total_size = sizeof(int) + sizeof(uint64_t) + sizeof(size_t)*3 +
        g_request_context.iface_attr.ep_addr_len + rkey_buf_size + user_arg_size;

    int pos = 0;
    void* send_buf = malloc(*total_size);

    memcpy(send_buf+pos, &g_tfs_info->mpi_rank, sizeof(int));
    pos += sizeof(int);

    memcpy(send_buf+pos, &g_request_context.iface_attr.ep_addr_len, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(send_buf+pos, ep_addr, g_request_context.iface_attr.ep_addr_len);
    pos += g_request_context.iface_attr.ep_addr_len;

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

void ep_create_get_address(tangram_uct_context_t* info, uct_ep_h *ep, uct_ep_addr_t* ep_addr) {
    uct_ep_params_t ep_params;
    ep_params.field_mask = UCT_EP_PARAM_FIELD_IFACE;
    ep_params.iface      = info->iface;

    ucs_status_t status = uct_ep_create(&ep_params, ep);
    assert(status == UCS_OK);

    uct_ep_get_address(*ep, ep_addr);
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
}

void build_iov_and_zcopy_comp(uct_iov_t *iov, zcopy_comp_t *comp, tangram_uct_context_t* info, void* buf, size_t buf_len) {
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

// Use am zcopy as qib0:1/rc_verbs does not support am short.
void do_am_zcopy(uct_ep_h ep, tangram_uct_context_t* info, uint8_t id, void* data, size_t len) {
    uct_iov_t iov;
    zcopy_comp_t comp;
    build_iov_and_zcopy_comp(&iov, &comp, info, data, len);

    ucs_status_t status = UCS_OK;
    do {
        status = uct_ep_am_zcopy(ep, id, NULL, 0, &iov, 1, 0,  (uct_completion_t *)&comp);
        uct_worker_progress(info->worker);
    } while (status == UCS_ERR_NO_RESOURCE);

    if (status == UCS_INPROGRESS) {
        while (!comp.done) {
            uct_worker_progress(info->worker);
        }
    }
}

void do_put_zcopy(uct_ep_h ep, tangram_uct_context_t* info, uint64_t remote_addr,
                    uct_rkey_t rkey, void* buf, size_t buf_len) {
    uct_iov_t iov;
    zcopy_comp_t comp;
    build_iov_and_zcopy_comp(&iov, &comp, info, buf, buf_len);

    ucs_status_t status = UCS_OK;
    do {
        status = uct_ep_put_zcopy(ep, &iov, 1, remote_addr, rkey, (uct_completion_t *)&comp);
        uct_worker_progress(info->worker);
    } while (status == UCS_ERR_NO_RESOURCE);

    if (status == UCS_INPROGRESS) {
        while (!comp.done) {
            uct_worker_progress(info->worker);
        }
    }
}

void* rma_respond(void* data) {
    pthread_mutex_lock(&g_respond_context.mutex);
    int dest_rank;
    uint64_t remote_addr;
    size_t rkey_buf_size, user_arg_size;
    void *rkey_buf, *user_arg;
    size_t ep_addr_len;
    uct_ep_addr_t* peer_ep_addr;

    int pos = 0;
    memcpy(&dest_rank, data+pos, sizeof(int));
    pos += sizeof(int);

    memcpy(&ep_addr_len, data+pos, sizeof(size_t));
    pos += sizeof(size_t);

    peer_ep_addr = alloca(ep_addr_len);
    memcpy(peer_ep_addr, data+pos, ep_addr_len);
    pos += ep_addr_len;

    memcpy(&remote_addr, data+pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(&rkey_buf_size, data+pos, sizeof(size_t));
    pos += sizeof(size_t);

    rkey_buf = alloca(rkey_buf_size);
    memcpy(rkey_buf, data+pos, rkey_buf_size);
    pos += rkey_buf_size;

    memcpy(&user_arg_size, data+pos, sizeof(size_t));
    pos += sizeof(size_t);
    user_arg = data+pos;

    // First send back my ep address
    ucs_status_t status;
    uct_ep_h ep;
    uct_ep_addr_t* ep_addr = alloca(g_respond_context.iface_attr.ep_addr_len);
    ep_create_get_address(&g_respond_context, &ep, ep_addr);
    tangram_ucx_send_ep_addr(AM_ID_RMA_EP_ADDR, dest_rank, ep_addr, g_respond_context.iface_attr.ep_addr_len);

    status = uct_ep_connect_to_ep(ep, g_request_dev_addrs[dest_rank], peer_ep_addr);
    assert(status == UCS_OK);

    // Get rkey
    uct_rkey_bundle_t rkey_ob;
    status = uct_rkey_unpack(g_respond_context.component, rkey_buf, &rkey_ob);
    assert(status == UCS_OK);

    // RMA
    size_t buf_len;
    void* buf = g_serve_rma_data(user_arg, &buf_len);
    do_put_zcopy(ep, &g_respond_context, remote_addr, rkey_ob.rkey, buf, buf_len);

    // Send ACK
    int garbage_data;
    do_am_zcopy(ep, &g_respond_context, AM_ID_RMA_RESPOND, &garbage_data, sizeof(int));

    uct_rkey_release(g_respond_context.component, &rkey_ob);
    uct_ep_destroy(ep);
    free(buf);

    pthread_mutex_unlock(&g_respond_context.mutex);
    return NULL;
}

static ucs_status_t am_rma_respond_listener(void *arg, void *data, size_t length, unsigned flags) {
    g_request_context.respond_flag = true;
    return UCS_OK;
}


/** Send a RMA request and wait for the peer
 *  to do the RMA put. There should be only
 *  one outgoing RMA request at a time.
 * (1) connect
 * (2) register memory
 * (3) send my rkey
 * (4) wait for ack
 *
 * void* recv_buf is the user's buffer in fs_read()
 * we will check if it is page-aligned, if so we
 * will use it directly for RMA.
 * if not we let UCX to allocate memory for RMA
 * and copy it to user's buffer.
 m
 * this function should be called by the main thread.
 */
void tangram_ucx_rma_request(int dest_rank, void* user_arg, size_t user_arg_size, void* recv_buf, size_t recv_size) {
    pthread_mutex_lock(&g_request_context.mutex);
    ucs_status_t status;
    // TODO: should directly put data to user's buffer

    uct_ep_h ep;
    uct_ep_addr_t* ep_addr = alloca(g_request_context.iface_attr.ep_addr_len);
    ep_create_get_address(&g_request_context, &ep, ep_addr);

    uct_mem_alloc_params_t params;
    params.field_mask = UCT_MEM_ALLOC_PARAM_FIELD_ADDRESS  |
                        UCT_MEM_ALLOC_PARAM_FIELD_MEM_TYPE;
    params.address    = NULL;
    params.mem_type   = UCS_MEMORY_TYPE_HOST;
    // TODO which one is the best?
    uct_alloc_method_t methods[] = {UCT_ALLOC_METHOD_MD, UCT_ALLOC_METHOD_HEAP};
    uct_allocated_memory_t mem;
    status = uct_mem_alloc(recv_size, methods, 2, &params, &mem);
    assert(mem.address && status == UCS_OK);

    uct_mem_h memh;
    if(mem.method != UCT_ALLOC_METHOD_MD)
        uct_md_mem_reg(g_request_context.md, mem.address, mem.length, UCT_MD_MEM_ACCESS_RMA, &memh);

    // pack rkey buf
    void* rkey_buf = alloca(g_request_context.md_attr.rkey_packed_size);
    uct_md_mkey_pack(g_request_context.md, memh, rkey_buf);

    // send to peer and get peer ep address to connect
    size_t sendbuf_size;
    void* sendbuf = pack_request_arg(ep_addr, mem.address, rkey_buf, g_request_context.md_attr.rkey_packed_size,
                                    user_arg, user_arg_size, &sendbuf_size);

    // Send RMA request to the destination client and wait
    // it to send back the ep address.
    g_request_context.respond_flag = false;
    uct_ep_addr_t* peer_ep_addr = alloca(g_request_context.iface_attr.ep_addr_len);
    tangram_ucx_sendrecv_peer(AM_ID_RMA_REQUEST, dest_rank, sendbuf, sendbuf_size, peer_ep_addr);

    status = uct_ep_connect_to_ep(ep, g_respond_dev_addrs[dest_rank], peer_ep_addr);
    assert(status == UCS_OK);

    // Once we made the connection with the dest clien,
    // we wait for it to finish the RMA put
    while(!g_request_context.respond_flag)
        uct_worker_progress(g_request_context.worker);
    memcpy(recv_buf, mem.address, recv_size);

    uct_ep_destroy(ep);

    if(mem.method != UCT_ALLOC_METHOD_MD)
        uct_md_mem_dereg(g_request_context.md, memh);
    uct_mem_free(&mem);
    pthread_mutex_unlock(&g_request_context.mutex);
}

void tangram_ucx_rma_service_start(TFS_Info* tfs_info, void* (serve_rma_data)(void*, size_t*)) {
    g_tfs_info = tfs_info;

    g_serve_rma_data = serve_rma_data;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_rma_async);

    tangram_uct_context_init(g_rma_async, g_tfs_info->rma_dev_name, g_tfs_info->rma_tl_name, false, &g_request_context);
    tangram_uct_context_init(g_rma_async, g_tfs_info->rma_dev_name, g_tfs_info->rma_tl_name, false, &g_respond_context);

    g_respond_dev_addrs = malloc(g_tfs_info->mpi_size * sizeof(uct_device_addr_t*));
    g_request_dev_addrs = malloc(g_tfs_info->mpi_size * sizeof(uct_device_addr_t*));
    exchange_dev_iface_addr(&g_respond_context, g_respond_dev_addrs, NULL);
    exchange_dev_iface_addr(&g_request_context, g_request_dev_addrs, NULL);

    status = uct_iface_set_am_handler(g_request_context.iface, AM_ID_RMA_RESPOND, am_rma_respond_listener, NULL, 0);
    assert(status == UCS_OK);
}

void tangram_ucx_rma_service_stop() {
    MPI_Barrier(g_tfs_info->mpi_comm);

    for(int i = 0; i < g_tfs_info->mpi_size; i++) {
        free(g_respond_dev_addrs[i]);
        free(g_request_dev_addrs[i]);
    }
    free(g_respond_dev_addrs);
    free(g_request_dev_addrs);

    tangram_uct_context_destroy(&g_request_context);
    tangram_uct_context_destroy(&g_respond_context);

    ucs_async_context_destroy(g_rma_async);
}
