#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <unistd.h>
#include <stdbool.h>
#include <alloca.h>
#include "tangramfs-ucx-rma.h"
#include "tangramfs-ucx-client.h"

static bool                 g_rma_running;
pthread_t                   g_rma_progress_thread;
tangram_rma_req_in_t*       g_rma_req_in;

static ucs_async_context_t* g_rma_async;
tfs_info_t*                 gg_tfs_info;

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
static tangram_uct_context_t  g_outgoing_context;
static tangram_uct_context_t  g_ingoing_context;


typedef struct zcopy_comp {
    uct_completion_t uct_comp;
    volatile bool    done;
    uct_md_h         md;
    uct_mem_h        memh;
} zcopy_comp_t;


// The user of RMA serice needs to provide
// this funciton to provide the actual data to
// send though RMA
void* (*g_serve_rma_data_cb)(void*, size_t *size);

void  rma_respond(tangram_rma_req_in_t* in);
void* rma_req_in_pack(tangram_rma_req_in_t* in, size_t* total_size);
void  rma_req_in_unpack(void* buf, tangram_rma_req_in_t* in);
void  rma_req_in_free(tangram_rma_req_in_t* in);


static ucs_status_t am_rma_request_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    g_rma_req_in = malloc(sizeof(tangram_rma_req_in_t));
    void* tmp;
    unpack_rpc_buffer(buf, buf_len, &g_rma_req_in->src, &tmp);
    rma_req_in_unpack(tmp, g_rma_req_in);
    free(tmp);
    return UCS_OK;
}

static ucs_status_t am_ep_addr_listener(void *arg, void *buf, size_t buf_len, unsigned flags) {
    unpack_rpc_buffer(buf, buf_len, TANGRAM_UCT_ADDR_IGNORE, g_outgoing_context.respond_ptr);
    g_outgoing_context.respond_flag = true;
    return UCS_OK;
}

static ucs_status_t am_rma_respond_listener(void *arg, void *data, size_t length, unsigned flags) {
    g_outgoing_context.respond_flag = true;
    return UCS_OK;
}

// Assume the mutex is locked by the caller
void rma_sendrecv_core(uint8_t id, tangram_uct_context_t* context, tangram_uct_addr_t* dest, void* data, size_t length, void** respond_ptr) {
    context->respond_ptr  = respond_ptr;
    context->respond_flag = false;

    uct_ep_h ep;
    uct_ep_create_connect(context->iface, dest, &ep);

    do_uct_am_short_progress(context->worker, ep, id, &context->self_addr, data, length);

    // if need to wait for a respond
    if(respond_ptr != NULL) {
        while(!context->respond_flag) {
            uct_worker_progress(context->worker);
        }
    }
    uct_ep_destroy(ep);
}


void* rma_req_in_pack(tangram_rma_req_in_t* in, size_t* total_size) {
    // Format:
    // ep_addr_len  | ep_addr           sizeof(size_t) + ep_addr_len
    // dev_addr_len | dev_addr          sizeof(size_t) + dev_addr_len
    // mem_addr                         sizeof(uint64_t)
    // rkey_len     | rkey              sizeof(size_t) + rkey_len
    // user_arg_len | user_arg          sizeof(size_t) + user_arg_len
    *total_size = sizeof(uint64_t) + sizeof(size_t)*4 + in->ep_addr_len + in->dev_addr_len + in->rkey_len + in->user_arg_len;

    int pos = 0;
    void* buf = malloc(*total_size);

    memcpy(buf+pos, &in->ep_addr_len, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(buf+pos, in->ep_addr, in->ep_addr_len);
    pos += in->ep_addr_len;

    memcpy(buf+pos, &in->dev_addr_len, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(buf+pos, in->dev_addr, in->dev_addr_len);
    pos += in->dev_addr_len;

    memcpy(buf+pos, &in->mem_addr, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(buf+pos, &in->rkey_len, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(buf+pos, in->rkey, in->rkey_len);
    pos += in->rkey_len;

    memcpy(buf+pos, &in->user_arg_len, sizeof(size_t));
    pos += sizeof(size_t);

    memcpy(buf+pos, in->user_arg, in->user_arg_len);
    pos += in->user_arg_len;

    return buf;
}

void rma_req_in_unpack(void* buf, tangram_rma_req_in_t* in) {
    size_t pos = 0;

    memcpy(&in->ep_addr_len, buf+pos, sizeof(size_t));
    pos += sizeof(size_t);

    in->ep_addr = malloc(in->ep_addr_len);
    memcpy(in->ep_addr, buf+pos, in->ep_addr_len);
    pos += in->ep_addr_len;

    memcpy(&in->dev_addr_len, buf+pos, sizeof(size_t));
    pos += sizeof(size_t);

    in->dev_addr = malloc(in->dev_addr_len);
    memcpy(in->dev_addr, buf+pos, in->dev_addr_len);
    pos += in->dev_addr_len;

    memcpy(&in->mem_addr, buf+pos, sizeof(uint64_t));
    pos += sizeof(uint64_t);

    memcpy(&in->rkey_len, buf+pos, sizeof(size_t));
    pos += sizeof(size_t);

    in->rkey = malloc(in->rkey_len);
    memcpy(in->rkey, buf+pos, in->rkey_len);
    pos += in->rkey_len;

    memcpy(&in->user_arg_len, buf+pos, sizeof(size_t));
    pos += sizeof(size_t);

    in->user_arg = malloc(in->user_arg_len);
    memcpy(in->user_arg, buf+pos, in->user_arg_len);
}

void rma_req_in_free(tangram_rma_req_in_t* in) {
    tangram_uct_addr_free(&in->src);
    free(in->ep_addr);
    free(in->dev_addr);
    free(in->rkey);
    free(in->user_arg);
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

void rma_respond(tangram_rma_req_in_t* in) {
    pthread_mutex_lock(&g_ingoing_context.mutex);

    // First send back rma dev address and my ep address
    uct_ep_h ep;
    size_t ep_addr_len = g_ingoing_context.iface_attr.ep_addr_len;
    uct_ep_addr_t* ep_addr = alloca(ep_addr_len);
    ep_create_get_address(&g_ingoing_context, &ep, ep_addr);
    size_t dev_and_ep_len = sizeof(size_t)*2 + ep_addr_len + g_ingoing_context.self_addr.dev_len;
    void* dev_and_ep = alloca(dev_and_ep_len);
    memcpy(dev_and_ep, &ep_addr_len, sizeof(size_t));
    memcpy(dev_and_ep+sizeof(size_t), ep_addr, g_ingoing_context.iface_attr.ep_addr_len);
    memcpy(dev_and_ep+sizeof(size_t)+ep_addr_len, &g_ingoing_context.self_addr.dev_len, sizeof(size_t));
    memcpy(dev_and_ep+sizeof(size_t)*2+ep_addr_len, g_ingoing_context.self_addr.dev, g_ingoing_context.self_addr.dev_len);
    rma_sendrecv_core(AM_ID_RMA_EP_ADDR, &g_ingoing_context, &in->src, dev_and_ep, dev_and_ep_len, NULL);

    // Then connect to the request ep
    ucs_status_t status;
    status = uct_ep_connect_to_ep(ep, in->dev_addr, in->ep_addr);
    assert(status == UCS_OK);

    // Get rkey
    uct_rkey_bundle_t rkey_ob;
    status = uct_rkey_unpack(g_ingoing_context.component, in->rkey, &rkey_ob);
    assert(status == UCS_OK);

    // RMA
    size_t buf_len;
    void* buf = g_serve_rma_data_cb(in->user_arg, &buf_len);
    do_put_zcopy(ep, &g_ingoing_context, in->mem_addr, rkey_ob.rkey, buf, buf_len);

    // Send ACK
    int ack;
    do_am_zcopy(ep, &g_ingoing_context, AM_ID_RMA_RESPOND, &ack, sizeof(ack));

    uct_rkey_release(g_ingoing_context.component, &rkey_ob);
    uct_ep_destroy(ep);
    free(buf);

    pthread_mutex_unlock(&g_ingoing_context.mutex);
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
void tangram_ucx_rma_request(tangram_uct_addr_t* dest, void* user_arg, size_t user_arg_len, void* recv_buf, size_t recv_size) {
    pthread_mutex_lock(&g_outgoing_context.mutex);
    ucs_status_t status;
    // TODO: should directly put data to user's buffer

    // Fill in every filed of rma_req_in
    tangram_rma_req_in_t req_in;
    req_in.user_arg     = user_arg;
    req_in.user_arg_len = user_arg_len;

    req_in.dev_addr_len = g_outgoing_context.self_addr.dev_len;
    req_in.dev_addr     = alloca(req_in.dev_addr_len);

    req_in.ep_addr_len  = g_outgoing_context.iface_attr.ep_addr_len;
    req_in.ep_addr      = alloca(req_in.ep_addr_len);

    uct_ep_h ep;
    ep_create_get_address(&g_outgoing_context, &ep, req_in.ep_addr);

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
    req_in.mem_addr = (uint64_t) mem.address;

    uct_mem_h memh;
    if(mem.method != UCT_ALLOC_METHOD_MD)
        uct_md_mem_reg(g_outgoing_context.md, mem.address, mem.length, UCT_MD_MEM_ACCESS_RMA, &memh);

    req_in.rkey_len = g_outgoing_context.md_attr.rkey_packed_size;
    req_in.rkey     = alloca(req_in.rkey_len);
    uct_md_mkey_pack(g_outgoing_context.md, memh, req_in.rkey);

    // send to peer and get peer ep address to connect
    size_t sendbuf_size;
    void*  sendbuf = rma_req_in_pack(&req_in, &sendbuf_size);

    // Send RMA request to the destination client and wait
    // it to send back the ep address. The outgoing context
    // will receive a RMA_EP_ADDR am.
    void* peer_ep_dev = NULL;
    rma_sendrecv_core(AM_ID_RMA_REQUEST, &g_outgoing_context, dest, sendbuf, sendbuf_size, &peer_ep_dev);
    assert(peer_ep_dev != NULL);

    size_t peer_ep_len, peer_dev_len;
    memcpy(&peer_ep_len, peer_ep_dev, sizeof(size_t));
    uct_ep_addr_t *peer_ep_addr = alloca(peer_ep_len);
    memcpy(peer_ep_addr, peer_ep_dev+sizeof(size_t), peer_ep_len);
    memcpy(&peer_dev_len, peer_ep_dev+sizeof(size_t)+peer_ep_len, sizeof(size_t));
    uct_device_addr_t* peer_dev_addr = alloca(peer_dev_len);
    memcpy(peer_dev_addr, peer_ep_dev+sizeof(size_t)*2+peer_ep_len, peer_dev_len);

    status = uct_ep_connect_to_ep(ep, peer_dev_addr, peer_ep_addr);
    assert(status == UCS_OK);
    free(peer_ep_dev);

    // Once we made the connection with the dest client,
    // we wait for it to finish the RMA put
    // The client will send us a RMA_RESPOND am.
    g_outgoing_context.respond_flag = false;
    while(!g_outgoing_context.respond_flag)
        uct_worker_progress(g_outgoing_context.worker);
    memcpy(recv_buf, mem.address, recv_size);

    uct_ep_destroy(ep);

    if(mem.method != UCT_ALLOC_METHOD_MD)
        uct_md_mem_dereg(g_outgoing_context.md, memh);
    uct_mem_free(&mem);
    pthread_mutex_unlock(&g_outgoing_context.mutex);
}


void* rma_ingoing_progress_loop(void* arg) {
    while(g_rma_running) {
        pthread_mutex_lock(&g_ingoing_context.mutex);
        uct_worker_progress(g_ingoing_context.worker);
        pthread_mutex_unlock(&g_ingoing_context.mutex);

        // We have a new RMA request we need to handle
        if(g_rma_req_in != NULL) {
            rma_respond(g_rma_req_in);

            rma_req_in_free(g_rma_req_in);
            free(g_rma_req_in);
            g_rma_req_in = NULL;
        }
    }
    return NULL;
}

void tangram_ucx_rma_service_start(tfs_info_t* tfs_info, void* (serve_rma_data_cb)(void*, size_t*)) {

    gg_tfs_info = tfs_info;
    g_serve_rma_data_cb = serve_rma_data_cb;

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_rma_async);

    tangram_uct_context_init(g_rma_async, gg_tfs_info, false, &g_outgoing_context);
    tangram_uct_context_init(g_rma_async, gg_tfs_info, false, &g_ingoing_context);

    // Listen for incoming RMA request
    uct_iface_set_am_handler(g_ingoing_context.iface,  AM_ID_RMA_REQUEST, am_rma_request_listener, NULL, 0);

    // Send out RMA request and wait for dest client's ep addr and rma respond
    uct_iface_set_am_handler(g_outgoing_context.iface, AM_ID_RMA_EP_ADDR, am_ep_addr_listener, NULL, 0);
    uct_iface_set_am_handler(g_outgoing_context.iface, AM_ID_RMA_RESPOND, am_rma_respond_listener, NULL, 0);

    pthread_create(&g_rma_progress_thread, NULL, rma_ingoing_progress_loop, NULL);
}

void tangram_ucx_rma_service_stop() {
    g_rma_running = false;
    pthread_join(g_rma_progress_thread, NULL);

    tangram_uct_context_destroy(&g_outgoing_context);
    tangram_uct_context_destroy(&g_ingoing_context);
    ucs_async_context_destroy(g_rma_async);
}
