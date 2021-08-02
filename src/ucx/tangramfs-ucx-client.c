#define _POSIX_C_SOURCE 200112L
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>
#include <mpi.h>
#include <sys/time.h>
#include "tangramfs-ucx.h"
#include "tangramfs-ucx-comm.h"

static int          g_mpi_rank;

static bool                 g_running = true;

static void*                g_server_respond;
static volatile bool        g_received_respond;

static uct_listener_info_t  g_client_info_s;
static uct_listener_info_t  g_client_info_r;
static ucs_async_context_t* g_client_context;


static pthread_t g_client_progress_thread;
pthread_mutex_t g_sendpeer_lock = PTHREAD_MUTEX_INITIALIZER;

static volatile bool g_request_sent = false;

inline double get_ts() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return (double)tv.tv_sec+(double)tv.tv_usec/1000000.0;
}

static ucs_status_t am_query_respond_listener(void *arg, void *data, size_t length, unsigned flags) {
    g_received_respond = true;
    if(g_server_respond) {
        memcpy(g_server_respond, data+sizeof(uint64_t), length-sizeof(uint64_t));
    }
    return UCS_OK;
}

static ucs_status_t am_post_respond_listener(void *arg, void *data, size_t length, unsigned flags) {
    g_received_respond = true;
    return UCS_OK;
}

static ucs_status_t am_rma_request_listener(void *arg, void *data, size_t length, unsigned flags) {
    // Note this is called by pthread-progress funciton.
    // we can not do am_send here as no one will perform the progress
    // So we create a new thread to handle this rma request
    //printf("%.6f rma request received %d\n", get_ts(), g_mpi_rank);
    void* persist_data = malloc(length-sizeof(uint64_t));
    memcpy(persist_data, data+sizeof(uint64_t), length-sizeof(uint64_t));
    pthread_t thread;
    pthread_create(&thread, NULL, rma_respond, persist_data);
    return UCS_OK;
}

static ucs_status_t am_ep_addr_listener(void *arg, void *data, size_t length, unsigned flags) {
    //printf("%.6f ep addr received %d\n", get_ts(), g_mpi_rank);
    void* peer_ep_addr = malloc(length-sizeof(uint64_t));
    memcpy(peer_ep_addr, data+sizeof(uint64_t), length-sizeof(uint64_t));
    set_peer_ep_addr(peer_ep_addr);
    return UCS_OK;
}


void* client_progress_loop(void* arg) {
    // TODO seems that the am handler must be set
    // by the progress thread
    while(g_running) {
        uct_worker_progress(g_client_info_r.worker);
    }
}

void tangram_ucx_sendrecv(uint8_t id, void* data, size_t length, void* respond) {
    pthread_mutex_lock(&g_sendpeer_lock);
    g_server_respond = respond;
    g_received_respond = false;

    uct_ep_h ep;
    uct_ep_create_connect(g_client_info_s.iface, g_client_info_s.server_dev_addr, g_client_info_s.server_iface_addr, &ep);

    do_uct_am_short_progress(g_client_info_s.worker, ep, id, g_mpi_rank, data, length);
    while(!g_received_respond) {
    }

    uct_ep_destroy(ep);
    pthread_mutex_unlock(&g_sendpeer_lock);
}



void tangram_ucx_send_peer(uint8_t id, int dest, void* data, size_t length) {
    // TODO how to optimize the mutex lock here?
    pthread_mutex_lock(&g_sendpeer_lock);
    uct_ep_h ep;

    uct_ep_create_connect(g_client_info_s.iface, g_client_info_r.client_dev_addrs[dest], g_client_info_r.client_iface_addrs[dest], &ep);

    do_uct_am_short_progress(g_client_info_s.worker, ep, id, g_mpi_rank, data, length);

    uct_iface_fence(g_client_info_s.iface, 0);
    ucs_status_t status = uct_ep_flush(ep, UCT_FLUSH_FLAG_LOCAL, NULL);
    assert(status == UCS_OK);
    uct_ep_destroy(ep);

    pthread_mutex_unlock(&g_sendpeer_lock);
}

void tangram_ucx_stop_server() {
    uct_ep_h ep;
    uct_ep_create_connect(g_client_info_s.iface, g_client_info_s.server_dev_addr, g_client_info_s.server_iface_addr, &ep);
    do_uct_am_short_progress(g_client_info_s.worker, ep, AM_ID_STOP_REQUEST, g_mpi_rank, NULL, 0);
    uct_ep_destroy(ep);
}

void send_address_to_server() {
    uct_ep_h ep;
    uct_ep_create_connect(g_client_info_s.iface, g_client_info_s.server_dev_addr, g_client_info_s.server_iface_addr, &ep);

    size_t len = 2 * sizeof(size_t) + g_client_info_r.iface_attr.device_addr_len + g_client_info_r.iface_attr.iface_addr_len;
    void* data = malloc(len);

    memcpy(data, &g_client_info_r.iface_attr.device_addr_len, sizeof(size_t));
    memcpy(data+sizeof(size_t), g_client_info_r.client_dev_addrs[g_mpi_rank], g_client_info_r.iface_attr.device_addr_len);
    memcpy(data+sizeof(size_t)+g_client_info_r.iface_attr.device_addr_len, &g_client_info_r.iface_attr.iface_addr_len, sizeof(size_t));
    memcpy(data+2*sizeof(size_t)+g_client_info_r.iface_attr.device_addr_len, g_client_info_r.client_iface_addrs[g_mpi_rank], g_client_info_r.iface_attr.iface_addr_len);

    do_uct_am_short_progress(g_client_info_s.worker, ep, AM_ID_CLIENT_ADDR, g_mpi_rank, data, len);

    uct_ep_destroy(ep);
    free(data);
}

void tangram_ucx_rpc_service_start(const char* persist_dir) {
    MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);

    ucs_status_t status;
    ucs_async_context_create(UCS_ASYNC_MODE_THREAD_SPINLOCK, &g_client_context);

    //init_uct_listener_info(g_client_context, "enp6s0", "tcp", false, &g_client_info);
    init_uct_listener_info(g_client_context, "hsi0", "tcp", false, &g_client_info_s);
    init_uct_listener_info(g_client_context, "hsi0", "tcp", false, &g_client_info_r);
    //init_uct_listener_info(g_client_context, "qib0:1", "ud_verbs", false, &g_client_info);

    status = uct_iface_set_am_handler(g_client_info_r.iface, AM_ID_QUERY_RESPOND, am_query_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_info_r.iface, AM_ID_POST_RESPOND, am_post_respond_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_info_r.iface, AM_ID_RMA_REQUEST, am_rma_request_listener, NULL, 0);
    assert(status == UCS_OK);
    status = uct_iface_set_am_handler(g_client_info_r.iface, AM_ID_RMA_EP_ADDR, am_ep_addr_listener, NULL, 0);
    assert(status == UCS_OK);

    pthread_create(&g_client_progress_thread, NULL, client_progress_loop, NULL);

    send_address_to_server();
}


void tangram_ucx_rpc_service_stop() {
    ucs_status_t status = uct_iface_flush(g_client_info_s.iface, UCT_FLUSH_FLAG_LOCAL, NULL);
    assert(status == UCS_OK);

    MPI_Barrier(MPI_COMM_WORLD);
    g_running = false;
    pthread_join(g_client_progress_thread, NULL);

    destroy_uct_listener_info(&g_client_info_s);
    destroy_uct_listener_info(&g_client_info_r);
    ucs_async_context_destroy(g_client_context);
}
