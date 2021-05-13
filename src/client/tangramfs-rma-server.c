#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include "mpi.h"
#include <mercury_macros.h>
#include "tangramfs.h"
#include "tangramfs-rma-server.h"
#include "tangramfs-utils.h"

static hg_class_t*     hg_class   = NULL;
static hg_context_t*   hg_context = NULL;

static bool running;
static pthread_t rma_server_progress_thread;


hg_return_t rpc_handler_transfer(hg_handle_t h);
hg_return_t rpc_handler_transfer_callback(const struct hg_cb_info *info);

void  mercury_rma_server_init(char* server_addr);
void  mercury_rma_server_finalize();
void  mercury_rma_server_register_rpcs();
void* mercury_rma_server_progress_loop(void* arg);


void tangram_rma_server_start(char* server_addr) {
    mercury_rma_server_init(server_addr);
    mercury_rma_server_register_rpcs();
    running = true;
    pthread_create(&rma_server_progress_thread, NULL, mercury_rma_server_progress_loop, NULL);
}

void tangram_rma_server_stop() {
    running = false;
    pthread_join(rma_server_progress_thread, NULL);
    mercury_rma_server_finalize();
}

/*
 * ------------------------------------------
 * Below are interal implementations
 * ------------------------------------------
 */

void mercury_rma_server_init(char* server_addr) {
    hg_return_t ret;

    hg_class = HG_Init(MERCURY_PROTOCOL, HG_TRUE);
    assert(hg_class != NULL);

    hg_size_t hostname_size;
    hg_addr_t self_addr;
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, NULL, &hostname_size, self_addr);
    HG_Addr_to_string(hg_class, server_addr, &hostname_size, self_addr);
    printf("Server running at address %s\n", server_addr);
    HG_Addr_free(hg_class, self_addr);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);
}

void mercury_rma_server_finalize() {
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);
}

void mercury_rma_server_register_rpcs() {
    hg_id_t rpc_id_transfer = MERCURY_REGISTER(hg_class, RPC_NAME_TRANSFER, rpc_transfer_in, rpc_transfer_out, rpc_handler_transfer);
}

void* mercury_rma_server_progress_loop(void* arg) {
    hg_return_t ret;
    do {
        unsigned int count = 0;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count);
        if(!running) break;
        ret = HG_Progress(hg_context, MERCURY_PROGRESS_TIMEOUT);
    } while(ret==HG_SUCCESS || ret == HG_TIMEOUT);

    return NULL;
}


/*
 * ------------------------------------------
 * Below are RPC handlers
 * ------------------------------------------
 */
typedef struct BulkTransferInfo_t {
    hg_handle_t handle;
    hg_bulk_t bulk_handle;
    void* buf;
    hg_size_t count;

    int rank;
    size_t offset;
} BulkTransferInfo;

hg_return_t rpc_handler_transfer(hg_handle_t h)
{
    hg_return_t ret;

    rpc_transfer_in in;
    HG_Get_input(h, &in);

    // Find out the location of the required data
    // TODO This is assumed that we have it
    size_t local_offset;
    TFS_File* tf = tangram_get_tfs_file(in.filename);
    assert(tf != NULL);
    bool exist = tangram_it_query(tf->it, in.offset, in.count, &local_offset);
    assert(exist);

    BulkTransferInfo *bt_info = tangram_malloc(sizeof(BulkTransferInfo));
    bt_info->handle = h;
    bt_info->count = in.count;
    bt_info->buf = tangram_malloc(bt_info->count);
    bt_info->rank = in.rank;
    bt_info->offset = in.offset;

    ssize_t res = pread(tf->local_fd, bt_info->buf, in.count, local_offset);

    const struct hg_info *hgi = HG_Get_info(h);

    ret = HG_Bulk_create(hgi->hg_class, 1, &bt_info->buf, &bt_info->count, HG_BULK_READWRITE, &bt_info->bulk_handle);
    assert(ret == HG_SUCCESS);

    ret = HG_Bulk_transfer(hgi->context, rpc_handler_transfer_callback, bt_info, HG_BULK_PUSH,
                            hgi->addr, in.bulk_handle, 0, bt_info->bulk_handle, 0, bt_info->count, HG_OP_ID_IGNORE);
    assert(ret == HG_SUCCESS);
    return HG_SUCCESS;
}

hg_return_t rpc_handler_transfer_callback(const struct hg_cb_info *info)
{
    hg_return_t ret;
    BulkTransferInfo *bt_info = info->arg;

    rpc_transfer_out out;
    HG_Respond(bt_info->handle, NULL, NULL, &out);

    HG_Bulk_free(bt_info->bulk_handle);
    ret = HG_Destroy(bt_info->handle);
    assert(ret == HG_SUCCESS);

    tangram_free(bt_info->buf, bt_info->count);
    tangram_free(bt_info, sizeof(BulkTransferInfo));

    return HG_SUCCESS;
}