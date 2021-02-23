#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <mercury.h>
#include "tangramfs-meta.h"

static hg_class_t*     hg_class   = NULL; /* Pointer to the Mercury class */
static hg_context_t*   hg_context = NULL; /* Pointer to the Mercury context */
static hg_addr_t       hg_addr = NULL;    /* addr retrived from the addr lookup callback */


static hg_id_t         hello_rpc_id;      /* ID of the RPC */
static int completed = 0;                 /* Variable indicating if the call has completed */

pthread_t client_thread;


/*
 * This callback will be called after looking up for the server's address.
 * This is the function that will also send the RPC to the servers, then
 * set the completed variable to 1.
 */
hg_return_t lookup_callback(const struct hg_cb_info *callback_info);

void mercury_client_init();
void mercury_client_finalize();

void mercury_register_rpcs() {
    /* Register a RPC function.
     * The first two NULL correspond to what would be pointers to
     * serialization/deserialization functions for input and output datatypes
     * (not used in this example).
     * The third NULL is the pointer to the function (which is on the server,
     * so NULL here on the client).
     */
    hello_rpc_id = HG_Register_name(hg_class, "hello", NULL, NULL, NULL);

    /* Indicate Mercury that we shouldn't expect a response from the server
     * when calling this RPC.
     */
    HG_Registered_disable_response(hg_class, hello_rpc_id, HG_TRUE);
}

void mercury_client_init() {
    hg_class = HG_Init(MERCURY_PROTOCOL, HG_FALSE);
    assert(hg_class != NULL);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);
}

void mercury_client_finalize() {
    hg_return_t ret;
    ret = HG_Context_destroy(hg_context);
    assert(ret == HG_SUCCESS);

    if(hg_addr)
        HG_Addr_free(hg_class, hg_addr);

    ret = HG_Finalize(hg_class);
    assert(ret == HG_SUCCESS);
}

void* mercury_client_progress_loop(void* arg) {
    hg_return_t ret;
    while(!completed)
    {
        unsigned int count;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count && !completed);
        HG_Progress(hg_context, 100);
    }
}

void tfs_meta_client_start()
{
    mercury_client_init();

    mercury_register_rpcs();

    HG_Addr_lookup(hg_context, lookup_callback, NULL, MERCURY_SERVER_ADDR, HG_OP_ID_IGNORE);

    pthread_create(&client_thread, NULL, mercury_client_progress_loop, NULL);
    //mercury_client_progress_loop(NULL);
}

void tfs_meta_client_stop() {
    tfs_meta_issue_rpc();

    completed = 1;
    pthread_join(client_thread, NULL);
    mercury_client_finalize();
}

void tfs_meta_issue_rpc() {
    while(hg_addr == NULL) {
        sleep(1);
    }

    hg_return_t ret;
    hg_handle_t handle;

    ret = HG_Create(hg_context, hg_addr, hello_rpc_id, &handle);
    assert(ret == HG_SUCCESS);

    /* Send the RPC. The first NULL correspond to the callback
     * function to call when receiving the response from the server
     * (we don't expect a response, hence NULL here).
     * The second NULL is a pointer to user-specified data that will
     * be passed to the response callback.
     * The third NULL is a pointer to the RPC's argument (we don't
     * use any here).
     */
    ret = HG_Forward(handle, NULL, NULL, NULL);
    assert(ret == HG_SUCCESS);

    completed = 1;

    ret = HG_Destroy(handle);
    assert(ret == HG_SUCCESS);
}


/*
 * This function is called when the address lookup operation has completed.
 */
hg_return_t lookup_callback(const struct hg_cb_info *callback_info)
{
    assert(callback_info->ret == 0);
    hg_addr = callback_info->info.lookup.addr;
    return HG_SUCCESS;
}
