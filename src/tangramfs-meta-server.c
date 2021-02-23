#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <mercury.h>
#include "mpi.h"
#include "tangramfs-meta.h"

static hg_class_t*     hg_class   = NULL; /* the mercury class */
static hg_context_t*   hg_context = NULL; /* the mercury context */
pthread_t server_thread;

static int stop_server = 0;


hg_return_t hello_world(hg_handle_t h);

void mercury_server_init();
void* mercury_server_progress_loop(void* arg);


void tfs_meta_server_start() {
    mercury_server_init();
    pthread_create(&server_thread, NULL, mercury_server_progress_loop, NULL);
}

void tfs_meta_server_stop() {
    stop_server = 1;
    pthread_join(server_thread, NULL);
    HG_Context_destroy(hg_context);
    HG_Finalize(hg_class);
}

void mercury_server_init() {
    hg_return_t ret;

    hg_class = HG_Init(MERCURY_PROTOCOL, HG_TRUE);
    assert(hg_class != NULL);

    char hostname[128] = {0};
    hg_size_t hostname_size;
    hg_addr_t self_addr;
    HG_Addr_self(hg_class, &self_addr);
    HG_Addr_to_string(hg_class, hostname, &hostname_size, self_addr);
    printf("Server running at address %s\n", hostname);
    HG_Addr_free(hg_class, self_addr);

    hg_context = HG_Context_create(hg_class);
    assert(hg_context != NULL);

    /* Register the RPC by its name ("hello").
     * The two NULL arguments correspond to the functions user to
     * serialize/deserialize the input and output parameters
     * (hello_world doesn't have parameters and doesn't return anything, hence NULL).
     */
    hg_id_t rpc_id = HG_Register_name(hg_class, "hello", NULL, NULL, hello_world);

    // Tell Mercury that hello_world will not send any response back to the client.
    HG_Registered_disable_response(hg_class, rpc_id, HG_TRUE);
}

void* mercury_server_progress_loop(void* arg) {
    hg_return_t ret;
    do {
        unsigned int count;
        do {
            ret = HG_Trigger(hg_context, 0, 1, &count);
        } while((ret == HG_SUCCESS) && count);
        HG_Progress(hg_context, 100);
    } while(!stop_server);
}

hg_return_t hello_world(hg_handle_t h)
{
    hg_return_t ret;

    printf("Server received RPC call: Hello World!\n");

    /* We are not going to use the handle anymore, so we should destroy it. */
    ret = HG_Destroy(h);
    assert(ret == HG_SUCCESS);
    return HG_SUCCESS;
}
