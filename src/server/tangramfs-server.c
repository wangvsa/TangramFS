#define _POSIX_C_SOURCE 200112L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "tangramfs.h"
#include "tangramfs-metadata.h"
#include "tangramfs-ucx.h"
#include "tangramfs-rpc.h"
#include "tangramfs-utils.h"

/**
 * Return a respond, can be NULL
 */
void* rpc_handler(int op, void* data, size_t length, size_t *respond_len) {
    *respond_len = 0;
    void *respond = NULL;

    if(op == OP_RPC_POST) {
        rpc_in_t* in = rpc_in_unpack(data);
        for(int i = 0; i < in->num_intervals; i++)
            tangram_ms_handle_post(in->rank, in->filename, in->intervals[i].offset, in->intervals[i].count);
        printf("post in->rank: %d, filename: %s, offset:%lu, count: %lu\n", in->rank, in->filename, in->intervals[0].offset/1024/1024, in->intervals[0].count/1024/1024);
        rpc_in_free(in);
        respond = malloc(sizeof(int));
        *respond_len = sizeof(int);
        return respond;
    } else if(op == OP_RPC_QUERY) {
        rpc_in_t* in = rpc_in_unpack(data);
        *respond_len = sizeof(rpc_out_t);
        rpc_out_t *out = malloc(sizeof(rpc_out_t));
        bool found = tangram_ms_handle_query(in->filename, in->intervals[0].offset, in->intervals[0].count, &(out->rank));
        printf("query in->rank: %d, filename: %s, offset:%lu, count: %lu, out->rank: %d\n",
                in->rank, in->filename, in->intervals[0].offset/1024/1024, in->intervals[0].count/1024/1024, out->rank);
        assert(found);
        rpc_in_free(in);
        respond = out;
    }

    return respond;
}


int main(int argc, char* argv[]) {
    assert(argc == 2);

    if( strcmp(argv[1], "start") == 0 ) {

        char* iface = NULL;
        iface  = getenv("UCX_NET_DEVICES");
        if(iface == NULL) {
            printf("[TangramFS] Please use UCX_NET_DEVICES environment variable to specify network interace.\n");
            return 0;
        }

        char ip_addr[1025] = {0};
        tangram_ucx_server_init(iface, ip_addr);
        printf("Server IP address: %s\n", ip_addr);

        tangram_write_server_addr("./", iface, ip_addr);
        tangram_ucx_server_register_rpc(rpc_handler);
        tangram_ucx_server_start();
    } else if( strcmp(argv[1], "stop") == 0 ) {
        char iface[64] = {0};
        char ip_addr[1025] = {0};
        tangram_read_server_addr("./", iface, ip_addr);
        tangram_ucx_set_iface_addr(iface, ip_addr);
        tangram_ucx_stop_server();
    }

    return 0;
}

