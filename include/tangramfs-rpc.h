#ifndef _TANGRAMFS_RPC_H_
#define _TANGRAMFS_RPC_H_

#define MERCURY_PROTOCOL    "ofi+tcp"

#define RPC_NAME_NOTIFY     "tfs_rpc_rpc_notify"
#define RPC_NAME_QUERY      "tfs_rpc_rpc_query"


#include <mercury.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>

typedef struct rpc_query_in_t {
    char* filename;
    int32_t rank;
    uint32_t offset;
    uint32_t count;
} rpc_query_in;


/* hg_proc_[structure name] is a special name */
static hg_return_t
hg_proc_rpc_query_in(hg_proc_t proc, void* data) {
    rpc_query_in *arg = (rpc_query_in*) data;
    hg_proc_hg_const_string_t(proc, &arg->filename);
    hg_proc_int32_t(proc, &arg->rank);
    hg_proc_uint32_t(proc, &arg->offset);
    hg_proc_uint32_t(proc, &arg->count);
    return HG_SUCCESS;
}


void tangram_rpc_server_start(char* server_addr);
void tangram_rpc_server_stop();

void tangram_rpc_client_start(const char* server_addr);
void tangram_rpc_client_stop();
void tangram_rpc_issue_rpc(const char* rpc_name, char* filename, int rank, size_t offset, size_t count);



#endif
