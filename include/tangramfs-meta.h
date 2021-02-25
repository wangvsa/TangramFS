#ifndef _TANGRAMFS_META
#define _TANGRAMFS_META

#define MERCURY_PROTOCOL    "mpi+static"
#define MERCURY_SERVER_ADDR "rank#0"

#define RPC_NAME_NOTIFY     "tfs_meta_rpc_notify"
#define RPC_NAME_QUERY      "tfs_meta_rpc_query"


#include <mercury.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>

typedef struct rpc_query_in_t {
    char* filename;
    int32_t rank;
    uint32_t offset;
    uint32_t count;
} rpc_query_in;


static hg_return_t
hg_proc_rpc_query_in(hg_proc_t proc, void* data) {
    rpc_query_in *arg = (rpc_query_in*) data;
    hg_proc_hg_const_string_t(proc, &arg->filename);
    hg_proc_int32_t(proc, &arg->rank);
    hg_proc_uint32_t(proc, &arg->offset);
    hg_proc_uint32_t(proc, &arg->count);
    return HG_SUCCESS;
}


void tangram_meta_server_start();
void tangram_meta_server_stop();

void tangram_meta_client_start();
void tangram_meta_client_stop();
void tangram_meta_issue_rpc(const char* rpc_name, const char* filename, int rank, size_t offset, size_t count);



#endif
