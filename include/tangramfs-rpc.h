#ifndef _TANGRAMFS_RPC_H_
#define _TANGRAMFS_RPC_H_

#define MERCURY_PROTOCOL    "ofi+tcp"

#define RPC_NAME_POST       "tfs_rpc_rpc_post"
#define RPC_NAME_QUERY      "tfs_rpc_rpc_query"
#define RPC_NAME_TRANSFER   "tfs_rpc_rpc_transfer"

#include <mercury.h>
#include <mercury_proc_string.h>
#include <mercury_proc_bulk.h>



typedef struct rpc_post_in_t {
    char* filename;
    int32_t rank;
    uint32_t offset;
    uint32_t count;
    struct rpc_post_in_t* next;
} *rpc_post_in;

static inline hg_return_t hg_proc_rpc_post_in(hg_proc_t proc, void* data)
{
    hg_return_t ret;
    rpc_post_in *list = (rpc_post_in*)data;

    hg_size_t length = 0;
    rpc_post_in tmp = NULL;
    rpc_post_in prev = NULL;

    switch(hg_proc_get_op(proc)) {
        case HG_ENCODE:
            tmp = *list;
            while(tmp != NULL) {
                tmp = tmp->next;
                length += 1;
            }
            // write the length
            hg_proc_hg_size_t(proc, &length);

            // write the list
            tmp = *list;
            hg_proc_hg_const_string_t(proc, &tmp->filename);
            hg_proc_int32_t(proc, &tmp->rank);
            while(tmp != NULL) {
                hg_proc_uint32_t(proc, &tmp->offset);
                hg_proc_uint32_t(proc, &tmp->count);
                tmp = tmp->next;
            }
            break;

        case HG_DECODE:
            // find out the length of the list
            hg_proc_hg_size_t(proc, &length);

            // loop and create list elements
            *list = NULL;
            while(length > 0) {
                tmp = (rpc_post_in)malloc(sizeof(*tmp));
                tmp->next = NULL;
                if(*list == NULL) {
                    *list = tmp;
                    hg_proc_hg_const_string_t(proc, &tmp->filename);
                    hg_proc_int32_t(proc, &tmp->rank);
                }
                if(prev != NULL)
                    prev->next = tmp;
                hg_proc_uint32_t(proc, &tmp->offset);
                hg_proc_uint32_t(proc, &tmp->count);
                prev = tmp;
                length -= 1;
            }
            break;

        case HG_FREE:
            tmp = *list;
            while(tmp != NULL) {
                prev = tmp;
                tmp  = prev->next;
                free(prev);
            }
            break;
    }

    ret = HG_SUCCESS;
    return ret;
}


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

typedef struct rpc_transfer_in_thhh {
    char* filename;
    int32_t rank;
    uint32_t offset;
    uint32_t count;
    hg_bulk_t bulk_handle;
} rpc_transfer_in;

static HG_INLINE hg_return_t
hg_proc_rpc_transfer_in(hg_proc_t proc, void* data)
{
    rpc_transfer_in *arg = (rpc_transfer_in*) data;
    hg_proc_hg_const_string_t(proc, &arg->filename);
    hg_proc_int32_t(proc, &arg->rank);
    hg_proc_uint32_t(proc, &arg->offset);
    hg_proc_uint32_t(proc, &arg->count);
    hg_proc_hg_bulk_t(proc, &arg->bulk_handle); // TODO a matching function for hg_bulk_t?
    return HG_SUCCESS;
}

typedef struct rpc_transfer_out_t {
    int32_t rank;
} rpc_transfer_out;


/* hg_proc_[structure name] is a special name */
static hg_return_t
hg_proc_rpc_transfer_out(hg_proc_t proc, void* data) {
    rpc_transfer_out *arg = (rpc_transfer_out*) data;
    hg_proc_int32_t(proc, &arg->rank);
    return HG_SUCCESS;
}

typedef struct rpc_query_out_t {
    int32_t rank;
} rpc_query_out;


/* hg_proc_[structure name] is a special name */
static hg_return_t
hg_proc_rpc_query_out(hg_proc_t proc, void* data) {
    rpc_query_out *arg = (rpc_query_out*) data;
    hg_proc_int32_t(proc, &arg->rank);
    return HG_SUCCESS;
}


void tangram_rpc_server_start(char* server_addr);
void tangram_rpc_server_stop();

void tangram_rpc_client_start(const char* server_addr);
void tangram_rpc_client_stop();
void tangram_rpc_issue_rpc(const char* rpc_name, char* filename, int rank, size_t *offsets, size_t *counts, int len);
rpc_query_out tangram_rpc_query_result();


void tangram_rpc_onetime_start(const char* server_addr);
void tangram_rpc_onetime_stop();
void tangram_rpc_onetime_transfer(char* filename, int rank, size_t offset, size_t count, void* local_buf);


#endif
