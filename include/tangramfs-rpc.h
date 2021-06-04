#ifndef _TANGRAMFS_RPC_H_
#define _TANGRAMFS_RPC_H_


#define RPC_OP_POST       0
#define RPC_OP_QUERY      1
#define RPC_OP_STOP       2
#define RPC_OP_TRANSFER   3


typedef struct rpc_post_in {
    char* filename;
    int32_t rank;
    uint64_t offset;
    uint64_t count;
} rpc_post_in_t;

typedef struct rpc_query_in {
    char* filename;
    int32_t rank;
    uint64_t offset;
    uint64_t count;
} rpc_query_in_t;

typedef struct rpc_query_out {
    int32_t rank;
} rpc_query_out_t;

typedef struct rpc_transfer_in {
    char* filename;
    int32_t rank;
    uint64_t offset;
    uint64_t count;
} rpc_transfer_in_t;

typedef struct rpc_transfer_out_t {
    int32_t rank;
} rpc_transfer_out_t;



void tangram_rpc_issue_rpc(int op, char* filename, int rank, size_t *offsets, size_t *counts, int len, void** respond);

#endif
