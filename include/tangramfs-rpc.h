#ifndef _TANGRAMFS_RPC_H_
#define _TANGRAMFS_RPC_H_
#include <stdlib.h>
#include <string.h>
#include "tangramfs-ucx.h"

typedef struct rpc_interval {
    size_t offset;
    size_t count;
    int    type;        // used for lock type
} interval_t;

typedef struct rpc_in {
    int num_intervals;
    int filename_len;
    char* filename;
    interval_t *intervals;
} rpc_in_t;


/**
 * Incase we want to send too many intervals a time,
 * We need to split them into multiple am messages
 *
 * This function calculates how many intervals we can
 * send in one AM
 */
static int rpc_in_intervals_per_am(char* filename, size_t am_max_size) {
    size_t filelen = filename ? strlen(filename) : 0;
    filelen += sizeof(int);

    size_t interval_size = sizeof(size_t)*2+sizeof(int);

    int num_intervals = (am_max_size - filelen - 64/*a safe guard, just in case*/) / interval_size;
    return num_intervals;
}

static void* rpc_in_pack(char* filename, int num_intervals, size_t *offsets, size_t *counts, int* types, size_t *size) {
    if(num_intervals == 0 && filename == NULL) {
        *size = 0;
        return NULL;
    }

    int filename_len = strlen(filename);

    size_t total = sizeof(int)*2;           // filename_len, num_intervals
    total += strlen(filename);              // filename
    for(int i = 0; i < num_intervals; i++)  // intervals (offset, count, type)
        total += (sizeof(size_t) * 2 + sizeof(int));

    int pos = 0;
    void* data = malloc(total);
    memset(data, 0, total);
    memcpy(data+pos, &num_intervals, sizeof(int));
    pos+= sizeof(int);
    memcpy(data+pos, &filename_len, sizeof(int));
    pos+=sizeof(int);
    memcpy(data+pos, filename, filename_len);
    pos+=filename_len;
    for(int i = 0; i < num_intervals; i++) {
        memcpy(data+pos, &offsets[i], sizeof(size_t));
        pos += sizeof(size_t);
        memcpy(data+pos, &counts[i], sizeof(size_t));
        pos += sizeof(size_t);
        if(types != NULL)
            memcpy(data+pos, &types[i], sizeof(int));
        pos += sizeof(int);
    }

    *size = total;
    return data;
}

static rpc_in_t* rpc_in_unpack(void* data) {
    size_t pos = 0;
    rpc_in_t *in = malloc(sizeof(rpc_in_t));

    memcpy(&in->num_intervals, data+pos, sizeof(int));
    pos += sizeof(int);

    memcpy(&in->filename_len, data+pos, sizeof(int));
    pos += sizeof(int);

    in->filename = malloc(in->filename_len + 1);
    memset(in->filename, 0, in->filename_len+1);
    memcpy(in->filename, data+pos, in->filename_len);
    pos += in->filename_len;

    in->intervals = malloc(sizeof(interval_t) * in->num_intervals);
    for(int i = 0; i < in->num_intervals; i++) {
        memcpy(&(in->intervals[i].offset), data+pos, sizeof(size_t));
        pos += sizeof(size_t);
        memcpy(&(in->intervals[i].count), data+pos, sizeof(size_t));
        pos += sizeof(size_t);
        memcpy(&(in->intervals[i].type), data+pos, sizeof(int));
        pos += sizeof(int);
    }
    return in;
}

static void rpc_in_free(rpc_in_t *in) {
    free(in->filename);
    free(in->intervals);
    free(in);
}

void tangram_issue_rpc(uint8_t id, char* filename, size_t* offsets, size_t* counts, int* types, int len, void** respond_ptr);
void tangram_issue_rma(uint8_t id, char* filename, tangram_uct_addr_t* dest, size_t *offsets, size_t *counts, int len, void* recv_buf);
void tangram_issue_metadata_rpc(uint8_t id, const char* filename, void** respond_ptr);
void tangram_rma_service_start(tfs_info_t *tfs_info, void* (*serve_rma_data_cb)(void*, size_t*));
void tangram_rma_service_stop();
void tangram_rpc_service_start(tfs_info_t* tfs_info, void (*revoke_lock_cb)(void*));
void tangram_rpc_service_stop(tfs_info_t* tfs_info);
tangram_uct_addr_t* tangram_rpc_get_client_addr();

#endif
