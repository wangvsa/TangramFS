#ifndef __TANGRAM_FS_H__
#define __TANGRAM_FS_H__
#include "tangramfs-interval-tree.h"
#include "uthash.h"

#define TANGRAM_STRONG_SEMANTICS        1
#define TANGRAM_COMMIT_SEMANTICS        2
#define TANGRAM_SESSION_SEMANTICS       3
#define TANGRAM_CUSTOM_SEMANTICS        4


/**
 * A list of environment variables that can be set
 *
 */
#define TANGRAM_PERSIST_DIR_ENV         "TANGRAM_PERSIST_DIR"
#define TANGRAM_BUFFER_DIR_ENV          "TANGRAM_BUFFER_DIR"
#define TANGRAM_SEMANTICS_ENV           "TANGRAM_SEMANTICS"
#define TANGRAM_UCX_RPC_DEV_ENV         "TANGRAM_RPC_DEV"
#define TANGRAM_UCX_RPC_TL_ENV          "TANGRAM_RPC_TL"
#define TANGRAM_UCX_RMA_DEV_ENV         "TANGRAM_RMA_DEV"
#define TANGRAM_UCX_RMA_TL_ENV          "TANGRAM_RMA_TL"


typedef struct tfs_file {
    char   filename[256]; // file name of the targeting file
    size_t offset;      // offset of the targeting file in this process
    int    local_fd;
    FILE*  local_stream;
    IntervalTree *it;
    UT_hash_handle hh;
} tfs_file_t;

void tfs_init();
void tfs_finalize();

tfs_file_t * tfs_open(const char* pathname, const char* mode);
int tfs_close(tfs_file_t* tf);

size_t tfs_write(tfs_file_t* tf, const void* buf, size_t size);
size_t tfs_read(tfs_file_t* tf, void* buf, size_t size);
size_t tfs_read_lazy(tfs_file_t* tf, void* buf, size_t size);
size_t tfs_seek(tfs_file_t* tf, size_t offset, int whence);

void tfs_post(tfs_file_t* tf, size_t offset, size_t count);
void tfs_post_all(tfs_file_t* tf);
void tfs_query(tfs_file_t* tf, size_t offset, size_t count, int* rank);

/*
 * Used by POSIX wrappers, tell if we should
 * intercept the call according to the file path
 */
bool tangram_should_intercept(const char* filename);
int tangram_get_semantics();

tfs_file_t* tangram_get_tfs_file(const char* filename);

#endif
