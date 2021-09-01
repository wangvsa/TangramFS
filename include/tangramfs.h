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


typedef struct TFS_File_t {
    char filename[256]; // file name of the targeting file
    size_t offset;      // offset of the targeting file in this process

    int local_fd;
    FILE* local_stream;
    IntervalTree *it;

    UT_hash_handle hh;
} TFS_File;

void tfs_init();
void tfs_finalize();

TFS_File* tfs_open(const char* pathname, const char* mode);
int tfs_close(TFS_File* tf);

size_t tfs_write(TFS_File* tf, const void* buf, size_t size);
size_t tfs_read(TFS_File* tf, void* buf, size_t size);
size_t tfs_read_lazy(TFS_File* tf, void* buf, size_t size);
size_t tfs_seek(TFS_File* tf, size_t offset, int whence);

void tfs_post(TFS_File* tf, size_t offset, size_t count);
void tfs_post_all(TFS_File* tf);
void tfs_query(TFS_File* tf, size_t offset, size_t count, int* rank);

/*
 * Used by POSIX wrappers, tell if we should
 * intercept the call according to the file path
 */
bool tangram_should_intercept(const char* filename);
int tangram_get_semantics();

TFS_File* tangram_get_tfs_file(const char* filename);

#endif
