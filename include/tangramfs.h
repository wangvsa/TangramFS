#ifndef __TANGRAM_FS_H__
#define __TANGRAM_FS_H__
#include <stdbool.h>
#include <sys/stat.h>
#include "seg_tree.h"
#include "uthash.h"
#include "tangramfs-rpc.h"

#define TANGRAM_STRONG_SEMANTICS        1
#define TANGRAM_COMMIT_SEMANTICS        2
#define TANGRAM_SESSION_SEMANTICS       3
#define TANGRAM_CUSTOM_SEMANTICS        4

#define TANGRAM_LOCK_ALGO_EXACT         1
#define TANGRAM_LOCK_ALGO_EXTEND        2

/**
 * A list of environment variables can be set
 */
#define TANGRAM_PERSIST_DIR_ENV         "TANGRAM_PERSIST_DIR"
#define TANGRAM_BUFFER_DIR_ENV          "TANGRAM_BUFFER_DIR"
#define TANGRAM_SEMANTICS_ENV           "TANGRAM_SEMANTICS"
#define TANGRAM_UCX_RPC_DEV_ENV         "TANGRAM_RPC_DEV"
#define TANGRAM_UCX_RPC_TL_ENV          "TANGRAM_RPC_TL"
#define TANGRAM_UCX_RMA_DEV_ENV         "TANGRAM_RMA_DEV"
#define TANGRAM_UCX_RMA_TL_ENV          "TANGRAM_RMA_TL"
// optional
#define TANGRAM_DEBUG_ENV               "TANGRAM_DEBUG"
#define TANGRAM_USE_DELEGATOR_ENV       "TANGRAM_USE_DELEGATOR"
#define TANGRAM_LOCK_ALGO_ENV           "TANGRAM_LOCK_ALGO"


typedef struct tfs_file {

    char   filename[256];           // File name of the targeting file on PFS
    int    fd;                      // File descriptor of the targeting file on PFS
    FILE*  stream;                  // File stream of the targeting file on PFS

    size_t offset;                  // Offset of the targeting file in this process

    int    local_fd;                // File descriptor of the local buffer file

    struct seg_tree seg_tree;

    UT_hash_handle hh;              // filename as key

} tfs_file_t;

void tfs_init();
void tfs_finalize();

tfs_file_t* tfs_open(const char* pathname);
int tfs_close(tfs_file_t* tf);

ssize_t tfs_write(tfs_file_t* tf, const void* buf, size_t size);
ssize_t tfs_read(tfs_file_t* tf, void* buf, size_t size);
ssize_t tfs_read_peer(tfs_file_t* tf, void* buf, size_t size, tangram_uct_addr_t* owner);
ssize_t tfs_read_local(tfs_file_t* tf, void* buf, size_t size);
size_t  tfs_seek(tfs_file_t* tf, size_t offset, int whence);
size_t  tfs_tell(tfs_file_t* tf);
void    tfs_stat(tfs_file_t* tf, struct stat* buf);
void    tfs_flush(tfs_file_t* tf);

void    tfs_post(tfs_file_t* tf, size_t offset, size_t count);
void    tfs_post_file(tfs_file_t* tf);
void    tfs_unpost_file(tfs_file_t* tf);
void    tfs_unpost_client();
int     tfs_query(tfs_file_t* tf, size_t offset, size_t count, tangram_uct_addr_t** owner);
int     tfs_query_many(tfs_file_t* tf, size_t* offsets, size_t* sizes, int num, tangram_uct_addr_t** owners);


// Lock based API
// Only used for consistency-centric programming model)
// to test the performance penalty
int tfs_acquire_lock(tfs_file_t* tf, size_t offset, size_t count, int type);
int tfs_release_lock(tfs_file_t* tf, size_t offset, size_t count);
int tfs_release_lock_file(tfs_file_t* tf);
int tfs_release_lock_client();


// Python API
size_t tfs_fetch(const char* filename, void* buf, size_t size);
size_t tfs_fetch_pfs(const char* filename, void* buf, size_t size);


/*
 * Used by POSIX wrappers, tell if we should
 * intercept the call according to the file path
 */
bool tangram_should_intercept(const char* filename);
int tangram_get_semantics();

#endif
