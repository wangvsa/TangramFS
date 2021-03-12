#ifndef __TANGRAM_FS_H__
#define __TANGRAM_FS_H__
#include "tangramfs-interval-tree.h"

typedef struct TFS_File_t {
    char filename[256]; // file name of the targeting file
    size_t offset;      // offset of the targeting file in this process

    int local_fd;
    IntervalTree *it;
} TFS_File;

void tfs_init(const char* persist_dir, const char* buffer_dir);
void tfs_finalize();

TFS_File* tfs_open(const char* pathname, const char* mode);
int tfs_close(TFS_File* tf);

size_t tfs_write(TFS_File* tf, const void* buf, size_t offset, size_t size);
void tfs_notify(TFS_File* tf, size_t offset, size_t count);
void tfs_query(TFS_File* tf, size_t offset, size_t count);

size_t tfs_read(TFS_File* tf, void* buf, size_t offset, size_t size);
size_t tfs_read_lazy(TFS_File* tf, void* buf, size_t offset, size_t size);


/*
 * Used by POSIX wrappers, tell if we should 
 * intercept the call according to the file path
 */
bool tangram_should_intercept(const char* filename);

#endif
