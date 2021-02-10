#ifndef __TANGRAM_FS_H__
#define __TANGRAM_FS_H__
#include "tangramfs-interval-tree.h"

typedef struct TFILE_t {
    FILE* local_file;
    IntervalTree *it;
} TFILE;

void tfs_init(const char* persist_dir, const char* buffer_dir);
void tfs_finalize();

TFILE* tfs_open(const char* pathname, const char* mode);
void tfs_write(TFILE* tf, void* buf, size_t count, size_t offset);
void tfs_read(TFILE* tf, void* buf, size_t count, size_t offset);
void tfs_close(TFILE* tf);

#endif
