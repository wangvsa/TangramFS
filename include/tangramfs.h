#ifndef __TANGRAM_FS_H__
#define __TANGRAM_FS_H__
#include "tangramfs-interval-tree.h"

int g_mpi_size;
int g_mpi_rank;

void tfs_init();
void tfs_finalize();

typedef struct TFILE_t {
    FILE* file;
    IntervalTree *it;
} TFILE;


TFILE* tfs_open(const char* pathname, const char* mode);
void tfs_write(TFILE* tf, void* buf, size_t count, size_t offset);
void tfs_read(TFILE* tf, void* buf, size_t count, size_t offset);
void tfs_close(TFILE* tf);

void tfs_commit();
void tfs_persist();

#endif
