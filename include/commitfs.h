#ifndef __COMMIT_FS_H__
#define __COMMIT_FS_H__
#include <sys/stat.h>
#include "uthash.h"
#include "tangramfs.h"
#include "tangramfs-rpc.h"

/** Implement commit consistency using TangramFS primitives **/

tfs_file_t* commitfs_open(const char* pathname);
int     commitfs_close(tfs_file_t* tf);
ssize_t commitfs_write(tfs_file_t* tf, const void* buf, size_t size);
ssize_t commitfs_read(tfs_file_t* tf, void* buf, size_t size);
ssize_t commitfs_read_lazy(tfs_file_t* tf, void* buf, size_t size);
size_t  commitfs_seek(tfs_file_t* tf, size_t offset, int whence);
size_t  commitfs_tell(tfs_file_t* tf);
void    commitfs_stat(tfs_file_t* tf, struct stat* buf);
void    commitfs_flush(tfs_file_t* tf);
int     commitfs_commit(tfs_file_t* tf, size_t offset, size_t count);

#endif
