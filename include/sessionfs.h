#ifndef __SESSION_FS_H__
#define __SESSION_FS_H__
#include <sys/stat.h>
#include "tangramfs.h"
#include "tangramfs-rpc.h"

/** Implement session consistency using TangramFS primitives **/

tfs_file_t* sessionfs_open(const char* pathname);
int     sessionfs_close(tfs_file_t* tf);
ssize_t sessionfs_write(tfs_file_t* tf, const void* buf, size_t size);
ssize_t sessionfs_read(tfs_file_t* tf, void* buf, size_t size);
ssize_t sessionfs_read_lazy(tfs_file_t* tf, void* buf, size_t size);
size_t  sessionfs_seek(tfs_file_t* tf, size_t offset, int whence);
size_t  sessionfs_tell(tfs_file_t* tf);
void    sessionfs_stat(tfs_file_t* tf, struct stat* buf);
void    sessionfs_flush(tfs_file_t* tf);
int     sessionfs_session_open(tfs_file_t* tf, size_t* offsets, size_t* sizes, int num);
int     sessionfs_session_close(tfs_file_t* tf);

#endif
