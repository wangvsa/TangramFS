#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <mpi.h>
#include <errno.h>
#include "uthash.h"
#include "sessionfs.h"
#include "tangramfs-utils.h"

tfs_file_t* sessionfs_open(const char* pathname) {
    return tfs_open(pathname);
}

void sessionfs_stat(tfs_file_t* tf, struct stat* buf) {
    tfs_stat(tf, buf);
}

void sessionfs_flush(tfs_file_t *tf) {
    tfs_flush(tf);
}

ssize_t sessionfs_write(tfs_file_t* tf, const void* buf, size_t size) {
    return tfs_write(tf, buf, size);
}

ssize_t sessionfs_read(tfs_file_t* tf, void* buf, size_t size) {
    return tfs_read(tf, buf, size);
}

size_t sessionfs_seek(tfs_file_t *tf, size_t offset, int whence) {
    return tfs_seek(tf, offset, whence);
}

size_t sessionfs_tell(tfs_file_t* tf) {
    return tfs_tell(tf);
}

int sessionfs_close(tfs_file_t* tf) {
    return tfs_close(tf);
}

int sessionfs_session_open(tfs_file_t* tf, size_t offset, size_t size) {
    return 0;
}

int sessionfs_session_close(tfs_file_t* tf, size_t offset, size_t size) {
    tfs_post(tf, offset, size);
    return 0;
}
