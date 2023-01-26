#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <mpi.h>
#include <errno.h>
#include "uthash.h"
#include "commitfs.h"
#include "tangramfs-utils.h"

tfs_file_t* commitfs_open(const char* pathname) {
    return tfs_open(pathname);
}

void commitfs_stat(tfs_file_t* tf, struct stat* buf) {
    tfs_stat(tf, buf);
}

void commitfs_flush(tfs_file_t *tf) {
    tfs_flush(tf);
}

ssize_t commitfs_write(tfs_file_t* tf, const void* buf, size_t size) {
    return tfs_write(tf, buf, size);
}

ssize_t commitfs_read(tfs_file_t* tf, void* buf, size_t size) {
    return tfs_read(tf, buf, size);
}

size_t commitfs_seek(tfs_file_t *tf, size_t offset, int whence) {
    return tfs_seek(tf, offset, whence);
}

size_t commitfs_tell(tfs_file_t* tf) {
    return tfs_tell(tf);
}

int commitfs_close(tfs_file_t* tf) {
    return tfs_close(tf);
}

int commitfs_commit_range(tfs_file_t* tf, size_t offset, size_t count) {
    tfs_post(tf, offset, count);
    return 0;
}

int commitfs_commit_file(tfs_file_t* tf) {
    tfs_post_file(tf);
    return 0;
}
