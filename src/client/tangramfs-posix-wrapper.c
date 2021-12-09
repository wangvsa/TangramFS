#define _GNU_SOURCE
#include <assert.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdbool.h>
#include <stdarg.h>
#include <fcntl.h>
#include <linux/limits.h>
#include <mpi.h>
#include "uthash.h"
#include "tangramfs.h"
#include "tangramfs-posix-wrapper.h"
#include "tangramfs-semantics-impl.h"

typedef struct TFSStreamMap_t {
    tfs_file_t *tf;
    FILE* stream;  // key
    UT_hash_handle hh;
} TFSStreamMap;

typedef struct TFSPathMat_t {
    tfs_file_t *tf;
    char* pathname;     // key
    UT_hash_handle hh;
} TFSPathMap;

typedef struct TFSFdMap_t {
    tfs_file_t *tf;
    int fd;             // key
    UT_hash_handle hh;
} TFSFdMap;

static TFSStreamMap* tf_stream_map = NULL;
static TFSPathMap* tf_path_map = NULL;     // TODO not realeased after use
static TFSFdMap*   tf_fd_map = NULL;


tfs_file_t* stream2tf(FILE* stream) {
    TFSStreamMap *found = NULL;
    HASH_FIND_PTR(tf_stream_map, &stream, found);
    if(found)
        return found->tf;
    return NULL;
}

tfs_file_t* fd2tf(int fd) {
    TFSFdMap* found = NULL;
    HASH_FIND_INT(tf_fd_map, &fd, found);
    if(found)
        return found->tf;
    return NULL;
}

tfs_file_t* path2tf(const char* path) {
    char *key = realpath(path, NULL);
    TFSPathMap *found = NULL;
    HASH_FIND(hh, tf_path_map, key, strlen(key), found);

    free(key);
    if(found)
        return found->tf;
    return NULL;
}


void* add_to_map(tfs_file_t* tf, const char* filename, int stream) {
    void *res;

    if(stream) {        // FILE* stream
        TFSStreamMap *entry = malloc(sizeof(TFSStreamMap));
        entry->tf = tf;
        entry->stream = tf->local_stream;
        HASH_ADD_PTR(tf_stream_map, stream, entry);
        res = entry;
    } else {            // int fd
        TFSFdMap *entry = malloc(sizeof(TFSFdMap));
        entry->tf = tf;
        entry->fd = tf->local_fd;
        HASH_ADD_INT(tf_fd_map, fd, entry);
        res = entry;
    }

    TFSPathMap *e2 = malloc(sizeof(TFSPathMap));
    e2->pathname = NULL;
    e2->pathname = realpath(filename, NULL);
    if(e2->pathname)
        HASH_ADD_KEYPTR(hh, tf_path_map, e2->pathname, strlen(e2->pathname), e2);

    return res;
}



FILE* TANGRAM_WRAP(fopen)(const char *filename, const char *mode)
{
    if(tangram_should_intercept(filename)) {
        tfs_file_t* tf = tfs_open(filename);
        tangram_debug("[tangramfs] fopen %s %p\n", filename, tf->local_stream);
        if(tf->local_stream != NULL) {
            TFSStreamMap* entry = add_to_map(tf, filename, true);
            return entry->stream;
        }
        return NULL;
    }

    MAP_OR_FAIL(fopen);
    return TANGRAM_REAL_CALL(fopen)(filename, mode);
}

int TANGRAM_WRAP(fseek)(FILE *stream, long int offset, int origin)
{
    tfs_file_t* tf = stream2tf(stream);
    if(tf) {
        tangram_debug("[tangramfs] fseek %s (%lu, %d)\n", tf->filename, offset, origin);
        tfs_seek(tf, offset, origin);
        return 0;   // unlike lseek(), fseek on success, return 0
    }

    MAP_OR_FAIL(fseek);
    return TANGRAM_REAL_CALL(fseek)(stream, offset, origin);
}

void TANGRAM_WRAP(rewind)(FILE *stream)
{
    tfs_file_t* tf = stream2tf(stream);
    if(tf) {
        tangram_debug("[tangramfs] rewind %s\n", tf->filename);
        tfs_seek(tf, 0, SEEK_SET);
    }

    MAP_OR_FAIL(rewind);
    return TANGRAM_REAL_CALL(rewind)(stream);
}

long int TANGRAM_WRAP(ftell)(FILE *stream)
{
    // TODO now returns locally offset
    tfs_file_t* tf = stream2tf(stream);
    if(tf) {
        size_t res = tfs_tell(tf);
        tangram_debug("[tangramfs] ftell %s %lu\n", tf->filename, res);
        return res;
    }

    MAP_OR_FAIL(ftell);
    long int res = TANGRAM_REAL_CALL(ftell)(stream);
    return res;
}

size_t TANGRAM_WRAP(fwrite)(const void *ptr, size_t size, size_t count, FILE * stream)
{
    tfs_file_t *tf = stream2tf(stream);
    if(tf) {
        size_t res = tangram_write_impl(tf, ptr, count*size);
        // Note that fwrite on success returns the count not total bytes.
        res = (res == size*count) ? count: res;
        tangram_debug("[tangramfs] fwrite %s (%lu, %lu), return: %lu\n", tf->filename, size, count, res);
        return res;
    }

    MAP_OR_FAIL(fwrite);
    return TANGRAM_REAL_CALL(fwrite)(ptr, size, count, stream);
}

size_t TANGRAM_WRAP(fread)(void * ptr, size_t size, size_t count, FILE * stream)
{
    tfs_file_t *tf = stream2tf(stream);
    if(tf) {
        size_t res = tangram_read_impl(tf, ptr, count*size);
        res = (res == size*count) ? count: res;
        tangram_debug("[tangramfs] fread %s (%lu, %lu), return: %lu\n", tf->filename, size, count, res);
        return res;
    }

    MAP_OR_FAIL(fread);
    return TANGRAM_REAL_CALL(fread)(ptr, size, count, stream);
}

int TANGRAM_WRAP(fclose)(FILE * stream)
{
    TFSStreamMap *found = NULL;
    HASH_FIND_PTR(tf_stream_map, &stream, found);

    if(found) {
        tangram_debug("[tangramfs] fclose %s\n", found->tf->filename);
        int res = tangram_close_impl(found->tf);
        HASH_DEL(tf_stream_map, found);
        return res;
    }

    MAP_OR_FAIL(fclose);
    return TANGRAM_REAL_CALL(fclose)(stream);
}

int TANGRAM_WRAP(open)(const char *pathname, int flags, ...)
{
    if(tangram_should_intercept(pathname)) {
        tfs_file_t* tf = tfs_open(pathname);
        tangram_debug("[tangramfs] open %s %d\n", pathname, tf->local_fd);
        TFSFdMap* entry = add_to_map(tf, pathname, false);
        return entry->fd;
    }

    MAP_OR_FAIL(open);
    if(flags & O_CREAT) {
        va_list arg;
        va_start(arg, flags);
        int mode = va_arg(arg, int);
        va_end(arg);
        return TANGRAM_REAL_CALL(open)(pathname, flags, mode);
    } else {
        return TANGRAM_REAL_CALL(open)(pathname, flags);
    }
}

int TANGRAM_WRAP(open64)(const char *pathname, int flags, ...)
{
    if(tangram_should_intercept(pathname)) {
        tfs_file_t* tf = tfs_open(pathname);
        TFSFdMap* entry = add_to_map(tf, pathname, false);
        return entry->fd;
    }

    MAP_OR_FAIL(open);
    if(flags & O_CREAT) {
        va_list arg;
        va_start(arg, flags);
        int mode = va_arg(arg, int);
        va_end(arg);
        return TANGRAM_REAL_CALL(open)(pathname, flags, mode);
    } else {
        return TANGRAM_REAL_CALL(open)(pathname, flags);
    }
}

off_t TANGRAM_WRAP(lseek)(int fd, off_t offset, int whence)
{
    tfs_file_t* tf = fd2tf(fd);
    if(tf) {
        tangram_debug("[tangramfs] lseek %s, offset: %lu, whence: %d)\n", tf->filename, offset, whence);
        return tfs_seek(tf, offset, whence);
    }

    MAP_OR_FAIL(lseek);
    return TANGRAM_REAL_CALL(lseek)(fd, offset, whence);
}

off64_t TANGRAM_WRAP(lseek64)(int fd, off64_t offset, int whence)
{
    tfs_file_t* tf = fd2tf(fd);
    if(tf) {
        tangram_debug("[tangramfs] lseek64 %s, offset: %lu, whence: %d)\n", tf->filename, offset, whence);
        return tfs_seek(tf, offset, whence);
    }

    MAP_OR_FAIL(lseek64);
    return TANGRAM_REAL_CALL(lseek64)(fd, offset, whence);
}


ssize_t TANGRAM_WRAP(write)(int fd, const void *buf, size_t count)
{
    tfs_file_t* tf = fd2tf(fd);
    if(tf) {
        tangram_debug("[tangramfs] write start %s (%lu, %lu)\n", tf->filename, tf->offset, count);
        size_t res = tangram_write_impl(tf, buf, count);
        tangram_debug("[tangramfs] write done %s (%lu), return: %lu\n", tf->filename, count, res);
        return res;
    }

    MAP_OR_FAIL(write);
    return TANGRAM_REAL_CALL(write)(fd, buf, count);
}

ssize_t TANGRAM_WRAP(read)(int fd, void *buf, size_t count)
{
    tfs_file_t* tf = fd2tf(fd);
    if(tf) {
        size_t res = tangram_read_impl(tf, buf, count);
        tangram_debug("[tangramfs] read %s (%lu), return: %lu\n", tf->filename, count, res);
        return res;
    }

    MAP_OR_FAIL(read);
    return TANGRAM_REAL_CALL(read)(fd, buf, count);
}

int TANGRAM_WRAP(close)(int fd) {
    TFSFdMap* found = NULL;
    HASH_FIND_INT(tf_fd_map, &fd, found);
    if(found) {
        tangram_debug("[tangramfs] close %s\n", found->tf->filename);
        int res = tangram_close_impl(found->tf);
        HASH_DEL(tf_fd_map, found);
        free(found);
        return res;
    }

    MAP_OR_FAIL(close);
    return TANGRAM_REAL_CALL(close)(fd);
}

ssize_t TANGRAM_WRAP(pwrite)(int fd, const void *buf, size_t count, off_t offset)
{
    tfs_file_t *tf = fd2tf(fd);
    if(tf) {
        tfs_seek(tf, offset, SEEK_SET);
        size_t res = tangram_write_impl(tf, buf, count);
        // Note that fwrite on success returns the count not total bytes.
        tangram_debug("[tangramfs] pwrite %s (%lu, %lu), return: %lu\n", tf->filename, offset, count, res);
        return res;
    }

    MAP_OR_FAIL(pwrite);
    return TANGRAM_REAL_CALL(pwrite)(fd, buf, count, offset);
}

ssize_t TANGRAM_WRAP(pread)(int fd, void *buf, size_t count, off_t offset)
{
    tfs_file_t *tf = fd2tf(fd);
    if(tf) {
        tfs_seek(tf, offset, SEEK_SET);
        size_t res = tangram_read_impl(tf, buf, count);
        tangram_debug("[tangramfs] pread %s (%lu, %lu), return: %lu\n", tf->filename, offset, count, res);
        return res;
    }

    MAP_OR_FAIL(pread);
    return TANGRAM_REAL_CALL(pread)(fd, buf, count, offset);
}

int TANGRAM_WRAP(fsync)(int fd) {
    tfs_file_t* tf = fd2tf(fd);
    if(tf)
        return tangram_commit_impl(tf);

    MAP_OR_FAIL(fsync);
    return TANGRAM_REAL_CALL(fsync)(fd);
}

int TANGRAM_WRAP(__xstat)(int vers, const char *path, struct stat *buf)
{
    // TODO: stat() call not implemented yet.
    if(tangram_should_intercept(path)) {
        tangram_issue_metadata_rpc(AM_ID_STAT_REQUEST, path, buf);
        return 0;
    }

    MAP_OR_FAIL(__xstat);
    return TANGRAM_REAL_CALL(__xstat)(vers, path, buf);
}

int TANGRAM_WRAP(__lxstat)(int vers, const char *path, struct stat *buf)
{
    // TODO: stat() call not implemented yet.
    if(tangram_should_intercept(path)) {
        tangram_issue_metadata_rpc(AM_ID_STAT_REQUEST, path, buf);
        return 0;
    }

    MAP_OR_FAIL(__lxstat);
    return TANGRAM_REAL_CALL(__lxstat)(vers, path, buf);
}

int TANGRAM_WRAP(access)(const char *pathname, int mode) {
    // TODO
    if(tangram_should_intercept(pathname)) {
        tfs_file_t* tf = path2tf(pathname);
        if (tf)
            return 0;
    }
    MAP_OR_FAIL(access);
    return TANGRAM_REAL_CALL(access)(pathname, mode);
}

int TANGRAM_WRAP(unlink)(const char *pathname) {
    // TODO
    if(tangram_should_intercept(pathname)) {
        tfs_file_t* tf = path2tf(pathname);
        if (tf)
            return 0;
    }
    MAP_OR_FAIL(unlink);
    return TANGRAM_REAL_CALL(unlink)(pathname);
}

int TANGRAM_WRAP(mkdir)(const char *pathname, mode_t mode) {
    // TODO
    if(tangram_should_intercept(pathname))
        return 0;
    MAP_OR_FAIL(mkdir);
    return TANGRAM_REAL_CALL(mkdir)(pathname, mode);
}

int TANGRAM_WRAP(rmdir)(const char *pathname) {
    // TODO
    if(tangram_should_intercept(pathname))
        return 0;
    MAP_OR_FAIL(rmdir);
    return TANGRAM_REAL_CALL(rmdir)(pathname);
}


int TANGRAM_WRAP(MPI_Init)(int *argc, char ***argv) {
    int res = PMPI_Init(argc, argv);
    tfs_init();
    return res;
}

int TANGRAM_WRAP(MPI_Init_thread)(int *argc, char ***argv, int required, int *provided) {
    int res = PMPI_Init_thread(argc, argv, required, provided);
    tfs_init();
    return res;
}

int TANGRAM_WRAP(MPI_Finalize)() {
    tfs_finalize();
    return PMPI_Finalize();
}
