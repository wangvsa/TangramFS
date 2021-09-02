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
#include "tangramfs-rpc.h"
#include "tangramfs-posix-wrapper.h"
#include "tangramfs-semantics-impl.h"

typedef struct TFSStreamMap_t {
    TFS_File *tf;
    FILE* stream;  // key
    UT_hash_handle hh;
} TFSStreamMap;

typedef struct TFSPathMat_t {
    TFS_File *tf;
    char* pathname;     // key
    UT_hash_handle hh;
} TFSPathMap;

typedef struct TFSFdMap_t {
    TFS_File *tf;
    int fd;             // key
    UT_hash_handle hh;
} TFSFdMap;

static TFSStreamMap* tf_stream_map = NULL;
static TFSPathMap* tf_path_map = NULL;     // TODO not realeased after use
static TFSFdMap*   tf_fd_map = NULL;


TFS_File* stream2tf(FILE* stream) {
    TFSStreamMap *found = NULL;
    HASH_FIND_PTR(tf_stream_map, &stream, found);
    if(found)
        return found->tf;
    return NULL;
}

TFS_File* fd2tf(int fd) {
    TFSFdMap* found = NULL;
    HASH_FIND_INT(tf_fd_map, &fd, found);
    if(found)
        found->tf;
    return NULL;
}

TFS_File* path2tf(const char* path) {
    char *key = realpath(path, NULL);
    TFSPathMap *found = NULL;
    HASH_FIND(hh, tf_path_map, key, strlen(key), found);

    free(key);
    if(found)
        found->tf;
    return NULL;
}


void* add_to_map(TFS_File* tf, const char* filename, int stream) {
    void *res;

    if(stream) {        // FILE* stream
        TFSStreamMap *entry = malloc(sizeof(TFSStreamMap));
        entry->tf = tf;
        entry->stream = tf->local_stream;
        HASH_ADD_PTR(tf_stream_map, stream, entry);
        printf("add to map %s %p\n", filename, entry->stream);
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
    printf("fopen: %s, mode: %s\n", filename, mode);
    if(tangram_should_intercept(filename)) {

        TFS_File* tf = tfs_open(filename, mode);

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
    TFS_File* tf = stream2tf(stream);
    if(tf) {
        tfs_seek(tf, offset, origin);
        return 0;   // unlike lseek(), fseek on success, return 0
    }

    MAP_OR_FAIL(fseek);
    return TANGRAM_REAL_CALL(fseek)(stream, offset, origin);
}

long int TANGRAM_WRAP(ftell)(FILE *stream)
{
    // TODO
    MAP_OR_FAIL(ftell);
    long int res = TANGRAM_REAL_CALL(ftell)(stream);
    return res;
}

size_t TANGRAM_WRAP(fwrite)(const void *ptr, size_t size, size_t count, FILE * stream)
{
    printf("fwrite out, %lu %lu %p\n", size, count, stream);
    TFS_File *tf = stream2tf(stream);
    if(tf) {
        size_t res = tangram_write_impl(tf, ptr, count*size);
        printf("fwrite %s, %lu %lu\n", tf->filename, size, count);
        // Note that fwrite on success returns the count not total bytes.
        if(res == size * count)
            return count;
        return res;
    }

    MAP_OR_FAIL(fwrite);
    return TANGRAM_REAL_CALL(fwrite)(ptr, size, count, stream);
}

size_t TANGRAM_WRAP(fread)(void * ptr, size_t size, size_t count, FILE * stream)
{
    TFS_File *tf = stream2tf(stream);
    if(tf) {
        printf("fread %s, %lu %lu\n", tf->filename, size, count);
        size_t res = tangram_read_impl(tf, ptr, count*size);
        if(res == size * count)
            return count;
        return size;
    }

    MAP_OR_FAIL(fread);
    return TANGRAM_REAL_CALL(fread)(ptr, size, count, stream);
}

int TANGRAM_WRAP(fclose)(FILE * stream)
{
    TFSStreamMap *found = NULL;
    HASH_FIND(hh, tf_stream_map, stream, sizeof(FILE), found);
    if(found) {
        int res = tangram_close_impl(found->tf);
        HASH_DEL(tf_stream_map, found);
        free(found);
        return res;
    }

    MAP_OR_FAIL(fclose);
    return TANGRAM_REAL_CALL(fclose)(stream);
}

int TANGRAM_WRAP(open)(const char *pathname, int flags, ...)
{
    if(tangram_should_intercept(pathname)) {
        TFS_File* tf = tfs_open(pathname, "w+");
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
        TFS_File* tf = tfs_open(pathname, "w+");
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
    TFS_File* tf = fd2tf(fd);
    if(tf)
        return tfs_seek(tf, offset, whence);

    MAP_OR_FAIL(lseek);
    return TANGRAM_REAL_CALL(lseek)(fd, offset, whence);
}

ssize_t TANGRAM_WRAP(write)(int fd, const void *buf, size_t count)
{
    TFS_File* tf = fd2tf(fd);
    if(tf)
        return tangram_write_impl(tf, buf, count);

    MAP_OR_FAIL(write);
    return TANGRAM_REAL_CALL(write)(fd, buf, count);
}

ssize_t TANGRAM_WRAP(read)(int fd, void *buf, size_t count)
{
    TFS_File* tf = fd2tf(fd);
    if(tf) {
        return tangram_read_impl(tf, buf, count);
    }

    MAP_OR_FAIL(read);
    return TANGRAM_REAL_CALL(read)(fd, buf, count);
}

int TANGRAM_WRAP(close)(int fd) {
    TFSFdMap* found = NULL;
    HASH_FIND_INT(tf_fd_map, &fd, found);
    if(found) {
        int res = tangram_close_impl(found->tf);
        HASH_DEL(tf_fd_map, found);
        free(found);
        return res;
    }

    MAP_OR_FAIL(close);
    return TANGRAM_REAL_CALL(close)(fd);
}

int TANGRAM_WRAP(fsync)(int fd) {
    TFS_File* tf = fd2tf(fd);
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

int TANGRAM_WRAP(access)(const char *pathname, int mode) {
    // TODO
    if(tangram_should_intercept(pathname)) {
        TFS_File* tf = path2tf(pathname);
        if (tf)
            return 0;
    }
    MAP_OR_FAIL(access);
    return TANGRAM_REAL_CALL(access)(pathname, mode);
}

int TANGRAM_WRAP(unlink)(const char *pathname) {
    // TODO
    if(tangram_should_intercept(pathname)) {
        TFS_File* tf = path2tf(pathname);
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
