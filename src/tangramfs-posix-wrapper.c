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

typedef struct TFSFileMap_t {
    TFS_File *tf;
    FILE* fake_stream;  // key
    UT_hash_handle hh;
} TFSFileMap;

typedef struct TFSFdMap_t {
    TFS_File *tf;
    int fd;             // key
    UT_hash_handle hh;
} TFSFdMap;

static TFSFileMap* tf_map;
static TFSFdMap* tf_fd_map;

TFS_File* stream2tf(FILE* stream) {
    TFSFileMap *found = NULL;
    HASH_FIND(hh, tf_map, stream, sizeof(FILE), found);
    if(!found) return NULL;

    return found->tf;
}

TFS_File* fd2tf(int fd) {
    TFSFdMap* found = NULL;
    HASH_FIND_INT(tf_fd_map, &fd, found);
    if(!found) return NULL;

    return found->tf;
}


FILE* TANGRAM_WRAP(fopen)(const char *filename, const char *mode)
{
    if(tangram_should_intercept(filename)) {
        TFSFileMap *entry = malloc(sizeof(TFSFileMap));
        entry->tf = tfs_open(filename);
        HASH_ADD_KEYPTR(hh, tf_map, entry->fake_stream, sizeof(FILE), entry);
        return entry->fake_stream;
    }

    MAP_OR_FAIL(fopen);
    return TANGRAM_REAL_CALL(fopen)(filename, mode);
}

int TANGRAM_WRAP(fseek)(FILE *stream, long int offset, int origin)
{
    TFS_File* tf = stream2tf(stream);
    if(tf)
        return tfs_seek(tf, offset, origin);

    MAP_OR_FAIL(fseek);
    return TANGRAM_REAL_CALL(fseek)(stream, offset, origin);
}

size_t TANGRAM_WRAP(fwrite)(const void *ptr, size_t size, size_t count, FILE * stream)
{
    TFS_File *tf = stream2tf(stream);
    if(tf)
        return tangram_write_impl(tf, ptr, count*size);

    MAP_OR_FAIL(fwrite);
    return TANGRAM_REAL_CALL(fwrite)(ptr, size, count, stream);
}

size_t TANGRAM_WRAP(fread)(void * ptr, size_t size, size_t count, FILE * stream)
{
    TFS_File *tf = stream2tf(stream);
    if(tf)
        return tangram_read_impl(tf, ptr, count*size);

    MAP_OR_FAIL(fread);
    return TANGRAM_REAL_CALL(fread)(ptr, size, count, stream);
}

int TANGRAM_WRAP(fclose)(FILE * stream)
{
    TFSFileMap *found = NULL;
    HASH_FIND(hh, tf_map, stream, sizeof(FILE), found);
    if(found) {
        int res = tangram_close_impl(found->tf);
        HASH_DEL(tf_map, found);
        free(found);
        return res;
    }

    MAP_OR_FAIL(fclose);
    return TANGRAM_REAL_CALL(fclose)(stream);
}

int TANGRAM_WRAP(open)(const char *pathname, int flags, ...)
{
    if(tangram_should_intercept(pathname)) {
        TFSFdMap *entry = malloc(sizeof(TFSFdMap));
        entry->tf = tfs_open(pathname);
        entry->fd = entry->tf->local_fd;
        HASH_ADD_INT(tf_fd_map, fd, entry);
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
    if(tf)
        return tangram_read_impl(tf, buf, count);

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
    if(tangram_should_intercept(path))
        return 0;

    MAP_OR_FAIL(__xstat);
    return TANGRAM_REAL_CALL(__xstat)(vers, path, buf);
}







void init_tfs() {
    const char* persist_dir = getenv("TANGRAM_PERSIST_DIR");
    const char* buffer_dir = getenv("TANGRAM_BUFFER_DIR");
    if(!persist_dir || !buffer_dir)
        printf("Please set TANGRAM_PERSIST_DIR and TANGRAM_BUFFER_DIR\n");
    else
        tfs_init(persist_dir, buffer_dir);
}


int TANGRAM_WRAP(MPI_Init)(int *argc, char ***argv) {
    int res = PMPI_Init(argc, argv);
    init_tfs();
    return res;
}

int TANGRAM_WRAP(MPI_Init_thread)(int *argc, char ***argv, int required, int *provided) {
    int res = PMPI_Init_thread(argc, argv, required, provided);
    init_tfs();
    return res;
}

int TANGRAM_WRAP(MPI_Finalize)() {
    tfs_finalize();
    return PMPI_Finalize();
}
