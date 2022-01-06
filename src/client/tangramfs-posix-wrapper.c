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

typedef struct tfs_file_entry {
    FILE* stream;   // key 1
    char* path;     // key 2
    int fd;         // key 3

    tfs_file_t *tf; // value

    UT_hash_handle hh_stream;
    UT_hash_handle hh_path;
    UT_hash_handle hh_fd;

} tfs_file_entry_t;

static tfs_file_entry_t* tf_by_stream = NULL;
static tfs_file_entry_t* tf_by_path   = NULL;
static tfs_file_entry_t* tf_by_fd     = NULL;

pthread_rwlock_t uthash_lock = PTHREAD_RWLOCK_INITIALIZER;

#define HASH_FIND_RLOCK(hh, head, key, keylen, entry)           \
    pthread_rwlock_rdlock(&uthash_lock);                        \
    HASH_FIND(hh, head, key, keylen, entry);                    \
    pthread_rwlock_unlock(&uthash_lock);

#define HASH_ADD_WLOCK(hh, head, key, keylen, entry)            \
    pthread_rwlock_wrlock(&uthash_lock);                        \
    HASH_ADD(hh, head, key, keylen, entry);                     \
    pthread_rwlock_unlock(&uthash_lock);

#define HASH_ADD_KEYPTR_WLOCK(hh, head, key, keylen, entry)     \
    pthread_rwlock_wrlock(&uthash_lock);                        \
    HASH_ADD_KEYPTR(hh, head, key, keylen, entry);              \
    pthread_rwlock_unlock(&uthash_lock);

tfs_file_t* stream2tf(FILE* stream) {
    tfs_file_entry_t *found = NULL;
    HASH_FIND_RLOCK(hh_stream, tf_by_stream, &stream, sizeof(FILE*), found);
    if(found)
        return found->tf;
    return NULL;
}

tfs_file_t* fd2tf(int fd) {
    tfs_file_entry_t *found = NULL;
    HASH_FIND_RLOCK(hh_fd, tf_by_fd, &fd, sizeof(int), found);
    if(found)
        return found->tf;
    return NULL;
}

tfs_file_t* path2tf(const char* path) {
    char *key = realpath(path, NULL);
    if(key == NULL || path == NULL) return NULL;

    tfs_file_entry_t *found = NULL;
    HASH_FIND_RLOCK(hh_path, tf_by_path, key, strlen(key), found);

    free(key);
    if(found)
        return found->tf;
    return NULL;
}


tfs_file_entry_t* add_to_map(tfs_file_t* tf) {

    tfs_file_entry_t *entry = malloc(sizeof(tfs_file_entry_t));
    entry->tf     = tf;
    entry->fd     = tf->fd;
    entry->stream = tf->stream;
    entry->path   = realpath(tf->filename, NULL);

    if(entry->fd != -1) {   // must have "{}", because the macro below will translate to multiple lines.
        HASH_ADD_WLOCK(hh_fd, tf_by_fd, fd, sizeof(int), entry);
    }

    if(entry->stream != NULL) {
        HASH_ADD_WLOCK(hh_stream, tf_by_stream, stream, sizeof(FILE*), entry);
    }

    if(entry->path != NULL) {
        HASH_ADD_KEYPTR_WLOCK(hh_path, tf_by_path, entry->path, strlen(entry->path), entry);

        char* key = entry->path;
        tfs_file_entry_t *found = NULL;
        HASH_FIND_RLOCK(hh_path, tf_by_path, key, strlen(key), found);
        assert(found != NULL);

    }

    return entry;
}



FILE* TANGRAM_WRAP(fopen)(const char *filename, const char *mode)
{
    bool intercept = tangram_should_intercept(filename);

    FILE* stream;
    MAP_OR_FAIL(fopen);
    stream = TANGRAM_REAL_CALL(fopen)(filename, mode);

    if(intercept) {
        tfs_file_t* tf = tfs_open(filename);
        tf->stream = stream;
        tangram_debug("[tangramfs] fopen %s\n", filename);
        tfs_file_entry_t* entry = add_to_map(tf);
    }

    return stream;
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

    tfs_file_t *tf = stream2tf(stream);

    if(tf) {
        tangram_debug("[tangramfs] fclose %s\n", tf->filename);
        return tangram_close_impl(tf);
    }

    MAP_OR_FAIL(fclose);
    return TANGRAM_REAL_CALL(fclose)(stream);
}

int TANGRAM_WRAP(open)(const char *pathname, int flags, ...)
{
    bool intercept = tangram_should_intercept(pathname);
    int fd;

    MAP_OR_FAIL(open);
    if((flags & O_CREAT) || (flags & O_TMPFILE)) {
        va_list arg;
        va_start(arg, flags);
        int mode = va_arg(arg, int);
        va_end(arg);
        fd = TANGRAM_REAL_CALL(open)(pathname, flags, mode);
    } else {
        fd = TANGRAM_REAL_CALL(open)(pathname, flags);
    }

    if(intercept) {
        tfs_file_t* tf = tfs_open(pathname);
        tf->fd = fd;
        tangram_debug("[tangramfs] open %s %d\n", pathname, tf->local_fd);
        tfs_file_entry_t* entry = add_to_map(tf);
    }
    return fd;
}

int TANGRAM_WRAP(open64)(const char *pathname, int flags, ...)
{
    bool intercept = tangram_should_intercept(pathname);
    int fd;

    MAP_OR_FAIL(open64);
    if((flags & O_CREAT) || (flags & O_TMPFILE)) {
        va_list arg;
        va_start(arg, flags);
        int mode = va_arg(arg, int);
        va_end(arg);
        fd = TANGRAM_REAL_CALL(open64)(pathname, flags, mode);
    } else {
        fd = TANGRAM_REAL_CALL(open64)(pathname, flags);
    }

    if(intercept) {
        tfs_file_t* tf = tfs_open(pathname);
        tf->fd = fd;
        tfs_file_entry_t* entry = add_to_map(tf);
    }

    return fd;
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
    tfs_file_t* tf = fd2tf(fd);
    if(tf) {
        tangram_debug("[tangramfs] close %s\n", tf->filename);
        return tangram_close_impl(tf);
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

int fill_local_stat(tfs_file_t* tf, struct stat* buf, int vers) {
    MAP_OR_FAIL(__fxstat);
    return TANGRAM_REAL_CALL(__fxstat)(vers, tf->local_fd, buf);
}

int TANGRAM_WRAP(__xstat)(int vers, const char *path, struct stat *buf)
{
    // TODO: stat() call not implemented yet.
    tfs_file_t* tf = path2tf(path);
    if(tf) {
        tangram_debug("[tangramfs] stat %s\n", tf->filename);
        fill_local_stat(tf, buf, vers);
        tangram_issue_metadata_rpc(AM_ID_STAT_REQUEST, path, buf);
        return 0;
    }

    MAP_OR_FAIL(__xstat);
    return TANGRAM_REAL_CALL(__xstat)(vers, path, buf);
}

int TANGRAM_WRAP(__fxstat)(int vers, int fd, struct stat *buf)
{
    tfs_file_t* tf = fd2tf(fd);
    if(tf) {
        tangram_debug("[tangramfs] fstat %s\n", tf->filename);
        fill_local_stat(tf, buf, vers);
        tangram_issue_metadata_rpc(AM_ID_STAT_REQUEST, tf->filename, buf);
        return 0;
    }

    MAP_OR_FAIL(__fxstat);
    return TANGRAM_REAL_CALL(__fxstat)(vers, fd, buf);
}

int TANGRAM_WRAP(__lxstat)(int vers, const char *path, struct stat *buf)
{
    // TODO: stat() call not implemented yet.
    tfs_file_t* tf = path2tf(path);
    if(tf) {
        tangram_debug("[tangramfs] lstat %s\n", tf->filename);
        fill_local_stat(tf, buf, vers);
        tangram_issue_metadata_rpc(AM_ID_STAT_REQUEST, path, buf);
        return 0;
    }

    MAP_OR_FAIL(__lxstat);
    return TANGRAM_REAL_CALL(__lxstat)(vers, path, buf);
}

int TANGRAM_WRAP(access)(const char *pathname, int mode) {
    // TODO
    tfs_file_t* tf = path2tf(pathname);
    if (tf) {
        return 0;
    }
    MAP_OR_FAIL(access);
    return TANGRAM_REAL_CALL(access)(pathname, mode);
}

int TANGRAM_WRAP(unlink)(const char *pathname) {
    // TODO
    tfs_file_t* tf = path2tf(pathname);
    if (tf) {
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

    pthread_rwlock_wrlock(&uthash_lock);
    tfs_file_entry_t *entry, *tmp;
    // Should be enough to just free entries
    // in tf_by_path
    HASH_ITER(hh_path, tf_by_path, entry, tmp) {
        HASH_DELETE(hh_path, tf_by_path, entry);
        if(entry->path)
            free(entry->path);
        free(entry);
    }
    pthread_rwlock_unlock(&uthash_lock);

    return PMPI_Finalize();
}
