#ifndef _TANGRAMFS_POSIX_WRAPPER_H_
#define _TANGRAMFS_POSIX_WRAPPER_H_
#include <unistd.h>
#include <sys/stat.h>
#include <dlfcn.h>
#include <fcntl.h>

/**
 * Note, whoever include this header file need to
 * put #define _GNU_SOURCE as their first line
 */

#ifdef TANGRAMFS_PRELOAD
    #define TANGRAM_WRAP(func) func

    /* Point __real_func to the real funciton using dlsym() */
    #define MAP_OR_FAIL(func)                                               \
    if (!(__real_##func)) {                                                 \
        __real_##func = dlsym(RTLD_NEXT, #func);                            \
        if (!(__real_##func)) {                                             \
            printf("Tangram failed to map symbol: %s\n", #func);            \
        }                                                                   \
    }

    #define TANGRAM_DECL_REAL_CALL(name, ret, args)  static ret(*__real_##name) args;

    /*
    * Call the real funciton
    * Before call the real function, we need to make sure its mapped by dlsym()
    * So, every time we use this marco directly, we need to call MAP_OR_FAIL before it
    */
    #define TANGRAM_REAL_CALL(func) __real_##func
#else
    #define TANGRAM_WRAP(func) __avoid_##func
    #define TANGRAM_REAL_CALL(func) func
    #define MAP_OR_FAIL(func)
    #define TANGRAM_DECL_REAL_CALL(name, ret, args)
#endif


TANGRAM_DECL_REAL_CALL(fopen, FILE*, (const char *filename, const char *mode));
TANGRAM_DECL_REAL_CALL(fdopen, FILE*, (int fd, const char *mode));
TANGRAM_DECL_REAL_CALL(fseek, int, (FILE *stream, long int offset, int origin));
TANGRAM_DECL_REAL_CALL(ftell, long int, (FILE *stream));
TANGRAM_DECL_REAL_CALL(rewind, void, (FILE *stream));
TANGRAM_DECL_REAL_CALL(fwrite, size_t, (const void *ptr, size_t size, size_t count, FILE* stream));
TANGRAM_DECL_REAL_CALL(fread, size_t, (void * ptr, size_t size, size_t count, FILE* stream));
TANGRAM_DECL_REAL_CALL(fclose, int, (FILE* stream));
TANGRAM_DECL_REAL_CALL(access, int, (const char *pathname, int mode));
TANGRAM_DECL_REAL_CALL(unlink, int, (const char* pathname));
// TODO on my local machine, this signature only intercepts calls with the 3rd argument
// Id does not intercept calls with two arguments like: open(filename, flag)
TANGRAM_DECL_REAL_CALL(open, int, (const char *pathname, int flags, ...));
TANGRAM_DECL_REAL_CALL(open64, int, (const char *pathname, int flags, ...));
TANGRAM_DECL_REAL_CALL(lseek, off_t, (int fd, off_t offset, int whence));
TANGRAM_DECL_REAL_CALL(write, ssize_t, (int fd, const void *buf, size_t count));
TANGRAM_DECL_REAL_CALL(read, ssize_t, (int fd, void *buf, size_t count));
TANGRAM_DECL_REAL_CALL(close, int, (int fd));
TANGRAM_DECL_REAL_CALL(pread, ssize_t, (int fd, void *buf, size_t count, off_t offset));
TANGRAM_DECL_REAL_CALL(pwrite, ssize_t, (int fd, const void *buf, size_t count, off_t offset));
TANGRAM_DECL_REAL_CALL(fsync, int, (int fd));
TANGRAM_DECL_REAL_CALL(__xstat, int, (int vers, const char* path, struct stat* buf));
TANGRAM_DECL_REAL_CALL(__lxstat, int, (int vers, const char *path, struct stat *buf));
TANGRAM_DECL_REAL_CALL(mkdir, int, (const char *pathname, mode_t mode));
TANGRAM_DECL_REAL_CALL(rmdir, int, (const char *pathname));



static inline
void tangram_map_real_calls() {
    MAP_OR_FAIL(fopen);
    MAP_OR_FAIL(fdopen);
    MAP_OR_FAIL(fseek);
    MAP_OR_FAIL(ftell);
    MAP_OR_FAIL(rewind);
    MAP_OR_FAIL(fwrite);
    MAP_OR_FAIL(fread);
    MAP_OR_FAIL(fclose);
    MAP_OR_FAIL(access);
    MAP_OR_FAIL(unlink);
    MAP_OR_FAIL(open);
    MAP_OR_FAIL(open64);
    MAP_OR_FAIL(lseek);
    MAP_OR_FAIL(write);
    MAP_OR_FAIL(read);
    MAP_OR_FAIL(close);
    MAP_OR_FAIL(pwrite);
    MAP_OR_FAIL(pread);
    MAP_OR_FAIL(fsync);
    MAP_OR_FAIL(__xstat);
    MAP_OR_FAIL(__lxstat);
    MAP_OR_FAIL(mkdir);
    MAP_OR_FAIL(rmdir);
}

#endif
