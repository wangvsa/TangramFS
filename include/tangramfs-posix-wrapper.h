#ifndef _TANGRAMFS_POSIX_WRAPPER_H_
#define _TANGRAMFS_POSIX_WRAPPER_H_
#include <dlfcn.h>

#ifdef TANGRAM_PRELOAD
    #define TANGRAM_WRAP(func) func

    /* Point __real_func to the real funciton using dlsym() */
    #define MAP_OR_FAIL(func)                                               \
    if (!(__real_##func)) {                                                 \
        __real_##func = dlsym(RTLD_NEXT, #func);                            \
        if (!(__real_##func)) {                                             \
            printf("Tangram failed to map symbol: %s\n", #func);            \
        }                                                                   \
    }

    #define TANGRAM_DECL_REAL_CALL(name, ret, args) ret(*__real_##name) args;

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
TANGRAM_DECL_REAL_CALL(fseek, int, (FILE *stream, long int offset, int origin));
TANGRAM_DECL_REAL_CALL(fwrite, size_t, (const void *ptr, size_t size, size_t count, FILE* stream));
TANGRAM_DECL_REAL_CALL(fread, size_t, (void * ptr, size_t size, size_t count, FILE* stream));
TANGRAM_DECL_REAL_CALL(fclose, int, (FILE* stream));

TANGRAM_DECL_REAL_CALL(open, int, (const char *pathname, int flags, ...));
TANGRAM_DECL_REAL_CALL(lseek, off_t, (int fd, off_t offset, int whence));
TANGRAM_DECL_REAL_CALL(write, ssize_t, (int fd, const void *buf, size_t count));
TANGRAM_DECL_REAL_CALL(read, ssize_t, (int fd, void *buf, size_t count));
TANGRAM_DECL_REAL_CALL(close, int, (int fd));
TANGRAM_DECL_REAL_CALL(fsync, int, (int fd));
TANGRAM_DECL_REAL_CALL(__xstat, int, (int vers, const char* path, struct stat* buf));

#endif

