#include <linux/limits.h>
#include <assert.h>
#include <stdio.h>
#include "tangramfs.h"
#include "uthash.h"

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
    /*
    * Call the real funciton
    * Before call the real function, we need to make sure its mapped by dlsym()
    * So, every time we use this marco directly, we need to call MAP_OR_FAIL before it
    */
    #define TANGRAM_REAL_CALL(func) __real_##func
#else
    #define TANGRAM_WRAP(func) __avoid_##func
    #define TANGRAM_REAL_CALL(func) func
#endif



typedef struct TFSFileMap_t {
    TFS_File *tf;
    FILE* fake_stream;

    UT_hash_handle hh;
} TFSFileMap;

static TFSFileMap* tf_map;

TFS_File* get_tfs_file(FILE* stream) {

    TFSFileMap *found = NULL;
    HASH_FIND(hh, tf_map, stream, sizeof(FILE), found);
    if(!found) return NULL;

    return found->tf;
}

FILE* TANGRAM_WRAP(fopen)(const char *filename, const char *mode)
{
    printf("filename: %s\n", filename);
    TFSFileMap *entry = malloc(sizeof(TFSFileMap));
    entry->tf = tfs_open(filename, mode);

    HASH_ADD_KEYPTR(hh, tf_map, entry->fake_stream, sizeof(FILE), entry);
    return entry->fake_stream;
}


int TANGRAM_WRAP(fseek)(FILE *stream, long int offset, int origin)
{
    TFS_File* tf = get_tfs_file(stream);
    if(!tf)
        return TANGRAM_REAL_CALL(fseek)(stream, offset, origin);

    if(origin == SEEK_SET) {
        tf->offset = offset;
    } else if(origin == SEEK_CUR) {
        tf->offset += offset;
    } else if(origin == SEEK_END) {
        printf("Seek to the end is not supported yet");
    }

    return 0;
}

size_t TANGRAM_WRAP(fwrite)(const void *ptr, size_t size, size_t count, FILE * stream)
{
    TFS_File *tf = get_tfs_file(stream);
    if(!tf)
        return TANGRAM_REAL_CALL(fwrite)(ptr, size, count, stream);

    tfs_write(tf, ptr, tf->offset, count*size);
}

size_t TANGRAM_WRAP(fread)(void * ptr, size_t size, size_t count, FILE * stream)
{
    TFS_File *tf = get_tfs_file(stream);
    if(!tf)
        return TANGRAM_REAL_CALL(fread)(ptr, size, count, stream);

    tfs_read(tf, ptr, tf->offset, count*size);
}

int TANGRAM_WRAP(fclose)(FILE * stream)
{
    TFSFileMap *found = NULL;
    HASH_FIND(hh, tf_map, stream, sizeof(FILE), found);
    if(!found)
        return TANGRAM_REAL_CALL(fclose)(stream);

    tfs_close(found->tf);

    HASH_DEL(tf_map, found);
    free(found);
}
