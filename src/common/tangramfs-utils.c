#include <stdlib.h>
#include <stdio.h>

static size_t memory_usage = 0;
#define CFG_PATH "/home/wangchen/Sources/TangramFS/src/server/tfs.cfg"

void* tangram_malloc(size_t size) {
    memory_usage += size;
    return malloc(size);
}

void tangram_free(void* ptr, size_t size) {
    memory_usage -= size;
    free(ptr);
}

void tangram_write_server_addr(char* addr) {
    FILE* f = fopen(CFG_PATH, "w");
    fprintf(f, "%s", addr);
    fclose(f);
}

void tangram_read_server_addr(char* addr) {
    FILE* f = fopen(CFG_PATH, "r");
    fscanf(f, "%s", addr);
    fclose(f);
}
