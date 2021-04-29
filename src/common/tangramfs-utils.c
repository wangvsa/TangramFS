#include <stdlib.h>
#include <stdio.h>

static size_t memory_usage = 0;

void* tangram_malloc(size_t size) {
    memory_usage += size;
    return malloc(size);
}

void tangram_free(void* ptr, size_t size) {
    memory_usage -= size;
    free(ptr);
}

void tangram_write_server_addr(char* persist_dir, char* addr) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "w");
    fprintf(f, "%s", addr);
    fclose(f);
}

void tangram_read_server_addr(char* persist_dir, char* addr) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "r");
    fscanf(f, "%s", addr);
    fclose(f);
}
