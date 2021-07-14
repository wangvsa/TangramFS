#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include "tangramfs-utils.h"

static size_t memory_usage = 0;

void* tangram_malloc(size_t size) {
    memory_usage += size;
    return malloc(size);
}

void tangram_free(void* ptr, size_t size) {
    memory_usage -= size;
    free(ptr);
}

void tangram_write_server_addr(const char* persist_dir, void* addr, size_t addr_len) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "wb");
    fwrite(&addr_len, sizeof(addr_len), 1, f);
    fwrite(addr, 1, addr_len, f);
    fclose(f);
}

void tangram_read_server_addr(const char* persist_dir, void** addr, size_t* addr_len) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "r");
    fread(addr_len, sizeof(size_t), 1, f);
    *addr = malloc(*addr_len);
    fread(*addr, 1, *addr_len, f);
    fclose(f);
}

double tangram_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (time.tv_sec + ((double)time.tv_usec / 1000000));
}

