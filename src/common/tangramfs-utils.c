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

void tangram_write_server_addr(const char* persist_dir, const char* iface, const char* ip_addr) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "w");
    fprintf(f, "%s\n", iface);
    fprintf(f, "%s\n", ip_addr);
    fclose(f);
}

void tangram_read_server_addr(const char* persist_dir, char* iface, char* ip_addr) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "r");
    fscanf(f, "%s\n%s\n", iface, ip_addr);
    fclose(f);
}

double tangram_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (time.tv_sec + ((double)time.tv_usec / 1000000));
}

