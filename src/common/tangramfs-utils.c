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

void tangram_write_uct_server_addr(const char* persist_dir, void* dev_addr, size_t dev_addr_len,
                                    void* iface_addr, size_t iface_addr_len) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = fopen(cfg_path, "wb");
    fwrite(&dev_addr_len, sizeof(dev_addr_len), 1, f);
    fwrite(dev_addr, 1, dev_addr_len, f);
    fwrite(&iface_addr_len, sizeof(iface_addr_len), 1, f);
    fwrite(iface_addr, 1, iface_addr_len, f);

    fclose(f);
}
void tangram_read_uct_server_addr(const char* persist_dir, void** dev_addr, void** iface_addr) {
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    size_t addr_len;
    FILE* f = fopen(cfg_path, "r");

    fread(&addr_len, sizeof(size_t), 1, f);
    *dev_addr = malloc(addr_len);
    fread(*dev_addr, 1, addr_len, f);

    fread(&addr_len, sizeof(size_t), 1, f);
    *iface_addr = malloc(addr_len);
    fread(*iface_addr, 1, addr_len, f);
    fclose(f);
}


double tangram_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (time.tv_sec + ((double)time.tv_usec / 1000000));
}

