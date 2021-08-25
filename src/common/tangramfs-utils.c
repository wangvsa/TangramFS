#define _GNU_SOURCE
#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include "tangramfs-utils.h"
#include "tangramfs-posix-wrapper.h"

static size_t memory_usage = 0;

void* tangram_malloc(size_t size) {
    memory_usage += size;
    return malloc(size);
}

void tangram_free(void* ptr, size_t size) {
    memory_usage -= size;
    free(ptr);
}

void tangram_write_uct_server_addr(const char* persist_dir, void* dev_addr, size_t dev_addr_len,
                                    void* iface_addr, size_t iface_addr_len) {
    tangram_map_real_calls();
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    FILE* f = TANGRAM_REAL_CALL(fopen)(cfg_path, "wb");
    TANGRAM_REAL_CALL(fwrite)(&dev_addr_len, sizeof(dev_addr_len), 1, f);
    TANGRAM_REAL_CALL(fwrite)(dev_addr, 1, dev_addr_len, f);
    TANGRAM_REAL_CALL(fwrite)(&iface_addr_len, sizeof(iface_addr_len), 1, f);
    TANGRAM_REAL_CALL(fwrite)(iface_addr, 1, iface_addr_len, f);

    TANGRAM_REAL_CALL(fclose)(f);
}

void tangram_read_uct_server_addr(const char* persist_dir, void** dev_addr, void** iface_addr) {
    tangram_map_real_calls();
    char cfg_path[512] = {0};
    sprintf(cfg_path, "%s/tfs.cfg", persist_dir);

    size_t addr_len;
    FILE* f = TANGRAM_REAL_CALL(fopen)(cfg_path, "r");

    TANGRAM_REAL_CALL(fread)(&addr_len, sizeof(size_t), 1, f);
    *dev_addr = malloc(addr_len);
    TANGRAM_REAL_CALL(fread)(*dev_addr, 1, addr_len, f);

    TANGRAM_REAL_CALL(fread)(&addr_len, sizeof(size_t), 1, f);
    *iface_addr = malloc(addr_len);
    TANGRAM_REAL_CALL(fread)(*iface_addr, 1, addr_len, f);
    TANGRAM_REAL_CALL(fclose)(f);
}


double tangram_wtime() {
    struct timeval time;
    gettimeofday(&time, NULL);
    return (time.tv_sec + ((double)time.tv_usec / 1000000));
}

