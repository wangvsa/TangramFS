#ifndef _TANGRAMFS_UTILS_H_
#define _TANGRAMFS_UTILS_H_

void* tangram_malloc(size_t size);
void  tangram_free(void*ptr, size_t size);

void tangram_write_uct_server_addr(const char* persist_dir, void* dev_addr, size_t dev_addr_len, void* iface_addr, size_t iface_addr_len);
void tangram_read_uct_server_addr(const char* persist_dir, void** dev_addr, void** iface_addr);

double tangram_wtime();

#endif
