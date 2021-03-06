#ifndef _TANGRAMFS_UTILS_H_
#define _TANGRAMFS_UTILS_H_

void* tangram_malloc(size_t size);
void  tangram_free(void*ptr, size_t size);

void tangram_write_server_addr(const char* persist_dir, void* addr, size_t addr_len);
void tangram_read_server_addr(const char* persist_dir, void** addr, size_t* addr_len);

double tangram_wtime();

#endif
