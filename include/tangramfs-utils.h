#ifndef _TANGRAMFS_UTILS_H_
#define _TANGRAMFS_UTILS_H_

void* tangram_malloc(size_t size);
void  tangram_free(void*ptr, size_t size);

void tangram_write_server_addr(char* addr);
void tangram_read_server_addr(char* addr);

#endif
