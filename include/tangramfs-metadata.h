#ifndef _TANGRAMFS_METADATA_H_
#define _TANGRAMFS_METADATA_H_
#include <stdbool.h>
#include <sys/stat.h>
#include "tangramfs-rpc.h"

void tangram_ms_init();
void tangram_ms_finalize();
void tangram_ms_handle_post(tangram_uct_addr_t* client, char* filename, size_t offset, size_t count);
tangram_uct_addr_t* tangram_ms_handle_query(char* filename, size_t offset, size_t count);
void tangram_ms_handle_stat(char* path, struct stat* buf);

#endif
