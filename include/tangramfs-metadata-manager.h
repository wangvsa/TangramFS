#ifndef _TANGRAMFS_METADATA_H_
#define _TANGRAMFS_METADATA_H_
#include <stdbool.h>
#include <sys/stat.h>
#include "tangramfs-rpc.h"

void tangram_metamgr_init();
void tangram_metamgr_finalize();
void tangram_metamgr_handle_post(tangram_uct_addr_t* client, char* filename, size_t offset, size_t count);
void tangram_metamgr_handle_unpost_file(tangram_uct_addr_t* client, char* filename);
void tangram_metamgr_handle_unpost_client(tangram_uct_addr_t* client);
void tangram_metamgr_handle_stat(char* path, struct stat* buf);
tangram_uct_addr_t* tangram_metamgr_handle_query(char* filename, size_t offset, size_t count);

#endif
