#ifndef _TANGRAMFS_METADATA_H_
#define _TANGRAMFS_METADATA_H_

void tangram_ms_init();
void tangram_ms_finalize();
void tangram_ms_handle_notify(int rank, char* filename, size_t offset, size_t count);

#endif
