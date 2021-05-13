#ifndef _TANGRAMFS_RMA_CLIENT_H_
#define _TANGRAMFS_RMA_CLIENT_H_

#include "tangramfs-rpc.h"

/**
 * The Mercury client for RMA, however, is started
 * only when the transfer is required. It will connect
 * to the server that has the data. It is run by the
 * main thread.
 */
void tangram_rma_client_start(const char* server_addr);
void tangram_rma_client_stop();
void tangram_rma_client_transfer(char* filename, int rank, size_t offset, size_t count, void* local_buf);

#endif
