#ifndef _TANGRAMFS_RMA_SERVER_H_
#define _TANGRAMFS_RMA_SERVER_H_

#include "tangramfs-rpc.h"

/**
 * Each client process spawns a pthread to run
 * a Mercury server for RMA
 */
void tangram_rma_server_start(char* server_addr);
void tangram_rma_server_stop();


#endif
