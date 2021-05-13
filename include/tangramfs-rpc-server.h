#ifndef _TANGRAMFS_RPC_SERVER_H_
#define _TANGRAMFS_RPC_SERVER_H_

#include "tangramfs-rpc.h"

/**
 * Mercury server for handling metadata operations,
 * incluing POST and QUERY
 *
 * The server program is a seperate program.
 * under ./src/server
 */
void tangram_server_start(char* persist_dir, char* server_addr);
void tangram_server_stop();

#endif
