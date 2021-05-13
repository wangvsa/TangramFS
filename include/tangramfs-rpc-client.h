#ifndef _TANGRAMFS_RPC_CLIENT_H_
#define _TANGRAMFS_RPC_CLIENT_H_

#include "tangramfs-rpc.h"

/**
 * Each client process spaws a pthread to run
 * a Mercury client that is connected with
 * the Mercury server.
 */
void tangram_rpc_client_start(const char* server_addr);
void tangram_rpc_client_stop();
void tangram_rpc_issue_rpc(const char* rpc_name, char* filename, int rank, size_t *offsets, size_t *counts, int len);
rpc_query_out tangram_rpc_query_result();

#endif
