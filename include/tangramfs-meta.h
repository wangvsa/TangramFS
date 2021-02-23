#ifndef _TANGRAMFS_META_SERVER_H_
#define _TANGRAMFS_META_SERVER_H_

#define MERCURY_PROTOCOL    "mpi+static"
#define MERCURY_SERVER_ADDR "rank#0"

void tfs_meta_server_start();
void tfs_meta_server_stop();

void tfs_meta_client_start();
void tfs_meta_client_stop();
void tfs_meta_issue_rpc();


#endif
