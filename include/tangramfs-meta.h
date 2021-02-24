#ifndef _TANGRAMFS_META_SERVER_H_
#define _TANGRAMFS_META_SERVER_H_

#define MERCURY_PROTOCOL    "mpi+static"
#define MERCURY_SERVER_ADDR "rank#0"

#define PRC_NAME            "hello"

void tangram_meta_server_start();
void tangram_meta_server_stop();

void tangram_meta_client_start();
void tangram_meta_client_stop();
void tangram_meta_issue_rpc();


#endif
