#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <rdma/fabric.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

struct fi_info *info;
struct fi_info *hints;
struct fid_fabric *fabric;
struct fid_eq *eq;                  // event queue
struct fid_cq *cq;                  // completion queue
struct fid_domain *domain;
struct fid_mr *mr;

struct fid_ep *ep;                  // active endpoint
struct fid_pep *pep;                // passive endpoint, used only by server


void *buff;
size_t buff_size = 32 * 1024 * 1024;


void *context = NULL;


void print_providers() {
    struct fi_info* tmp = info;
    while(tmp) {
        char* prov_name = tmp->fabric_attr->prov_name;
        printf("name: %s, prov_name: %s\n", tmp->fabric_attr->name, prov_name);
        tmp = tmp->next;
    }
}


void common_init(const char* addr, uint64_t flags) {
    // 1. fi_info
    fi_getinfo(FI_VERSION(1, 11), addr, "12345", flags, hints, &info);

    // 2. fi_fabric
    fi_fabric(info->fabric_attr, &fabric, context);

    // 3. fi_eq
    struct fi_eq_attr eq_attr = {
		.size = 0,
		.flags = 0,
		.wait_obj = FI_WAIT_UNSPEC,
		.signaling_vector = 0,
		.wait_set = NULL,
	};
	fi_eq_open(fabric, &eq_attr, &eq, NULL);

    // 4. fi_domain
    fi_domain(fabric, info, &domain, NULL);

    // 5. fi_cq
    struct fi_cq_attr cq_attr = {
		.size = 0,
		.flags = 0,
		.format = FI_CQ_FORMAT_MSG,
		.wait_obj = FI_WAIT_UNSPEC,
		.signaling_vector = 0,
		.wait_cond = FI_CQ_COND_NONE,
		.wait_set = NULL,
	};
    fi_cq_open(domain, &cq_attr, &cq, NULL);

    fi_mr_reg(domain, buff, buff_size,  FI_REMOTE_READ|FI_REMOTE_WRITE|FI_SEND|FI_RECV,
			            0, 0, 0, &mr, NULL);
    printf("name: %s, provider: %s\n", info->fabric_attr->name, info->fabric_attr->prov_name);
}




void client_init() {
    int err;
    err = fi_endpoint(domain, info, &ep, context);

    /*
    struct fid_av *av;
    struct fi_av_attr av_attr = {
        .type = FI_AV_UNSPEC,
        .rx_ctx_bits = 0,
        .count = 0,
        .ep_per_node = 0,
        .name = "my_av",
        .map_addr = NULL,
        .flags = FI_SYMMETRIC,
    };
    fi_av_open(domain, &av_attr, &av, NULL);
    fi_ep_bind(ep, &av->fid, 0);
    */

    fi_ep_bind(ep, &eq->fid, 0);
    fi_ep_bind(ep, &cq->fid, FI_TRANSMIT | FI_RECV);

    fi_enable(ep);

    fi_connect(ep, info->dest_addr, NULL, 0);

    struct fi_eq_cm_entry entry;
	uint32_t event;
    fi_eq_sread(eq, &event, &entry, sizeof (entry), -1, 0);
    assert(event == FI_CONNECTED);
}


void client() {
    hints = fi_allocinfo();
    hints->addr_format = FI_SOCKADDR_IN;
	hints->ep_attr->type = FI_EP_MSG;
	hints->domain_attr->mr_mode = FI_MR_BASIC;
	hints->caps = FI_MSG | FI_RMA;
	hints->mode = FI_CONTEXT | FI_LOCAL_MR | FI_RX_CQ_DATA;

    common_init("127.0.0.1", 0);
    client_init();

    // The server should receive a shutdown event
    fi_shutdown(ep, 0);
}

void server_init() {
    // 1. Server uses passive endpoint to listen for connect request
    fi_passive_ep(fabric, info, &pep, NULL);
    fi_pep_bind(pep, &eq->fid, 0);
    fi_listen(pep);
    printf("Server listening...\n");

    struct fi_eq_cm_entry entry;
    uint32_t event;

    // Wait for the FI_CONNREQ event
    fi_eq_sread(eq, &event, &entry, sizeof(entry), -1, 0);
    assert(event == FI_CONNREQ);
    printf("Server received connect request\n");

    // 2. Use active endpoint to perform actual data transfer
    fi_endpoint(domain, entry.info, &ep, NULL);
    fi_ep_bind(ep, &eq->fid, 0);
    fi_ep_bind(ep, &cq->fid, FI_TRANSMIT | FI_RECV);
    fi_accept(ep, NULL, 0);

    // Now wait for the FI_CONNECTED event
    fi_eq_sread(eq, &event, &entry, sizeof(entry), -1, 0);
    assert(event == FI_CONNECTED);
    printf("Connected!\n");

    //fi_send(ep, buff, sizeof(keys), fi_mr_desc(mr), 0, NULL);

    // Finally, we should receive a shutdown event
    fi_eq_sread(eq, &event, &entry, sizeof(entry), -1, 0);
    assert(event == FI_SHUTDOWN);
}

void server() {
    hints = fi_allocinfo();
    hints->addr_format = FI_SOCKADDR_IN;
	hints->ep_attr->type = FI_EP_MSG;
	hints->domain_attr->mr_mode = FI_MR_BASIC;
	hints->caps = FI_MSG | FI_RMA;
	hints->mode = FI_CONTEXT | FI_LOCAL_MR | FI_RX_CQ_DATA;

    common_init(NULL, FI_SOURCE);
    server_init();
}

int main(int argc, char* argv[]) {

    buff = malloc(buff_size);
    if(argc > 1)
        client();
    else
        server();

    //print_providers();

    fi_close(&fabric->fid);
    fi_close(&ep->fid);
	fi_close(&mr->fid);
	fi_close(&cq->fid);
	fi_close(&eq->fid);
	fi_close(&domain->fid);
	fi_close(&fabric->fid);
    fi_freeinfo(hints);
    fi_freeinfo(info);
    free(buff);

    return 0;
}
