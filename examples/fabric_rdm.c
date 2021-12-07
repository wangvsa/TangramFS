#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <mpi.h>

#include <rdma/fabric.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>

int mpi_rank, mpi_size;

struct fi_info *info;
struct fi_info *hints;
struct fid_fabric *fabric;
struct fid_eq *eq;                  // event queue
struct fid_cq *cq;                  // completion queue
struct fid_domain *domain;
struct fid_mr *mr;

struct fid_ep *ep;                  // active endpoint


void *buff;
size_t buff_size = 32 * 1024;
int ret;


void *context = NULL;


void print_providers() {
    struct fi_info* tmp = info;
    while(tmp) {
        char* prov_name = tmp->fabric_attr->prov_name;
        printf("name: %s, prov_name: %s\n", tmp->fabric_attr->name, prov_name);
        tmp = tmp->next;
    }
}

void* exchange_addr_mpi() {

    size_t addrlen = 0;
    fi_getname(&ep->fid, NULL, &addrlen);
    void* myaddr = malloc(addrlen);
    fi_getname(&ep->fid, myaddr, &addrlen);

    void* recvbuf = malloc(addrlen);

    int dest = (mpi_rank + 1) % mpi_size;
    MPI_Sendrecv(myaddr, addrlen, MPI_BYTE, dest, 999,
                recvbuf, addrlen, MPI_BYTE, dest, 999, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    free(myaddr);
    return recvbuf;
}

void print_addr(struct fid_av *av, void* addr) {
    char addrstr[128] = {0};
    size_t len = 128;
    fi_av_straddr(av, addr, addrstr, &len);
    printf("straddr: %s\n", addrstr);
}

void common_init() {

    hints = fi_allocinfo();
    //hints->addr_format = FI_SOCKADDR_IN;
	hints->ep_attr->type = FI_EP_RDM;
	hints->domain_attr->mr_mode = FI_MR_BASIC;
	//hints->caps = FI_MSG | FI_RMA;
	hints->caps = FI_RMA;
	hints->mode = FI_LOCAL_MR;

    // 1. fi_info
    ret = fi_getinfo(FI_VERSION(1, 11), NULL, NULL, 0, hints, &info);
    assert(ret == 0);

    // 2. fi_fabric
    fi_fabric(info->fabric_attr, &fabric, context);
    assert(ret == 0);

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
    struct fi_cq_attr cq_attr;
    memset(&cq_attr, 0, sizeof(struct fi_cq_attr));
    cq_attr.format = FI_CQ_FORMAT_DATA;
    //cq_attr.size = buff_size;
    fi_cq_open(domain, &cq_attr, &cq, NULL);

    fi_mr_reg(domain, buff, buff_size,  FI_REMOTE_READ|FI_REMOTE_WRITE|FI_SEND|FI_RECV, 0, 0, 0, &mr, NULL);
    printf("name: %s, provider: %s\n", info->fabric_attr->name, info->fabric_attr->prov_name);
}


void client_init() {
    fi_endpoint(domain, info, &ep, context);

    struct fi_av_attr av_attr;
    memset(&av_attr, 0, sizeof(av_attr));
    av_attr.type = FI_AV_MAP;
    struct fid_av *av;
    fi_av_open(domain, &av_attr, &av, NULL);

    void* recvaddr = exchange_addr_mpi();
    print_addr(av, recvaddr);
    fi_addr_t remote_addr = FI_ADDR_UNSPEC;
    fi_av_insert(av, recvaddr, 1, &remote_addr, 0, context);
    free(recvaddr);

    fi_ep_bind(ep, &eq->fid, 0);
    fi_ep_bind(ep, &cq->fid, FI_SEND | FI_TRANSMIT | FI_RECV);
    fi_ep_bind(ep, &av->fid, 0);        // will crash at fi_send without binding av
    fi_enable(ep);

    int data = 10;
    memcpy(buff, &data, sizeof(data));
    ret = fi_send(ep, buff, sizeof(data), NULL, remote_addr, context);
    printf("err: %s\n", fi_strerror(-ret));
    assert(ret==0);
    /*
    struct iovec myiov = {
        .iov_base = buff,
        .iov_len = sizeof(data),
    };
    struct fi_msg msg = {
        .msg_iov = &myiov,
        .desc = NULL,
        .iov_count = 1,
        .addr = remote_addr,
        .context = NULL,
        .data = 0,
    };
    fi_sendmsg(ep, &msg, 0);
    */

    printf("Client sending data\n");
    struct fi_cq_data_entry cq_entry;
    while(1) {
        ret = fi_cq_read(cq, &cq_entry, 1);
        if(ret > 0) {
            printf("Client: send data successfully.\n");
            break;
        }
        assert(ret == -FI_EAGAIN);
    }

    fi_close(&av->fid);
}


void client() {

    common_init();
    client_init();

    // The server should receive a shutdown event
    fi_shutdown(ep, 0);
    printf("Client: shutdown\n");
}

void server_init() {
    fi_endpoint(domain, info, &ep, context);

    struct fi_av_attr av_attr;
    memset(&av_attr, 0, sizeof(av_attr));
    av_attr.type = FI_AV_MAP;
    struct fid_av *av;
    fi_av_open(domain, &av_attr, &av, NULL);

    void* recvaddr = exchange_addr_mpi();
    print_addr(av, recvaddr);
    fi_addr_t remote_addr = FI_ADDR_UNSPEC;
    fi_av_insert(av,recvaddr, 1, &remote_addr, 0, NULL);
    free(recvaddr);

    fi_ep_bind(ep, &eq->fid, 0);
    fi_ep_bind(ep, &cq->fid, FI_SEND | FI_TRANSMIT | FI_RECV);
    fi_ep_bind(ep, &av->fid, 0);        // will crash at fi_send without binding av
    fi_enable(ep);

    ret = fi_recv(ep, buff, sizeof(int), NULL, remote_addr, NULL);
    assert(ret == 0);
    struct fi_cq_data_entry cq_entry;
    while(1) {
        ret = fi_cq_read(cq, &cq_entry, 1);
        if(ret > 0) {
            printf("Server : recv data successfully.\n");
            break;
        }
        assert(ret == -FI_EAGAIN);
    }
}


void server() {
    common_init();
    server_init();
}


int main(int argc, char* argv[]) {

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    buff = malloc(buff_size);
    if(mpi_rank == 0)
        server();
    if(mpi_rank == 1)
        client();

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

    MPI_Finalize();
    return 0;
}
