#include <stdio.h>
#include <stdlib.h>

#include <rdma/fabric.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>


int main(int argc, char* argv[]) {

    struct fi_info *hints, *info;
    hints = fi_allocinfo();

    fi_getinfo(FI_VERSION(1, 11), NULL, NULL, 0, hints, &info);
    fi_freeinfo(hints);

    struct fi_info* tmp = info;
    while(tmp) {
        char* prov_name = tmp->fabric_attr->prov_name;
        printf("name: %s, prov_name: %s\n", tmp->fabric_attr->name, prov_name);
        tmp = tmp->next;
    }


    fi_freeinfo(info);
    return 0;
}
