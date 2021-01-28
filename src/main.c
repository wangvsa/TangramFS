#include <stdio.h>
#include "mpi.h"
#include "tangramfs.h"

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);

    tfs_init();

    printf("Hello World\n");

    TFILE* tf;
    tf = tfs_open("./1.txt", "w");

    char data[256];
    tfs_write(tf, data, 100, 0);
    tfs_read(tf, data, 100, 0);

    tfs_close(tf);

    MPI_Finalize();
    return 0;
}
