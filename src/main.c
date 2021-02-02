#include <stdio.h>
#include "mpi.h"
#include "tangramfs.h"

#define MB (1024*1024)

static int DATA_SIZE = 16*MB;
static int N = 5;

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //tfs_init("./", "/l/ssd");
    tfs_init("./", "/tmp");
    TFILE* tf = tfs_open("./test.txt", "w");

    MPI_Barrier(MPI_COMM_WORLD);

    double tstart = MPI_Wtime();
    char* data = malloc(sizeof(char)*DATA_SIZE);
    int i;
    for(i = 0; i < N; i++)
        tfs_write(tf, data, DATA_SIZE, size*DATA_SIZE*i+rank*DATA_SIZE);

    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    if(rank == 0) {
        printf("Total write size: %d MB, elapsed time: %fs\n", size*DATA_SIZE*N/MB, (tend-tstart));
        printf("Bandwidth: %.2f MB/s\n", size*DATA_SIZE*N/MB/(tend-tstart));
    }

    tfs_close(tf);
    MPI_Finalize();
    return 0;
}
