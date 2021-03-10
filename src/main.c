#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"
#include "tangramfs.h"

#define MB (1024*1024)

static int DATA_SIZE = 16*MB;
static int N = 5;


int size, rank, provided;




int main(int argc, char* argv[]) {
    // Have to use MPI_THREAD_MULTIPLE for Mercury+pthread to work
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //tfs_init("./", "/l/ssd");
    tfs_init("./", "/tmp");

    TFS_File* tf = tfs_open("./test.txt", "w");

    MPI_Barrier(MPI_COMM_WORLD);


    char* data = malloc(sizeof(char)*DATA_SIZE);
    double tstart = MPI_Wtime();
    int i;
    for(i = 0; i < N; i++) {
        tfs_write(tf, data, DATA_SIZE, size*DATA_SIZE*i+rank*DATA_SIZE);
        tfs_notify(tf, size*DATA_SIZE*i+rank*DATA_SIZE, DATA_SIZE);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    if(rank == 0) {
        printf("Total write size: %d MB, elapsed time: %fs\n", DATA_SIZE/MB*N*size, (tend-tstart));
        printf("Bandwidth: %.2f MB/s\n", DATA_SIZE/MB*size*N/(tend-tstart));
    }


    tstart = MPI_Wtime();
    for(i = 0; i < N; i++)
        tfs_read_lazy(tf, data, DATA_SIZE, size*DATA_SIZE*i+rank*DATA_SIZE);

    MPI_Barrier(MPI_COMM_WORLD);
    tend = MPI_Wtime();

    if(rank == 0) {
        printf("Total read size: %d MB, elapsed time: %fs\n", DATA_SIZE/MB*N*size, (tend-tstart));
        printf("Bandwidth: %.2f MB/s\n", DATA_SIZE/MB*size*N/(tend-tstart));
    }

    tfs_close(tf);
    tfs_finalize();
    MPI_Finalize();
    return 0;
}
