#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include "mpi.h"

#define MB (1024*1024)

static int DATA_SIZE = 16*MB;
static int N = 5;

int size, rank;

void test_passive_mode() {
    MPI_Barrier(MPI_COMM_WORLD);

    int fd = open("./chen/test.txt", O_RDWR);

    char* data = malloc(sizeof(char)*DATA_SIZE);
    double tstart = MPI_Wtime();
    int i;
    for(i = 0; i < N; i++) {
        write(fd, data, DATA_SIZE);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    if(rank == 0) {
        printf("Total write size: %d MB, elapsed time: %fs\n", DATA_SIZE/MB*N*size, (tend-tstart));
        printf("Bandwidth: %.2f MB/s\n", DATA_SIZE/MB*size*N/(tend-tstart));
    }

    tstart = MPI_Wtime();
    for(i = 0; i < N; i++)
        read(fd, data, DATA_SIZE);

    MPI_Barrier(MPI_COMM_WORLD);
    tend = MPI_Wtime();

    if(rank == 0) {
        printf("Total read size: %d MB, elapsed time: %fs\n", DATA_SIZE/MB*N*size, (tend-tstart));
        printf("Bandwidth: %.2f MB/s\n", DATA_SIZE/MB*size*N/(tend-tstart));
    }

    close(fd);
}


int main(int argc, char* argv[]) {
    int provided;

    // Have to use MPI_THREAD_MULTIPLE for Mercury+pthread to work
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    test_passive_mode();

    MPI_Finalize();
    return 0;
}
