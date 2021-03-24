#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"
#include "tangramfs.h"

#define MB (1024*1024)

static int DATA_SIZE = 4*MB;
static int N = 16;


int size, rank, provided;


int main(int argc, char* argv[]) {
    // Have to use MPI_THREAD_MULTIPLE for Mercury+pthread to work
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    //tfs_init("./", "/l/ssd");
    tfs_init("./chen/", "/tmp");

    TFS_File* tf = tfs_open("./test.txt");


    char* data = malloc(sizeof(char)*DATA_SIZE);
    size_t offset = rank*DATA_SIZE*N;

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();

    int i;
    tfs_seek(tf, offset, SEEK_SET);
    for(i = 0; i < N; i++)
        tfs_write(tf, data, DATA_SIZE);
    // Post all writes in one RPC
    tfs_post(tf, offset, N*DATA_SIZE);
    //tfs_post_all(tf);
    double tend = MPI_Wtime();

    double min_tstart, max_tend;
    MPI_Reduce(&tstart, &min_tstart, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&tend, &max_tend, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);

    if(rank == 0) {
        printf("Total write size: %d MB, elapsed time: %fs\n", DATA_SIZE/MB*N*size, (max_tend-min_tstart));
        printf("Bandwidth: %.2f MB/s\n", DATA_SIZE/MB*size*N/(max_tend-min_tstart));
    }

    MPI_Barrier(MPI_COMM_WORLD);
    // read neighbor rank's data
    int neighbor_rank = (rank + 1) % size;
    offset = neighbor_rank * DATA_SIZE * N;
    tfs_seek(tf, offset, SEEK_SET);
    tstart = MPI_Wtime();
    for(i = 0; i < N; i++)
        tfs_read_lazy(tf, data, DATA_SIZE);


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
