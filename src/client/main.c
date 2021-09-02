#include <stdlib.h>
#include <stdio.h>

#include <execinfo.h>
#include <signal.h>
#include <unistd.h>

#include "mpi.h"
#include "tangramfs.h"


#define MB (1024*1024)

static size_t DATA_SIZE = 2*MB;
static int N = 1;

int mpi_size, mpi_rank;
double write_bandwidth, read_bandwidth;

void write_phase() {
    tfs_file_t* tf = tfs_open("./test.txt", "w+");

    char* data = malloc(sizeof(char)*DATA_SIZE);
    size_t offset = mpi_rank*DATA_SIZE*N;

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();

    tfs_seek(tf, offset, SEEK_SET);
    for(int i = 0; i < N; i++) {
        tfs_write(tf, data, DATA_SIZE);
        tfs_post(tf, offset+i*DATA_SIZE, DATA_SIZE);
    }
    // Post all writes in one RPC
    //tfs_post_all(tf);
    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    write_bandwidth = 1.0*DATA_SIZE*mpi_size*N/MB/(tend-tstart);

    free(data);
    tfs_close(tf);
}

void read_phase() {
    tfs_file_t* tf = tfs_open("./test.txt", "r+");
    MPI_Barrier(MPI_COMM_WORLD);        // make sure everyone has opened the file first

    char* data = malloc(sizeof(char)*DATA_SIZE);

    // read neighbor rank's data
    int neighbor_rank = (mpi_rank + 8) % mpi_size;
    size_t offset = neighbor_rank * DATA_SIZE * N;
    tfs_seek(tf, offset, SEEK_SET);

    double tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        //tfs_read_lazy(tf, data, DATA_SIZE);
        tfs_read(tf, data, DATA_SIZE);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    read_bandwidth = 1.0*DATA_SIZE/MB*mpi_size*N/(tend-tstart);

    free(data);
    tfs_close(tf);
}

/*
void error_handler(int sig) {
   void *array[30];
   size_t size;
   size = backtrace(array, 30); //get the void pointers for all of the entries
   backtrace_symbols_fd(array, size, STDERR_FILENO);
   exit(1);
}
*/

int main(int argc, char* argv[]) {
    //signal(SIGSEGV, error_handler);

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    tfs_init();

    for(int i = 0; i < 1; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        write_phase();
    }

    for(int i = 0; i < 1; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        read_phase();
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if(mpi_rank == 0) {
        printf("Write Bandwidth: %.2f MB/s\t\tRead Bandwidth: %.2f\n", write_bandwidth, read_bandwidth);
    }

    tfs_finalize();
    MPI_Finalize();
    return 0;
}
