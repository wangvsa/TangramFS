#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include "mpi.h"

#define MB (1024*1024)
#define KB (1024)

#define FILENAME "./test.txt"

// Can be modified by input arguments
static size_t DATA_SIZE = 4*MB;
static int    N = 10;

int mpi_size, mpi_rank;

// Final output result
static double write_tstart, write_tend;
static double read_tstart, read_tend;
static int    write_iops, read_iops;
static double write_bandwidth, read_bandwidth;

void write_contiguous() {
    char hostname[128];
    gethostname(hostname, 128);

    FILE* fp = fopen(FILENAME, "wb");

    char* data = malloc(sizeof(char)*DATA_SIZE);
    size_t offset = mpi_rank*DATA_SIZE*N;
    fseek(fp, offset, SEEK_SET);

    MPI_Barrier(MPI_COMM_WORLD);
    write_tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        fwrite(data, 1, DATA_SIZE, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    write_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void write_strided() {
    FILE* fp = fopen(FILENAME, "wb");

    size_t offset;
    char* data = malloc(sizeof(char)*DATA_SIZE);

    MPI_Barrier(MPI_COMM_WORLD);
    write_tstart = MPI_Wtime();

    for(int i = 0; i < N; i++) {
        size_t offset = mpi_size*DATA_SIZE*i + mpi_rank*DATA_SIZE;
        fseek(fp, offset, SEEK_SET);
        fwrite(data, 1, DATA_SIZE, fp);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    write_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void read_contiguous() {
    FILE* fp = fopen(FILENAME, "rb");

    char* data = malloc(sizeof(char)*DATA_SIZE);

    size_t offset = mpi_rank*DATA_SIZE*N;
    fseek(fp, offset, SEEK_SET);

    MPI_Barrier(MPI_COMM_WORLD);
    read_tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        fread(data, 1, DATA_SIZE, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    read_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void read_random() {
    FILE* fp = fopen(FILENAME, "rb");
    struct stat st;

    char* data = malloc(sizeof(char)*DATA_SIZE);

    time_t t;
    srand((unsigned) time(&t));
    int num_blocks = N * mpi_size;
    size_t offset;

    MPI_Barrier(MPI_COMM_WORLD);
    read_tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        offset = (rand() % num_blocks) * DATA_SIZE;
        fseek(fp, offset, SEEK_SET);
        fread(data, 1, DATA_SIZE, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    read_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

int main(int argc, char* argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    if(mpi_rank == 0) {
        if(argc == 3) {
            N = atoi(argv[1]);
            DATA_SIZE = atoi(argv[2]) * KB;
        }
    }
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&DATA_SIZE, 1, MPI_LONG, 0, MPI_COMM_WORLD);

    for(int i = 0; i < 1; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        //write_contiguous();
        write_strided();
    }

    for(int i = 0; i < 1; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        read_contiguous();
        //read_random();
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if(mpi_rank == 0) {

        write_iops = N * mpi_size / (write_tend-write_tstart);
        write_bandwidth = DATA_SIZE / MB * N * mpi_size / (write_tend-write_tstart);

        read_iops = N * mpi_size / (read_tend-read_tstart);
        read_bandwidth = DATA_SIZE / MB * N * mpi_size / (read_tend-read_tstart);

        printf("Write IOPS: %8d, Bandwidth: %.3f\t\tRead IOPS: %d, Bandwidth: %.3f\n", write_iops, write_bandwidth, read_iops, read_bandwidth);
        fflush(stdout);
    }

    MPI_Finalize();
    return 0;
}
