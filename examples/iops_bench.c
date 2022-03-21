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

static size_t DATA_SIZE = 4*KB;
static int N = 1000;

int mpi_size, mpi_rank;
int write_iops, read_iops;

void write_nonstrided() {
    FILE* fp = fopen(FILENAME, "wb");

    char* data = malloc(sizeof(char)*DATA_SIZE);
    size_t offset = mpi_rank*DATA_SIZE*N;
    fseek(fp, offset, SEEK_SET);

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        fwrite(data, 1, DATA_SIZE, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    write_iops = N / (tend-tstart);

    free(data);
    fclose(fp);
}

void write_strided() {
    FILE* fp = fopen(FILENAME, "wb");

    size_t offset;
    char* data = malloc(sizeof(char)*DATA_SIZE);

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();

    for(int i = 0; i < N; i++) {
        size_t offset = mpi_size*DATA_SIZE*i + mpi_rank*DATA_SIZE;
        fseek(fp, offset, SEEK_SET);
        fwrite(data, 1, DATA_SIZE, fp);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    write_iops = N / (tend-tstart);
    free(data);
    fclose(fp);
}

void read_sequential() {
    FILE* fp = fopen(FILENAME, "rb");

    char* data = malloc(sizeof(char)*DATA_SIZE);

    size_t offset = mpi_rank*DATA_SIZE*N;
    fseek(fp, offset, SEEK_SET);

    MPI_Barrier(MPI_COMM_WORLD);
    double tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        fwrite(data, 1, DATA_SIZE, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    read_iops = N / (tend-tstart);

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
    double tstart = MPI_Wtime();
    for(int i = 0; i < N; i++) {
        offset = (rand() % num_blocks) * DATA_SIZE;
        fseek(fp, offset, SEEK_SET);
        fread(data, 1, DATA_SIZE, fp);
        //if(i%50 == 0)
        //    printf("%d/%d\n", i, N);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    double tend = MPI_Wtime();

    read_iops = N / (tend-tstart);

    free(data);
}

int main(int argc, char* argv[]) {

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    for(int i = 0; i < 1; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        //write_nonstrided();
        write_strided();
    }

    for(int i = 0; i < 1; i++) {
        MPI_Barrier(MPI_COMM_WORLD);
        //read_sequential();
        read_random();
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if(mpi_rank == 0) {
        printf("Write IOPS: %d\t\tRead IOPS: %d\n", write_iops, read_iops);
    }

    MPI_Finalize();
    return 0;
}