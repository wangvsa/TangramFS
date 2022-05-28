#include <stdlib.h>
#include <stdio.h>
#include <execinfo.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include "mpi.h"

#define WRITE_MODE_STRIDED    "strided"
#define WRITE_MODE_CONTIGUOUS "contiguous"
#define READ_MODE_CONTIGUOUS  "contiguous"
#define READ_MODE_RANDOM      "random"

#define MB (1024*1024)
#define KB (1024)

#define FILENAME "./test.txt"


// Can be modified by input arguments
static char   write_mode[20];
static char   read_mode[20];
static size_t access_size;      // in KB
static int    num_accesses;


int mpi_size;
int mpi_rank;

// Final output result
static double write_tstart, write_tend;
static double read_tstart, read_tend;
static int    write_iops, read_iops;
static double write_bandwidth, read_bandwidth;

void write_contiguous() {
    char hostname[128];
    gethostname(hostname, 128);

    FILE* fp = fopen(FILENAME, "wb");

    char* data = malloc(sizeof(char)*access_size);
    size_t offset = mpi_rank*access_size*num_accesses;
    fseek(fp, offset, SEEK_SET);

    MPI_Barrier(MPI_COMM_WORLD);
    write_tstart = MPI_Wtime();
    for(int i = 0; i < num_accesses; i++) {
        fwrite(data, 1, access_size, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    write_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void write_strided() {
    FILE* fp = fopen(FILENAME, "wb");

    size_t offset;
    char* data = malloc(sizeof(char)*access_size);

    MPI_Barrier(MPI_COMM_WORLD);
    write_tstart = MPI_Wtime();

    for(int i = 0; i < num_accesses; i++) {
        size_t offset = mpi_size*access_size*i + mpi_rank*access_size;
        fseek(fp, offset, SEEK_SET);
        fwrite(data, 1, access_size, fp);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    write_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void read_contiguous() {
    FILE* fp = fopen(FILENAME, "rb");

    char* data = malloc(sizeof(char)*access_size);

    size_t offset = mpi_rank*access_size*num_accesses;
    fseek(fp, offset, SEEK_SET);

    MPI_Barrier(MPI_COMM_WORLD);
    read_tstart = MPI_Wtime();
    for(int i = 0; i < num_accesses; i++) {
        fread(data, 1, access_size, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    read_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void read_random() {
    FILE* fp = fopen(FILENAME, "rb");
    struct stat st;

    char* data = malloc(sizeof(char)*access_size);

    time_t t;
    srand((unsigned) time(&t));
    int num_blocks = num_accesses * mpi_size;
    size_t offset;

    MPI_Barrier(MPI_COMM_WORLD);
    read_tstart = MPI_Wtime();
    for(int i = 0; i < num_accesses; i++) {
        offset = (rand() % num_blocks) * access_size;
        fseek(fp, offset, SEEK_SET);
        fread(data, 1, access_size, fp);
    }
    MPI_Barrier(MPI_COMM_WORLD);
    read_tend = MPI_Wtime();

    free(data);
    fclose(fp);
}

void init_args() {
    strcpy(write_mode, "");
    strcpy(read_mode, "");
    access_size  = 4*KB;
    num_accesses = 10;
}

void parse_cmd_args(int argc, char* argv[]) {
    int opt;
    while((opt = getopt(argc, argv, ":w:r:s:n:")) != -1) {
        switch(opt) {
            case 'w':
                strcpy(write_mode, optarg);
                break;
            case 'r':
                strcpy(read_mode, optarg);
                break;
            case 's':
                access_size = atol(optarg) * KB;
                break;
            case 'n':
                num_accesses = atoi(optarg);
                break;
            case '?':
                printf("Unknown option: %c\n", optopt);
                break;
        }
    }
    printf("Write mode: %s, Read mode: %s, Access size: %ldKB, Num accesses: %d\n", write_mode, read_mode, access_size, num_accesses);
}

int main(int argc, char* argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);

    if(mpi_rank == 0) {
        init_args();
        parse_cmd_args(argc, argv);
    }
    MPI_Bcast(&write_mode,  20, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&read_mode,   20, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&num_accesses, 1, MPI_INT,  0, MPI_COMM_WORLD);
    MPI_Bcast(&access_size,  1, MPI_LONG, 0, MPI_COMM_WORLD);

    // Write phase
    MPI_Barrier(MPI_COMM_WORLD);
    if(strcmp(write_mode, WRITE_MODE_CONTIGUOUS) == 0)
        write_contiguous();
    if(strcmp(write_mode, WRITE_MODE_STRIDED) == 0)
        write_strided();

    // Read phase
    MPI_Barrier(MPI_COMM_WORLD);
    if(strcmp(read_mode, READ_MODE_CONTIGUOUS) == 0)
        read_contiguous();
    if(strcmp(read_mode, READ_MODE_RANDOM) == 0)
        read_random();

    MPI_Barrier(MPI_COMM_WORLD);
    if(mpi_rank == 0) {

        write_iops = num_accesses * mpi_size / (write_tend-write_tstart);
        write_bandwidth = access_size / (access_size>=MB?MB:KB) * num_accesses * mpi_size / (write_tend-write_tstart);

        read_iops = num_accesses * mpi_size / (read_tend-read_tstart);
        read_bandwidth  = access_size / (access_size>=MB?MB:KB) * num_accesses * mpi_size / (read_tend-read_tstart);

        printf("Write IOPS: %8d, Bandwidth: %.3f\t\tRead IOPS: %d, Bandwidth: %.3f\n", write_iops, write_bandwidth, read_iops, read_bandwidth);
        fflush(stdout);
    }

    MPI_Finalize();
    return 0;
}
