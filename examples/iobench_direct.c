/**
 * To test the impact of different consistency models
 * on performance.
 * We have implemented commitfs and sessionfs on top of
 * TangramFS primitives.
 *
 * This code directly uses APIs from commitfs and sessionfs.
 */
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
#include "commitfs.h"
#include "sessionfs.h"

#define IO_PATTERN_STRIDED    "strided"
#define IO_PATTERN_CONTIGUOUS "contiguous"
#define IO_PATTERN_FPP        "fpp"
#define IO_PATTERN_RANDOM     "random"

#define CONSISTENCY_MODEL_SESSION   "session"
#define CONSISTENCY_MODEL_COMMIT    "commit"
#define CONSISTENCY_MODEL_POSIX     "posix"


#define MB (1024*1024)
#define KB (1024)

#define FILENAME "./test.txt"


// Can be modified by input arguments
static char   write_pattern[20];
static char   read_pattern[20];
static char   consistency_model[20];
static size_t access_size;      // in KB
static int    num_writes, num_reads;
static int    num_writers, num_readers;


MPI_Comm io_comm;   // to store reader comm or writer comm
int io_comm_rank;

int mpi_size;
int mpi_rank;

// Final output result
static double write_tstart, write_tend;
static double read_tstart, read_tend;
static int    write_iops, read_iops;
static double write_bandwidth, read_bandwidth;

tfs_file_t* iobench_file_open(const char* filename) {
    tfs_file_t* tf;
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        tf = sessionfs_open(filename);
    else if(strcmp(consistency_model, CONSISTENCY_MODEL_COMMIT) == 0)
        tf =  commitfs_open(filename);
    return tf;
}
void iobench_file_read(tfs_file_t* tf, void* buf, size_t size) {
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        sessionfs_read(tf, buf, size);
    else if(strcmp(consistency_model, CONSISTENCY_MODEL_COMMIT) == 0)
        commitfs_read(tf, buf, size);
}
void iobench_file_write(tfs_file_t* tf, void* buf, size_t size) {
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        sessionfs_write(tf, buf, size);
    else if(strcmp(consistency_model, CONSISTENCY_MODEL_COMMIT) == 0)
        commitfs_write(tf, buf, size);
}
void iobench_file_close(tfs_file_t* tf) {
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        sessionfs_close(tf);
    else if(strcmp(consistency_model, CONSISTENCY_MODEL_COMMIT) == 0)
        commitfs_close(tf);
}
void iobench_file_seek(tfs_file_t* tf, size_t offset, int whence) {
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        sessionfs_seek(tf, offset, whence);
    else if(strcmp(consistency_model, CONSISTENCY_MODEL_COMMIT) == 0)
        commitfs_seek(tf, offset, whence);
}
void iobench_file_prologue(tfs_file_t* tf, size_t* offsets, size_t* sizes, int num) {
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        sessionfs_session_open(tf, offsets, sizes, num);
}
void iobench_file_epilogue(tfs_file_t* tf) {
    if(strcmp(consistency_model, CONSISTENCY_MODEL_SESSION) == 0)
        sessionfs_session_close(tf);
    else if(strcmp(consistency_model, CONSISTENCY_MODEL_COMMIT) == 0)
        commitfs_commit_file(tf);
}

void write_contiguous() {
    tfs_file_t* tf = iobench_file_open(FILENAME);

    char* data = malloc(sizeof(char)*access_size);
    size_t offset = mpi_rank*access_size*num_writes;
    iobench_file_seek(tf, offset, SEEK_SET);

    size_t* offsets = malloc(sizeof(size_t) * num_writes);
    size_t* sizes   = malloc(sizeof(size_t) * num_writes);
    for(int i = 0; i < num_writes; i++) {
        offsets[i] = offset + i * access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(io_comm);
    write_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_writes);
    for(int i = 0; i < num_writes; i++) {
        iobench_file_write(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(io_comm);
    write_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

void write_strided() {
    tfs_file_t* tf = iobench_file_open(FILENAME);

    size_t offset;
    char* data = malloc(sizeof(char)*access_size);

    size_t* offsets = malloc(sizeof(size_t) * num_writes);
    size_t* sizes   = malloc(sizeof(size_t) * num_writes);
    for(int i = 0; i < num_writes; i++) {
        offsets[i] = num_writers*access_size*i + mpi_rank*access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(io_comm);
    write_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_writes);
    for(int i = 0; i < num_writes; i++) {
        iobench_file_seek(tf, offsets[i], SEEK_SET);
        iobench_file_write(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(io_comm);
    write_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

// file per process
void write_fpp() {
    char fname[256];
    sprintf(fname, "%s.%d", FILENAME, mpi_rank);

    tfs_file_t* tf = iobench_file_open(fname);

    char*  data = malloc(sizeof(char)*access_size);
    size_t offset = 0;
    iobench_file_seek(tf, offset, SEEK_SET);

    MPI_Barrier(io_comm);
    write_tstart = MPI_Wtime();
    //iobench_file_prologue(tf);
    for(int i = 0; i < num_writes; i++) {
        iobench_file_write(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(io_comm);
    write_tend = MPI_Wtime();

    free(data);
    iobench_file_close(tf);
}

void read_contiguous() {
    tfs_file_t* tf = iobench_file_open(FILENAME);

    char* data = malloc(sizeof(char)*access_size);

    int rank = mpi_rank - num_writers;
    size_t offset = rank*access_size*num_reads;
    iobench_file_seek(tf, offset, SEEK_SET);

    size_t* offsets = malloc(sizeof(size_t) * num_reads);
    size_t* sizes   = malloc(sizeof(size_t) * num_reads);
    for(int i = 0; i < num_reads; i++) {
        offsets[i] = offset + i * access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(io_comm);
    read_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_reads);
    for(int i = 0; i < num_reads; i++) {
        iobench_file_read(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(io_comm);
    read_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

void read_strided() {
    tfs_file_t* tf = iobench_file_open(FILENAME);

    char* data = malloc(sizeof(char)*access_size);

    int rank = mpi_rank - num_writers;

    size_t* offsets = malloc(sizeof(size_t) * num_reads);
    size_t* sizes   = malloc(sizeof(size_t) * num_reads);
    for(int i = 0; i < num_reads; i++) {
        offsets[i] = num_readers*access_size*i + rank*access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(io_comm);
    read_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_reads);
    for(int i = 0; i < num_reads; i++) {
        iobench_file_seek(tf, offsets[i], SEEK_SET);
        iobench_file_read(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(io_comm);
    read_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

void read_random() {
    tfs_file_t* tf = iobench_file_open(FILENAME);
    struct stat st;

    char* data = malloc(sizeof(char)*access_size);

    time_t t;
    srand((unsigned) time(&t));
    int num_blocks = num_writes * num_writers;

    size_t* offsets = malloc(sizeof(size_t) * num_writes);
    size_t* sizes   = malloc(sizeof(size_t) * num_writes);
    for(int i = 0; i < num_writes; i++) {
        offsets[i] = (rand() % num_blocks) * access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(io_comm);
    read_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_reads);
    for(int i = 0; i < num_reads; i++) {
        iobench_file_seek(tf, offsets[i], SEEK_SET);
        iobench_file_read(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(io_comm);
    read_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

void init_args() {
    strcpy(write_pattern, "");
    strcpy(read_pattern, "");
    strcpy(consistency_model, CONSISTENCY_MODEL_COMMIT);
    access_size  = 4*KB;
    num_writes  = 10;
    num_readers = 0;
}

void parse_cmd_args(int argc, char* argv[]) {
    int opt;
    while((opt = getopt(argc, argv, ":w:r:m:s:c:n:")) != -1) {
        switch(opt) {
            case 'w':
                strcpy(write_pattern, optarg);
                break;
            case 'r':
                strcpy(read_pattern, optarg);
                break;
            case 'm':
                strcpy(consistency_model, optarg);
                break;
            case 's':
                access_size = atol(optarg) * KB;
                break;
            case 'c':
                num_readers = atoi(optarg);
                break;
            case 'n':
                num_writes = atoi(optarg);
                break;
            case '?':
                printf("Unknown option: %c\n", optopt);
                break;
        }
    }
}

int main(int argc, char* argv[]) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    tfs_init();

    if(mpi_rank == 0) {
        init_args();
        parse_cmd_args(argc, argv);
        num_writers = mpi_size - num_readers;   // if not set, num_readers = 0, all prcocesses are writer.
        if(num_readers > 0)
            num_reads = num_writers * num_writes / num_readers;
        printf("Consistency: %s, Write pattern: %s, Read pattern: %s, Access size: %ldKB, Num writes/reads: %d/%d, Readers: %d\n", consistency_model, write_pattern, read_pattern, access_size/KB, num_writes, num_reads, num_readers);
    }
    MPI_Bcast(&write_pattern,  20, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&read_pattern,   20, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&consistency_model, 20, MPI_BYTE, 0, MPI_COMM_WORLD);
    MPI_Bcast(&num_writes, 1, MPI_INT,  0, MPI_COMM_WORLD);
    MPI_Bcast(&num_reads, 1, MPI_INT,  0, MPI_COMM_WORLD);
    MPI_Bcast(&num_writers, 1, MPI_INT,  0, MPI_COMM_WORLD);
    MPI_Bcast(&num_readers, 1, MPI_INT,  0, MPI_COMM_WORLD);
    MPI_Bcast(&access_size,  1, MPI_LONG, 0, MPI_COMM_WORLD);
    MPI_Comm_split(MPI_COMM_WORLD, mpi_rank<num_writers, 0, &io_comm);
    MPI_Comm_rank(io_comm, &io_comm_rank);

    // Write phase
    MPI_Barrier(MPI_COMM_WORLD);
    if(mpi_rank < num_writers) {
        if(strcmp(write_pattern, IO_PATTERN_CONTIGUOUS) == 0)
            write_contiguous();
        if(strcmp(write_pattern, IO_PATTERN_STRIDED) == 0)
            write_strided();
        if(strcmp(write_pattern, IO_PATTERN_FPP) == 0)
            write_fpp();
    }

    // Read phase
    MPI_Barrier(MPI_COMM_WORLD);
    if(mpi_rank >= num_writers) {
        if(strcmp(read_pattern, IO_PATTERN_CONTIGUOUS) == 0)
            read_contiguous();
        if(strcmp(read_pattern, IO_PATTERN_STRIDED) == 0)
            read_strided();
        if(strcmp(read_pattern, IO_PATTERN_RANDOM) == 0)
            read_random();
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if(io_comm_rank == 0) {
        if(mpi_rank == 0) {
            if(num_readers > 0) {
                MPI_Recv(&read_tstart, 1, MPI_DOUBLE, num_writers, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(&read_tend, 1, MPI_DOUBLE, num_writers, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            }

            write_iops = num_writes * num_writers / (write_tend-write_tstart);
            write_bandwidth = access_size * num_writes / (double)MB * num_writers / (write_tend-write_tstart);

            read_iops = num_reads * num_readers / (read_tend-read_tstart);
            read_bandwidth  = access_size * num_reads / (double)MB * num_readers / (read_tend-read_tstart);

            printf("Write/Read time: %3.3f/%3.3f, Write IOPS: %8d, Bandwidth(MB/s): %.3f\t\tRead IOPS: %d, Bandwidth(MB/s): %.3f\n",
                    (write_tend-write_tstart), (read_tend-read_tstart), write_iops, write_bandwidth, read_iops, read_bandwidth);
        } else {
            if(num_readers > 0) {
                MPI_Send(&read_tstart, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
                MPI_Send(&read_tend, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
            }
        }
    }

    tfs_finalize();
    MPI_Finalize();
    return 0;
}
