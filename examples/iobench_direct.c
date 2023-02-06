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
#include <mpi.h>
#include "commitfs.h"
#include "sessionfs.h"

#define IO_PATTERN_STRIDED    "strided"
#define IO_PATTERN_CONTIGUOUS "contiguous"
#define IO_PATTERN_FPP        "fpp"
#define IO_PATTERN_RANDOM     "random"
#define IO_PATTERN_SCR        "scr"

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
int global_comm_size;
int global_comm_rank;
int mpi_num_nodes;
int mpi_ppn;        //  number of processes per node

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

void write_contiguous_core(const char* filename, size_t start_offset, MPI_Comm comm) {
    tfs_file_t* tf = iobench_file_open(filename);

    char* data = malloc(sizeof(char)*access_size);
    size_t offset = start_offset;
    iobench_file_seek(tf, offset, SEEK_SET);

    size_t* offsets = malloc(sizeof(size_t) * num_writes);
    size_t* sizes   = malloc(sizeof(size_t) * num_writes);
    for(int i = 0; i < num_writes; i++) {
        offsets[i] = offset + i * access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(comm);
    write_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_writes);
    for(int i = 0; i < num_writes; i++) {
        iobench_file_write(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(comm);
    write_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

void write_contiguous() {
    size_t start_offset = global_comm_rank*access_size*num_writes;
    write_contiguous_core(FILENAME, start_offset, io_comm);
}

/*
 * file per process, same pattern as of contiguous
 * just to different files
 */
void write_fpp() {
    char fname[256];
    sprintf(fname, "%s.%d", FILENAME, global_comm_rank);

    size_t start_offset = 0;
    write_contiguous_core(fname, start_offset, io_comm);
}

void write_strided() {
    tfs_file_t* tf = iobench_file_open(FILENAME);

    size_t offset;
    char* data = malloc(sizeof(char)*access_size);

    size_t* offsets = malloc(sizeof(size_t) * num_writes);
    size_t* sizes   = malloc(sizeof(size_t) * num_writes);
    for(int i = 0; i < num_writes; i++) {
        offsets[i] = num_writers*access_size*i + global_comm_rank*access_size;
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


/* Simulate SCR checkpoint I/O behavior
 * Use Partner redundancy schema
 * https://scr.readthedocs.io/en/latest/users/overview.html#redundancy-schemes
 *
 * Run jobs on n+1 nodes, where the extra one node is a spare node.
 * The actual computation/checkpointing is done by n nodes.
 * Each node i checkppoints its own data and the data from a partner node (e.g., node i-1).
 * i.e., 2x space overhead.
 * If one of the n nodes failed, we can recover thanks to the spare node.
 */
typedef struct haccio_data {
    float*   xx;
    float*   yy;
    float*   zz;
    float*   vx;
    float*   vy;
    float*   vz;
    float*   phi;
    int64_t* pid;
    int16_t* mask;
    int num_particles;
} haccio_data_t;

void haccio_init_data(haccio_data_t* data, int num_particles) {
    data->xx   = malloc(sizeof(float) * num_particles);
    data->yy   = malloc(sizeof(float) * num_particles);
    data->zz   = malloc(sizeof(float) * num_particles);
    data->vx   = malloc(sizeof(float) * num_particles);
    data->vy   = malloc(sizeof(float) * num_particles);
    data->vz   = malloc(sizeof(float) * num_particles);
    data->phi  = malloc(sizeof(float) * num_particles);
    data->pid  = malloc(sizeof(int64_t) * num_particles);
    data->mask = malloc(sizeof(int16_t) * num_particles);
    data->num_particles = num_particles;
}
void haccio_free_data(haccio_data_t* data) {
    free(data->xx);
    free(data->yy);
    free(data->zz);
    free(data->vx);
    free(data->vy);
    free(data->vz);
    free(data->phi);
    free(data->pid);
    free(data->mask);
}

void haccio_checkpoint(const char* filename, haccio_data_t* data, MPI_Comm comm) {
    tfs_file_t* tf = iobench_file_open(filename);

    // 9 variables in total
    const int num_variables = 9;
    const int num_particles = data->num_particles;

    size_t* offsets = malloc(sizeof(size_t) * num_variables);
    size_t* sizes   = malloc(sizeof(size_t) * num_variables);
    for(int i = 0; i< 7; i++)
        sizes[i] = sizeof(float) * num_particles;
    sizes[7] = sizeof(int64_t) * num_particles;
    sizes[8] = sizeof(int16_t) * num_particles;
    offsets[0] = 0;
    for(int i = 1; i < num_variables; i++)
        offsets[i] = offsets[i-1] + sizes[i-1];

    MPI_Barrier(comm);
    write_tstart = MPI_Wtime();
    iobench_file_seek(tf, 0, SEEK_SET);
    iobench_file_prologue(tf, offsets, sizes, num_variables);

    iobench_file_write(tf, data->xx, sizes[0]);
    iobench_file_write(tf, data->yy, sizes[1]);
    iobench_file_write(tf, data->zz, sizes[2]);
    iobench_file_write(tf, data->vx, sizes[3]);
    iobench_file_write(tf, data->vy, sizes[4]);
    iobench_file_write(tf, data->vz, sizes[5]);
    iobench_file_write(tf, data->phi, sizes[6]);
    iobench_file_write(tf, data->pid, sizes[7]);
    iobench_file_write(tf, data->mask, sizes[8]);

    iobench_file_epilogue(tf);
    MPI_Barrier(comm);
    write_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    iobench_file_close(tf);
}

void haccio_restart(const char* filename, haccio_data_t* data, MPI_Comm comm) {
    tfs_file_t* tf = iobench_file_open(filename);

    // 9 variables in total
    const int num_variables = 9;
    const int num_particles = data->num_particles;


    size_t* offsets = malloc(sizeof(size_t) * num_variables);
    size_t* sizes   = malloc(sizeof(size_t) * num_variables);
    for(int i = 0; i< 7; i++)
        sizes[i] = sizeof(float) * num_particles;
    sizes[7] = sizeof(int64_t) * num_particles;
    sizes[8] = sizeof(int16_t) * num_particles;
    offsets[0] = 0;
    for(int i = 1; i < num_variables; i++)
        offsets[i] = offsets[i-1] + sizes[i-1];

    MPI_Barrier(comm);
    read_tstart = MPI_Wtime();
    iobench_file_seek(tf, 0, SEEK_SET);
    iobench_file_prologue(tf, offsets, sizes, num_variables);

    iobench_file_read(tf, data->xx, sizes[0]);
    iobench_file_read(tf, data->yy, sizes[1]);
    iobench_file_read(tf, data->zz, sizes[2]);
    iobench_file_read(tf, data->vx, sizes[3]);
    iobench_file_read(tf, data->vy, sizes[4]);
    iobench_file_read(tf, data->vz, sizes[5]);
    iobench_file_read(tf, data->phi, sizes[6]);
    iobench_file_read(tf, data->pid, sizes[7]);
    iobench_file_read(tf, data->mask, sizes[8]);

    iobench_file_epilogue(tf);
    MPI_Barrier(comm);
    read_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    iobench_file_close(tf);
}

void write_haccio_scr(int num_particles) {
    double t1, t2, t3, t4;

    MPI_Comm scr_comm;
    int spare_node = 0;
    if(global_comm_rank >= (global_comm_size-mpi_ppn))
        spare_node = 1;
    MPI_Comm_split(MPI_COMM_WORLD, spare_node, 0, &scr_comm);

    // Checkpoint myself's data
    char fname[256];
    if(!spare_node) {
        sprintf(fname, "%s.%d.self", FILENAME, global_comm_rank);
        haccio_data_t self_data;
        haccio_init_data(&self_data, num_particles);
        haccio_checkpoint(fname, &self_data, scr_comm);
        haccio_free_data(&self_data);
        t1 = write_tstart;
        t2 = write_tend;
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // Recv partner's data
    //char* data = malloc(sizeof(char)*access_size*num_writes);
    //MPI_Sendrecv();
    //free(data);

    // Checkpoint partner's data
    if(!spare_node) {
        sprintf(fname, "%s.%d.partner", FILENAME, global_comm_rank);
        haccio_data_t partner_data;
        haccio_init_data(&partner_data, num_particles);
        haccio_checkpoint(fname, &partner_data, scr_comm);
        haccio_free_data(&partner_data);
        t3 = write_tstart;
        t4 = write_tend;
        write_tstart = t3 - (t2 - t1);
    }
    MPI_Comm_free(&scr_comm);
    MPI_Barrier(MPI_COMM_WORLD);
}

void read_haccio_scr(int num_particles) {
    // Assume node 1 failed
    int failed_or_spare = 0;
    if((global_comm_rank >= mpi_ppn) && (global_comm_rank < 2*mpi_ppn))
        failed_or_spare = 1;
    if(global_comm_rank >= (global_comm_size-mpi_ppn))
        failed_or_spare = 1;
    
    MPI_Comm scr_comm;
    MPI_Comm_split(MPI_COMM_WORLD, failed_or_spare, 0, &scr_comm);

    // Recover
    if(!failed_or_spare) {
        char fname[256];
        sprintf(fname, "%s.%d.partner", FILENAME, global_comm_rank);
        printf("read %s, %d %d\n", fname, global_comm_rank, mpi_ppn);

        haccio_data_t data;
        haccio_init_data(&data, num_particles);
        haccio_restart(fname, &data, scr_comm);
        haccio_free_data(&data);
    }

    // Node n sends data to node n+1
    //MPI_Sendrecv();
    MPI_Comm_free(&scr_comm);
    MPI_Barrier(MPI_COMM_WORLD);
}



void read_contiguous_core(const char* filename, size_t start_offset, MPI_Comm comm) {
    tfs_file_t* tf = iobench_file_open(filename);

    char* data = malloc(sizeof(char)*access_size);

    size_t offset = start_offset;
    iobench_file_seek(tf, offset, SEEK_SET);

    size_t* offsets = malloc(sizeof(size_t) * num_reads);
    size_t* sizes   = malloc(sizeof(size_t) * num_reads);
    for(int i = 0; i < num_reads; i++) {
        offsets[i] = offset + i * access_size;
        sizes[i]   = access_size;
    }

    MPI_Barrier(comm);
    read_tstart = MPI_Wtime();
    iobench_file_prologue(tf, offsets, sizes, num_reads);
    for(int i = 0; i < num_reads; i++) {
        iobench_file_read(tf, data, access_size);
    }
    iobench_file_epilogue(tf);
    MPI_Barrier(comm);
    read_tend = MPI_Wtime();

    free(offsets);
    free(sizes);
    free(data);
    iobench_file_close(tf);
}

void read_contiguous() {
    size_t start_offset = io_comm_rank*access_size*num_reads;
    read_contiguous_core(FILENAME, start_offset, io_comm);
}


void read_fpp() {
    char fname[256];
    sprintf(fname, "%s.%d", FILENAME, global_comm_rank);
    size_t start_offset = 0;
    read_contiguous_core(fname, start_offset, io_comm);
}

void read_strided() {
    tfs_file_t* tf = iobench_file_open(FILENAME);

    char* data = malloc(sizeof(char)*access_size);

    size_t* offsets = malloc(sizeof(size_t) * num_reads);
    size_t* sizes   = malloc(sizeof(size_t) * num_reads);
    for(int i = 0; i < num_reads; i++) {
        offsets[i] = num_readers*access_size*i + io_comm_rank*access_size;
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


void shuffle(unsigned int seed, int *array, size_t n)
{
    if (n > 1) {
        size_t i;
        for (i = 0; i < n - 1; i++) {
            size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            int t = array[j];
            array[j] = array[i];
            array[i] = t;
        }
    }
}

/**
 * Simulate ML I/O pattern
 * Deep neural networks are almost always trained with variants of
 * mini-batch SGD. Training consists of many epochs; each epoch
 * is a complete pass over the training dataset in a different, random
 * order. The samples that make up a given mini-batch are randomly
 * selected without replacement from the entire training dataset. This
 * is typically implemented by assigning an index to each sample,
 * randomly shuffling the indices each epoch, and then partitioning
 * the shuffled indices into mini-batches.
 * We assume a data-parallel regime, where a mini-batch
 * is partitioned among workers. (Worker === MPI Rank === GPU)
 */
void read_random_ml() {

    tangram_assert(num_writes  == num_reads);
    tangram_assert(num_writers == num_readers);

    // Each worker (MPI Rank) reads num_reads samples
    int total_samples = num_reads * num_readers;
    int* sample_indices = malloc(sizeof(int)*total_samples);
    for(int i = 0; i < total_samples; i++)
        sample_indices[i] = i;

    // Pick a seed and broadcast to everyone
    time_t t;
    unsigned int seed;
    if(global_comm_rank == 0) {
        seed = time(&t);
    }
    MPI_Bcast(&seed, sizeof(seed), MPI_BYTE, 0, MPI_COMM_WORLD);

    // Use the received seed to shuffle indices
    shuffle(seed, sample_indices, total_samples);

    tfs_file_t* tf = iobench_file_open(FILENAME);

    char* data = malloc(sizeof(char)*access_size);

    size_t* offsets = malloc(sizeof(size_t) * num_reads);
    size_t* sizes   = malloc(sizeof(size_t) * num_reads);
    for(int i = 0; i < num_reads; i++) {
        offsets[i] = sample_indices[i] * access_size;
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
    free(sample_indices);
    iobench_file_close(tf);
}

void init_args() {
    strcpy(write_pattern, "");
    strcpy(read_pattern, "");
    strcpy(consistency_model, CONSISTENCY_MODEL_COMMIT);
    access_size  = 4*KB;
    num_writes  = 10;
    num_writers = global_comm_size;
    num_readers = 0;
}

void parse_cmd_args(int argc, char* argv[]) {
    int opt;
    while((opt = getopt(argc, argv, ":w:r:m:s:p:c:n:")) != -1) {
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
            case 'p':
                num_writers = atoi(optarg);
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
    MPI_Comm_size(MPI_COMM_WORLD, &global_comm_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &global_comm_rank);
    tfs_init();

    if(global_comm_rank == 0) {
        init_args();
        parse_cmd_args(argc, argv);
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

    // Calculate number of nodes and processes per node
    int tmp_rank, is_rank0;
    MPI_Comm shmcomm;
    MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &shmcomm);
    MPI_Comm_rank(shmcomm, &tmp_rank);
    is_rank0 = (tmp_rank == 0) ? 1 : 0;
    MPI_Allreduce(&is_rank0, &mpi_num_nodes, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    MPI_Comm_free(&shmcomm);
    mpi_ppn = global_comm_size / mpi_num_nodes;
    if(global_comm_rank == 0)
        printf("\nnum nodes: %d, ppn: %d\n", mpi_num_nodes, mpi_ppn);

    // Split to two communicators, writer group and reader group
    MPI_Comm_split(MPI_COMM_WORLD, global_comm_rank<num_writers, 0, &io_comm);
    MPI_Comm_rank(io_comm, &io_comm_rank);



    // We allow only two scenarios:
    // 1. num_writers + num_readers = global_comm_size
    // 2. num_writers = global_comm_size
    int reader_start_rank = num_writers % global_comm_size;

    // Write phase
    MPI_Barrier(MPI_COMM_WORLD);
    if(global_comm_rank < num_writers) {
        if(strcmp(write_pattern, IO_PATTERN_CONTIGUOUS) == 0)
            write_contiguous();
        if(strcmp(write_pattern, IO_PATTERN_STRIDED) == 0)
            write_strided();
        if(strcmp(write_pattern, IO_PATTERN_FPP) == 0)
            write_fpp();
        if(strcmp(write_pattern, IO_PATTERN_SCR) == 0)
            write_haccio_scr(num_writes);
    }

    // Read phase
    MPI_Barrier(MPI_COMM_WORLD);
    if(global_comm_rank >= reader_start_rank && io_comm_rank < num_readers) {
        if(strcmp(read_pattern, IO_PATTERN_CONTIGUOUS) == 0)
            read_contiguous();
        if(strcmp(read_pattern, IO_PATTERN_STRIDED) == 0)
            read_strided();
        if(strcmp(read_pattern, IO_PATTERN_RANDOM) == 0)
            read_random_ml();
        if(strcmp(read_pattern, IO_PATTERN_SCR) == 0)
            read_haccio_scr(num_writes);
    }

    MPI_Barrier(MPI_COMM_WORLD);

    // reader rank 0 sends read time to writer rank 0
    if(reader_start_rank != 0) {
        if(global_comm_rank == 0) {
            MPI_Recv(&read_tstart, 1, MPI_DOUBLE, reader_start_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            MPI_Recv(&read_tend, 1, MPI_DOUBLE, reader_start_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        if(global_comm_rank == reader_start_rank ) {
            MPI_Send(&read_tstart, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
            MPI_Send(&read_tend, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
        }
    }


    if(global_comm_rank == 0) {

        if(strcmp(write_pattern, IO_PATTERN_SCR) == 0) {
            int num_particles = num_writes;
            write_bandwidth = 2*num_particles*(mpi_num_nodes-1)*mpi_ppn/(double)MB*38/(write_tend-write_tstart);
            read_bandwidth  = 1*num_particles*(mpi_num_nodes-2)*mpi_ppn/(double)MB*38/(read_tend-read_tstart);
            printf("SCR Aggregated write time: %.4f, %.3f \tMB/s\t\t read time: %.4f, %.3f \tMB/s\n", (write_tend-write_tstart), write_bandwidth, (read_tend-read_tstart), read_bandwidth);

        } else {

            write_iops = num_writes * num_writers / (write_tend-write_tstart);
            write_bandwidth = access_size * num_writes / (double)MB * num_writers / (write_tend-write_tstart);

            read_iops = num_reads * num_readers / (read_tend-read_tstart);
            read_bandwidth  = access_size * num_reads / (double)MB * num_readers / (read_tend-read_tstart);

            printf("Write/Read time: %3.3f/%3.3f, Write IOPS: %8d, Bandwidth(MB/s): %.3f\t\tRead IOPS: %8d, Bandwidth(MB/s): %.3f\n",
                    (write_tend-write_tstart), (read_tend-read_tstart), write_iops, write_bandwidth, read_iops, read_bandwidth);
        }
        fflush(stdout);
    }

    tfs_finalize();
    MPI_Comm_free(&io_comm);
    MPI_Finalize();
    return 0;
}
