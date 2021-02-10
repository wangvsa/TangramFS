#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mpi.h"
#include "tangramfs.h"

typedef struct TFSInfo_t {
    int mpi_rank;
    int mpi_size;
    MPI_Comm mpi_comm;
    char buffer_dir[128];
    char persist_dir[128];
} TFSInfo;

static TFSInfo tfs;

void tfs_init(const char* persist_dir, const char* buffer_dir) {
    MPI_Comm_dup(MPI_COMM_WORLD, &tfs.mpi_comm);
    MPI_Comm_rank(tfs.mpi_comm, &tfs.mpi_rank);
    MPI_Comm_size(tfs.mpi_comm, &tfs.mpi_size);

    strcpy(tfs.persist_dir, persist_dir);
    strcpy(tfs.buffer_dir, buffer_dir);
}

void tfs_finalize() {
    MPI_Comm_free(&tfs.mpi_comm);
}

TFILE* tfs_open(const char* pathname, const char* mode) {
    TFILE* tf = malloc(sizeof(TFILE));
    tf->it = malloc(sizeof(IntervalTree));
    tfs_it_init(tf->it);

    char filename[256];
    sprintf(filename, "%s/_tfs_tmpfile.%d", tfs.buffer_dir, tfs.mpi_rank);
    tf->local_file = fopen(filename, mode);
    return tf;
}

void tfs_write(TFILE* tf, void* buf, size_t count, size_t offset) {
    tfs_it_insert(tf->it, offset, count, ftell(tf->local_file));
    fwrite(buf, 1, count, tf->local_file);
    fsync(fileno(tf->local_file));
}

void tfs_read(TFILE* tf, void* buf, size_t count, size_t offset) {
    size_t local_offset;
    bool found = tfs_it_query(tf->it, offset, count, &local_offset);

    if(found)
        fread(buf, 1, count, tf->local_file);
    else
        printf("Copy not exist. Not handled yet\n");
}

void tfs_read_lazy(TFILE* tf, void* buf, size_t count, size_t offset) {
    fread(buf, 1, count, tf->local_file);
}

void tfs_notify() {
}

void tfs_close(TFILE* tf) {
    fclose(tf->local_file);
    tfs_it_destroy(tf->it);

    free(tf);
    tf = NULL;
}


