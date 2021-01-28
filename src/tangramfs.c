#include <stdio.h>
#include <stdlib.h>
#include "mpi.h"
#include "tangramfs.h"

void tfs_init() {
    MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_size);
}

void tfs_finalize() {
}


TFILE* tfs_open(const char* pathname, const char* mode) {
    TFILE* tf = malloc(sizeof(TFILE));
    char filename[256];
    sprintf(filename, "/tmp/tangram_tmp_file.%d", g_mpi_rank);
    tf->file = fopen(filename, mode);
    return tf;
}

void tfs_write(TFILE* tf, void* buf, size_t count, size_t offset) {
    fwrite(buf, 1, count, tf->file);
}

void tfs_read(TFILE* tf, void* buf, size_t count, size_t offset) {
    fread(buf, 1, count, tf->file);
}

void tfs_close(TFILE* tf) {
    fclose(tf->file);
    free(tf);
    tf = NULL;
}

void tfs_notify() {
}

