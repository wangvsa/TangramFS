#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mpi.h"
#include "tangramfs.h"

void tfs_init(const char* mount_point) {
    MPI_Comm_rank(MPI_COMM_WORLD, &g_mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &g_mpi_size);
    strcpy(g_mount_point, mount_point);
}

void tfs_finalize() {
}

TFILE* tfs_open(const char* pathname, const char* mode) {
    TFILE* tf = malloc(sizeof(TFILE));
    tf->it = malloc(sizeof(IntervalTree));
    tfs_it_init(tf->it);

    char filename[256];
    sprintf(filename, "%s_tfs_tmpfile.%d", g_mount_point, g_mpi_rank);
    tf->file = fopen(filename, mode);
    return tf;
}

void tfs_write(TFILE* tf, void* buf, size_t count, size_t offset) {
    tfs_it_insert(tf->it, offset, count, ftell(tf->file));
    fwrite(buf, 1, count, tf->file);
}

void tfs_read(TFILE* tf, void* buf, size_t count, size_t offset) {
    fread(buf, 1, count, tf->file);
}

void tfs_close(TFILE* tf) {
    fclose(tf->file);
    tfs_it_destroy(tf->it);

    free(tf);
    tf = NULL;
}

void tfs_commit() {
}

void tfs_persist() {
}
