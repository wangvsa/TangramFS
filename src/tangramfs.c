#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "mpi.h"
#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-meta.h"

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

    if(tfs.mpi_rank == 0) {
        tangram_meta_server_start();
    } else {
        tangram_meta_client_start();
        tangram_meta_issue_rpc();
    }
}

void tfs_finalize() {
    if(tfs.mpi_rank == 0)
        tangram_meta_server_stop();
    else
        tangram_meta_client_stop();
    MPI_Comm_free(&tfs.mpi_comm);
}

TFILE* tfs_open(const char* pathname, const char* mode) {
    TFILE* tf = tangram_malloc(sizeof(TFILE));
    tf->it = tangram_malloc(sizeof(IntervalTree));
    tangram_it_init(tf->it);

    char filename[256];
    sprintf(filename, "%s/_tfs_tmpfile.%d", tfs.buffer_dir, tfs.mpi_rank);
    tf->local_file = fopen(filename, mode);
    return tf;
}

void tfs_write(TFILE* tf, void* buf, size_t count, size_t offset) {

    int res, num_overlaps, i;

    fseek(tf->local_file, 0, SEEK_END);
    Interval *interval = tangram_it_new(offset, count, ftell(tf->local_file));
    Interval** overlaps = tangram_it_overlaps(tf->it, interval, &res, &num_overlaps);

    Interval *old, *start, *end;

    switch(res) {
        // 1. No overlap
        // Write the data at the end of the local file
        // Insert the new interval
        case IT_NO_OVERLAP:
            tangram_it_insert(tf->it, interval);
            fwrite(buf, 1, count, tf->local_file);
            break;
        // 2. Have exactly one overlap
        // The old interval fully covers the new one
        // Only need to update the old content
        case IT_COVERED_BY_ONE:
            old = overlaps[0];
            size_t local_offset = old->local_offset+(offset - old->offset);
            fseek(tf->local_file, local_offset, SEEK_CUR);
            fwrite(buf, 1, count, tf->local_file);
            tangram_free(interval, sizeof(Interval));
            break;
        // 3. The new interval fully covers several old ones
        // Delete all old intervals and insert this new one
        case IT_COVERS_ALL:
            for(i = 0; i < num_overlaps; i++)
                tangram_it_delete(tf->it, overlaps[i]);
            tangram_it_insert(tf->it, interval);
            fwrite(buf, 1, count, tf->local_file);
            break;
        case IT_PARTIAL_COVERED:
            printf("IT_PARTIAL_COVERED: Not handled\n");
            /*
            Interval *start_interval = interval;
            Interval *end_interval = interval;
            size_t start_offset = interval->offset;
            size_t end_offset = interval->offset + interval->count;
            for(i = 0; i < *num_overlaps; i++) {
                if(overlaps[i]->offset < start_offset) {
                    start_offset = overlaps[i]->offset;
                    start_interval = overlaps[i];
                } else if(overlaps[i]->offset+overlaps[i]->count > end_offset) {
                    end_offset = overlaps[i]->offset+overlaps[i]->count;
                    //end_interval = overlaps[i];
                }
            }
            */
            break;
    }

    if(overlaps)
        tangram_free(overlaps, sizeof(Interval*)*num_overlaps);

    fsync(fileno(tf->local_file));
}

void tfs_read(TFILE* tf, void* buf, size_t count, size_t offset) {
    size_t local_offset;
    bool found = tangram_it_query(tf->it, offset, count, &local_offset);

    if(found) {
        fseek(tf->local_file, local_offset, SEEK_CUR);
        fread(buf, 1, count, tf->local_file);
    } else {
        printf("Local copy not exist. Not handled yet\n");
    }
}

void tfs_read_lazy(TFILE* tf, void* buf, size_t count, size_t offset) {
    fread(buf, 1, count, tf->local_file);
}

void tfs_notify() {
}

void tfs_close(TFILE* tf) {
    fclose(tf->local_file);
    tangram_it_destroy(tf->it);

    tangram_free(tf->it, sizeof(Interval));
    tangram_free(tf, sizeof(TFILE));
    tf = NULL;
}


