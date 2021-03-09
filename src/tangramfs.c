#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <mpi.h>
#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-meta.h"

typedef struct TFS_Info_t {
    int mpi_rank;
    int mpi_size;
    MPI_Comm mpi_comm;
    char buffer_dir[128];
    char persist_dir[128];
} TFS_Info;

static TFS_Info tfs;

void tfs_init(const char* persist_dir, const char* buffer_dir) {
    MPI_Comm_dup(MPI_COMM_WORLD, &tfs.mpi_comm);
    MPI_Comm_rank(tfs.mpi_comm, &tfs.mpi_rank);
    MPI_Comm_size(tfs.mpi_comm, &tfs.mpi_size);

    strcpy(tfs.persist_dir, persist_dir);
    strcpy(tfs.buffer_dir, buffer_dir);

    char server_addr[128] = {0};

    if(tfs.mpi_rank == 0) {
        tangram_meta_server_start(server_addr);
        MPI_Bcast(server_addr, 128, MPI_BYTE, 0, tfs.mpi_comm);
    } else {
        MPI_Bcast(server_addr, 128, MPI_BYTE, 0, tfs.mpi_comm);
        tangram_meta_client_start(server_addr);
    }
}

void tfs_finalize() {
    if(tfs.mpi_rank == 0)
        tangram_meta_server_stop();
    else
        tangram_meta_client_stop();
    MPI_Comm_free(&tfs.mpi_comm);
}

TFS_File* tfs_open(const char* pathname, const char* mode) {
    TFS_File* tf = tangram_malloc(sizeof(TFS_File));
    strcpy(tf->filename, pathname);
    tf->it = tangram_malloc(sizeof(IntervalTree));
    tangram_it_init(tf->it);

    char filename[256];
    sprintf(filename, "%s/_tfs_tmpfile.%d", tfs.buffer_dir, tfs.mpi_rank);
    tf->local_fd = open(filename, O_CREAT|O_RDWR, S_IRWXU);
    return tf;
}

void tfs_write(TFS_File* tf, const void* buf, size_t offset, size_t size) {

    int res, num_overlaps, i;

    size_t local_offset;
    local_offset = lseek(tf->local_fd, 0, SEEK_END);

    Interval *interval = tangram_it_new(offset, size, local_offset);
    Interval** overlaps = tangram_it_overlaps(tf->it, interval, &res, &num_overlaps);

    Interval *old, *start, *end;

    switch(res) {
        // 1. No overlap
        // Write the data at the end of the local file
        // Insert the new interval
        case IT_NO_OVERLAP:
            tangram_it_insert(tf->it, interval);
            size_t res = pwrite(tf->local_fd, buf, size, local_offset);
            break;
        // 2. Have exactly one overlap
        // The old interval fully covers the new one
        // Only need to update the old content
        case IT_COVERED_BY_ONE:
            old = overlaps[0];
            local_offset = old->local_offset+(offset - old->offset);
            pwrite(tf->local_fd, buf, size, local_offset);
            tangram_free(interval, sizeof(Interval));
            break;
        // 3. The new interval fully covers several old ones
        // Delete all old intervals and insert this new one
        case IT_COVERS_ALL:
            for(i = 0; i < num_overlaps; i++)
                tangram_it_delete(tf->it, overlaps[i]);
            tangram_it_insert(tf->it, interval);
            pwrite(tf->local_fd, buf, size, local_offset);
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

    fsync(tf->local_fd);
    tf->offset += size;
}

void tfs_read(TFS_File* tf, void* buf, size_t offset, size_t size) {
    printf("Local copy not exist. Not handled yet\n");
    tf->offset += size;
}

void tfs_read_lazy(TFS_File* tf, void* buf, size_t offset, size_t size) {
    size_t local_offset;
    bool found = tangram_it_query(tf->it, offset, size, &local_offset);

    if(found) {
        pread(tf->local_fd, buf, size, local_offset);
    } else {
        tfs_read(tf, buf, size, offset);
    }
    tf->offset += size;
}

void tfs_notify(TFS_File* tf, size_t offset, size_t size) {
    tangram_meta_issue_rpc(RPC_NAME_NOTIFY, tf->filename, tfs.mpi_rank, offset, size);
}

void tfs_close(TFS_File* tf) {
    close(tf->local_fd);
    tangram_it_destroy(tf->it);

    tangram_free(tf->it, sizeof(Interval));
    tangram_free(tf, sizeof(TFS_File));
    tf = NULL;
}


