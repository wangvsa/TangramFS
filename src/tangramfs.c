#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <mpi.h>
#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-rpc.h"
#include "tangramfs-posix-wrapper.h"

typedef struct TFS_Info_t {
    int mpi_rank;
    int mpi_size;
    MPI_Comm mpi_comm;
    char buffer_dir[PATH_MAX];
    char persist_dir[PATH_MAX];

    int semantics;  // Strong, Session or Commit; only needed in passive mode.
    bool initialized;
} TFS_Info;

static TFS_Info tfs;

void tfs_init(const char* persist_dir, const char* buffer_dir) {
    MPI_Comm_dup(MPI_COMM_WORLD, &tfs.mpi_comm);
    MPI_Comm_rank(tfs.mpi_comm, &tfs.mpi_rank);
    MPI_Comm_size(tfs.mpi_comm, &tfs.mpi_size);

    realpath(persist_dir, tfs.persist_dir);
    realpath(buffer_dir, tfs.buffer_dir);

    const char* semantics_str = getenv("TANGRAM_SEMANTICS");
    if(semantics_str)
        tfs.semantics = atoi(semantics_str);

    char server_addr[128] = {0};

    // Rank 0 runs the mercury server
    // All ranks run the mercury client
    if(tfs.mpi_rank == 0)
        tangram_rpc_server_start(server_addr);

    MPI_Bcast(server_addr, 128, MPI_BYTE, 0, tfs.mpi_comm);
    tangram_rpc_client_start(server_addr);

    MAP_OR_FAIL(open);
    MAP_OR_FAIL(close);
    
    tfs.initialized = true;
}

void tfs_finalize() {
    if(!tfs.initialized)
        return;

    // Need to have a barrier here because we can not allow
    // server stoped before all other clients
    MPI_Barrier(tfs.mpi_comm);

    if(tfs.mpi_rank == 0)
        tangram_rpc_server_stop();
    tangram_rpc_client_stop();
    MPI_Comm_free(&tfs.mpi_comm);
}

TFS_File* tfs_open(const char* pathname) {
    TFS_File* tf = tangram_malloc(sizeof(TFS_File));
    strcpy(tf->filename, pathname);
    tf->it = tangram_malloc(sizeof(IntervalTree));
    tf->offset = 0;
    tangram_it_init(tf->it);


    char filename[PATH_MAX+64];
    sprintf(filename, "%s/_tfs_tmpfile.%d", tfs.buffer_dir, tfs.mpi_rank);
    tf->local_fd = TANGRAM_REAL_CALL(open)(filename, O_CREAT|O_RDWR, S_IRWXU);

    return tf;
}

size_t tfs_write(TFS_File* tf, const void* buf, size_t size) {

    int num_overlaps, i, overlap_type;

    size_t local_offset, res;
    local_offset = lseek(tf->local_fd, 0, SEEK_END);

    Interval *interval = tangram_it_new(tf->offset, size, local_offset);
    Interval** overlaps = tangram_it_overlaps(tf->it, interval, &overlap_type, &num_overlaps);

    Interval *old, *start, *end;

    switch(overlap_type) {
        // 1. No overlap
        // Write the data at the end of the local file
        // Insert the new interval
        case IT_NO_OVERLAP:
            tangram_it_insert(tf->it, interval);
            res = pwrite(tf->local_fd, buf, size, local_offset);
            break;
        // 2. Have exactly one overlap
        // The old interval fully covers the new one
        // Only need to update the old content
        case IT_COVERED_BY_ONE:
            old = overlaps[0];
            old->posted = false;
            local_offset = old->local_offset+(tf->offset - old->offset);
            res = pwrite(tf->local_fd, buf, size, local_offset);
            tangram_free(interval, sizeof(Interval));
            break;
        // 3. The new interval fully covers several old ones
        // Delete all old intervals and insert this new one
        case IT_COVERS_ALL:
            for(i = 0; i < num_overlaps; i++)
                tangram_it_delete(tf->it, overlaps[i]);
            tangram_it_insert(tf->it, interval);
            res = pwrite(tf->local_fd, buf, size, local_offset);
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
    return res;
}

size_t tfs_read(TFS_File* tf, void* buf, size_t size) {
    tfs_query(tf, tf->offset, size);
    tf->offset += size;
    return 0;
}

size_t tfs_read_lazy(TFS_File* tf, void* buf, size_t size) {
    size_t res;
    size_t local_offset;
    bool found = tangram_it_query(tf->it, tf->offset, size, &local_offset);

    if(found) {
        res = pread(tf->local_fd, buf, size, local_offset);
        tf->offset += size;
    } else {
        res = tfs_read(tf, buf, size);
    }

    return res;
}

size_t tfs_seek(TFS_File *tf, size_t offset, int whence) {
    if(whence == SEEK_SET)
        tf->offset = offset;
    if(whence == SEEK_CUR)
        tf->offset += offset;
    // TODO do not support SEEK_END For now.
    return tf->offset;
}

void tfs_post(TFS_File* tf, size_t offset, size_t count) {
    tangram_rpc_issue_rpc(RPC_NAME_POST, tf->filename, tfs.mpi_rank, offset, count);
}

void tfs_post_all(TFS_File* tf) {
    int num;
    Interval** unposted = tangram_it_unposted(tf->it, &num);
    for(int i = 0; i < num; i++) {
        tfs_post(tf, unposted[i]->offset, unposted[i]->count);
    }
    tangram_free(unposted, num*sizeof(Interval*));
}

void tfs_query(TFS_File* tf, size_t offset, size_t size) {
    tangram_rpc_issue_rpc(RPC_NAME_QUERY, tf->filename, tfs.mpi_rank, offset, size);
}

int tfs_close(TFS_File* tf) {
    int res = TANGRAM_REAL_CALL(close)(tf->local_fd);
    tangram_it_finalize(tf->it);

    tangram_free(tf->it, sizeof(Interval));
    tangram_free(tf, sizeof(TFS_File));
    tf = NULL;

    return res;
}

bool tangram_should_intercept(const char* filename) {
    // Not initialized yet
    if(!tfs.initialized) {
        return false;
    }

    char abs_path[PATH_MAX];
    realpath(filename, abs_path);
    return strncmp(tfs.persist_dir, abs_path, strlen(tfs.persist_dir)) == 0;
}

int tangram_get_semantics() {
    return tfs.semantics;
}
