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

#define PATH_MAX 4096

typedef struct TFS_Info_t {
    int mpi_rank;
    int mpi_size;
    MPI_Comm mpi_comm;
    char tfs_dir[PATH_MAX];
    char persist_dir[PATH_MAX];

    int semantics;  // Strong, Session or Commit; only needed in passive mode.
    bool initialized;

} TFS_Info;


static TFS_File *tfs_files;   // hash map of currently opened files

static TFS_Info tfs;


void tfs_init(const char* persist_dir, const char* tfs_dir) {
    MPI_Comm_dup(MPI_COMM_WORLD, &tfs.mpi_comm);
    MPI_Comm_rank(tfs.mpi_comm, &tfs.mpi_rank);
    MPI_Comm_size(tfs.mpi_comm, &tfs.mpi_size);

    realpath(persist_dir, tfs.persist_dir);
    realpath(tfs_dir, tfs.tfs_dir);

    const char* semantics_str = getenv("TANGRAM_SEMANTICS");
    if(semantics_str)
        tfs.semantics = atoi(semantics_str);

    MAP_OR_FAIL(open);
    MAP_OR_FAIL(close);
    MAP_OR_FAIL(fsync);
    MAP_OR_FAIL(lseek);
    tfs.initialized = true;
}

void tfs_finalize() {
    if(!tfs.initialized)
        return;

    // Need to have a barrier here because we can not allow
    // server stoped before all other clients
    MPI_Barrier(tfs.mpi_comm);

    MPI_Comm_free(&tfs.mpi_comm);

    // Clear all resources
    TFS_File *tf, *tmp;
    HASH_ITER(hh, tfs_files, tf, tmp) {
        HASH_DEL(tfs_files, tf);
        tangram_it_finalize(tf->it);
        tangram_free(tf->it, sizeof(IntervalTree));
        tangram_free(tf, sizeof(TFS_File));
    }
}


TFS_File* tfs_open(const char* pathname) {
    TFS_File *tf = NULL;

    char abs_filename[PATH_MAX+64];
    sprintf(abs_filename, "%s/_tfs_tmpfile.%d", tfs.tfs_dir, tfs.mpi_rank);

    HASH_FIND_STR(tfs_files, pathname, tf);
    if(tf) {
        tf->offset = 0;
    } else {
        tf = tangram_malloc(sizeof(TFS_File));
        strcpy(tf->filename, pathname);
        tf->it = tangram_malloc(sizeof(IntervalTree));
        tf->offset = 0;
        tangram_it_init(tf->it);

        remove(abs_filename);   // delete the file first
        HASH_ADD_STR(tfs_files, filename, tf);
    }

    // open local file as buffer
    tf->local_fd = TANGRAM_REAL_CALL(open)(abs_filename, O_CREAT|O_RDWR, S_IRWXU);

    return tf;
}

size_t tfs_write(TFS_File* tf, const void* buf, size_t size) {

    int num_overlaps, i, overlap_type;

    size_t local_offset, res;
    local_offset = TANGRAM_REAL_CALL(lseek)(tf->local_fd, 0, SEEK_END);

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

    TANGRAM_REAL_CALL(fsync)(tf->local_fd);
    tf->offset += size;
    return res;
}

size_t tfs_read(TFS_File* tf, void* buf, size_t size) {
    int owner_rank;
    tfs_query(tf, tf->offset, size, &owner_rank);
    owner_rank = tfs.mpi_rank;
    //printf("my rank: %d, query: %lu, owner rank: %d\n", tfs.mpi_rank, tf->offset/1024/1024, owner_rank);

    // Turns out that myself has the latest data,
    // just read it locally.
    if(owner_rank == tfs.mpi_rank) {
        size_t local_offset;
        bool found = tangram_it_query(tf->it, tf->offset, size, &local_offset);
        assert(found);
        pread(tf->local_fd, buf, size, local_offset);
        tf->offset += size;
        return size;
    }

    /*TODO RMA read*/
    tf->offset += size;
    return size;
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

// TODO: need to check the range to make sure it is valid.
void tfs_post(TFS_File* tf, size_t offset, size_t count) {
    int num_covered;
    Interval** covered = tangram_it_covers(tf->it, offset, count, &num_covered);

    tangram_rpc_issue_rpc(RPC_OP_POST, tf->filename, tfs.mpi_rank, &offset, &count, 1, NULL);

    int i;
    for(i = 0; i < num_covered; i++)
        covered[i]->posted = true;
    tangram_free(covered, sizeof(Interval*)*num_covered);
}

void tfs_post_all(TFS_File* tf) {
    int num, i;
    Interval** unposted = tangram_it_unposted(tf->it, &num);

    size_t offsets[num];
    size_t counts[num];
    for(i = 0; i < num; i++) {
        offsets[i] = unposted[i]->offset;
        counts[i] = unposted[i]->count;
    }

    tangram_free(unposted, num*sizeof(Interval*));
    tangram_rpc_issue_rpc(RPC_OP_POST, tf->filename, tfs.mpi_rank, offsets, counts, num, NULL);
}

void tfs_query(TFS_File* tf, size_t offset, size_t size, int *out_rank) {
    void* out;
    tangram_rpc_issue_rpc(RPC_OP_QUERY, tf->filename, tfs.mpi_rank, &offset, &size, 1, &out);
    //rpc_query_out res = tangram_rpc_query_result();
    //*out_rank = res.rank;
}

int tfs_close(TFS_File* tf) {
    int res = TANGRAM_REAL_CALL(close)(tf->local_fd);

    // The TFS_File and its interval tree is not released
    // just like Linux page cache won't be cleared at close point
    // because the the same might be opened later for read.
    // We clean all resources at tfs_finalize();
    // TODO this assumes we have enough buffer space.
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

TFS_File* tangram_get_tfs_file(const char* filename) {
    TFS_File *tf= NULL;
    HASH_FIND_STR(tfs_files, filename, tf);
    return tf;
}
