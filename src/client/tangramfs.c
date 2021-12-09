#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <mpi.h>
#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-posix-wrapper.h"


tfs_file_t *tfs_files;   // hash map of currently opened files
tfs_info_t tfs;

void* serve_rma_data(void* in_arg, size_t* size);

void tfs_init() {
    tangram_get_info(&tfs);

    tangram_map_real_calls();
    tangram_rpc_service_start(&tfs);
    tangram_rma_service_start(&tfs, serve_rma_data);

    MPI_Barrier(tfs.mpi_comm);
    tfs.initialized = true;
}

void tfs_finalize() {
    if(!tfs.initialized)
        return;

    // Need to have a barrier here because we can not allow
    // server stoped before all other clients
    MPI_Barrier(tfs.mpi_comm);

    tangram_rma_service_stop();
    tangram_rpc_service_stop();

    // Clear all resources
    tfs_file_t *tf, *tmp;
    HASH_ITER(hh, tfs_files, tf, tmp) {
        HASH_DEL(tfs_files, tf);
        tangram_free(tf, sizeof(tfs_file_t));
    }

    tangram_release_info(&tfs);
}


tfs_file_t* tfs_open(const char* pathname) {

    char abs_filename[PATH_MAX+64];

    char* tmp = strdup(pathname);
    for(int i = 0; i < strlen(pathname); i++) {
        if(tmp[i] == '/')
            tmp[i] = '_';
    }

    sprintf(abs_filename, "%s/tfs_tmp.%s.%d", tfs.tfs_dir, tmp, tfs.mpi_rank);
    free(tmp);

    tfs_file_t *tf = tangram_get_tfs_file(pathname);
    if(tf) {
        tf->offset = 0;
    } else {
        tf = tangram_malloc(sizeof(tfs_file_t));
        strcpy(tf->filename, pathname);
        tf->offset = 0;
        seg_tree_init(&tf->it2);

        remove(abs_filename);   // delete the local file first
        HASH_ADD_STR(tfs_files, filename, tf);
    }

    // open local file as buffer, the local file probaly already exists
    tf->local_fd = TANGRAM_REAL_CALL(open)(abs_filename, O_CREAT|O_RDWR, S_IRWXU);
    if(tf->local_fd != -1)
        tf->local_stream = TANGRAM_REAL_CALL(fdopen)(tf->local_fd, "r+");
    else
        tf->local_stream = NULL;

    return tf;
}


size_t tfs_write(tfs_file_t* tf, const void* buf, size_t size) {
    size_t local_offset = TANGRAM_REAL_CALL(lseek)(tf->local_fd, 0, SEEK_END);
    size_t res = TANGRAM_REAL_CALL(pwrite)(tf->local_fd, buf, size, local_offset);
    //TANGRAM_REAL_CALL(fsync)(tf->local_fd);
    seg_tree_add(&tf->it2, tf->offset, tf->offset+size-1, local_offset, tfs.mpi_rank);
    tf->offset += size;
    return res;
}

size_t tfs_read(tfs_file_t* tf, void* buf, size_t size) {
    tangram_debug("[tangramfs: %d] read %s (%lu, %lu)\n", tfs.mpi_rank, tf->filename, tf->offset, size);
    rpc_out_t out;
    tfs_query(tf, tf->offset, size, &out);

    // 1. turns out myself has the latest data, then just read it locally.
    // 2. in case server does not know who has the data, we'll also try it locally.
    if(out.res == QUERY_NOT_FOUND || out.rank == tfs.mpi_rank)
        return tfs_read_lazy(tf, buf, size);

    /*TODO RMA read*/
    size_t offset = tf->offset;
    tangram_issue_rpc_rma(AM_ID_RMA_REQUEST, tf->filename, tfs.mpi_rank, out.rank, &offset, &size, 1, buf);
    tf->offset += size;
    return size;
}

size_t read_local(tfs_file_t* tf, void* buf, size_t req_start, size_t req_end) {

    struct seg_tree *extents = &tf->it2;

    seg_tree_rdlock(extents);

    /* can we fully satisfy this request? assume we can */
    int have_local = 1;

    /* this will point to the offset of the next byte we
     * need to account for */
    size_t expected_start = req_start;

    /* iterate over extents we have for this file,
     * and check that there are no holes in coverage.
     * we search for a starting extent using a range
     * of just the very first byte that we need */
    struct seg_tree_node* first;
    first = seg_tree_find_nolock(extents, req_start, req_start);
    struct seg_tree_node* next = first;
    while (next != NULL && next->start < req_end) {
        if (expected_start >= next->start) {
            /* this extent has the next byte we expect,
             * bump up to the first byte past the end
             * of this extent */
            expected_start = next->end + 1;
        } else {
            /* there is a gap between extents so we're missing
             * some bytes */
            have_local = 0;
            break;
        }

        /* get the next element in the tree */
        next = seg_tree_iter(extents, next);
    }

    /* check that we account for the full request
     * up until the last byte */
    if (expected_start < req_end) {
        /* missing some bytes at the end of the request */
        have_local = 0;
    }

    /* if we can't fully satisfy the request, normally this
     * means the consistency model is not enough.
     */
    if(!have_local) {
        seg_tree_unlock(extents);
        tangram_debug("[tangramfs] read_local() does not have the full content\n");
        return 0;
    }

    /* otherwise we can copy the data locally, iterate
     * over the extents and copy data into request buffer.
     * again search for a starting extent using a range
     * of just the very first byte that we need */
    next = first;
    size_t off = 0;
    expected_start = req_start;
    while ((next != NULL) && (next->start < req_end)) {
        /* get start and length of this extent */
        size_t ext_start = next->start;
        size_t ext_length = (next->end + 1) - ext_start;
        size_t ext_pos = next->ptr;

        /* the bytes this extent can provide */
        size_t this_pos = ext_pos + (req_start - ext_start);
        size_t this_length = (next->end < req_end) ? (next->end-req_start+1) : (req_end-req_start+1);
        TANGRAM_REAL_CALL(pread)(tf->local_fd, buf+off, this_length, this_pos);

        off += this_length;
        expected_start = next->end + 1;

        /* get the next element in the tree */
        next = seg_tree_iter(extents, next);
    }

    /* done reading the tree */
    seg_tree_unlock(extents);
    return req_end-req_start+1;
}

size_t tfs_read_lazy(tfs_file_t* tf, void* buf, size_t size) {
    size_t req_start = tf->offset;
    size_t req_end   = tf->offset + size - 1;
    size_t res = read_local(tf, buf, req_start, req_end);
    tf->offset += res;
    return res;
}

size_t tfs_seek(tfs_file_t *tf, size_t offset, int whence) {
    if(whence == SEEK_SET)
        tf->offset = offset;
    if(whence == SEEK_CUR)
        tf->offset += offset;

    // TODO
    // now we assume the local file end offset
    // is the global end offset
    if(whence == SEEK_END) {
        if( seg_tree_count(&tf->it2) > 0)
            tf->offset = seg_tree_max(&tf->it2) + 1;
        else
            tf->offset = 0;
    }

    return tf->offset;
}

size_t tfs_tell(tfs_file_t* tf) {
    return tf->offset;
}

void tfs_post(tfs_file_t* tf, size_t offset, size_t count) {
    int ack;
    tangram_issue_rpc_rma(AM_ID_POST_REQUEST, tf->filename, tfs.mpi_rank, 0, &offset, &count, 1, &ack);
    // TODO: need to check the range to make sure it is valid.
    // Also need to set those ranges as posted, so when we do
    // post_all() later we only send the unposted ones.
}

void tfs_post_all(tfs_file_t* tf) {

    // TODO only send unposted ones
    int num = seg_tree_count(&tf->it2);
    size_t offsets[num];
    size_t counts[num];

    seg_tree_rdlock(&tf->it2);

    int i = 0;
    struct seg_tree_node *node = NULL;
    while ((node = seg_tree_iter(&tf->it2, node))) {
        offsets[i] = node->start;
        counts[i]  = node->end - node->start + 1;
        i++;
    }

    seg_tree_unlock(&tf->it2);

    int ack;
    tangram_issue_rpc_rma(AM_ID_POST_REQUEST, tf->filename, tfs.mpi_rank, 0, offsets, counts, num, &ack);
}

void tfs_query(tfs_file_t* tf, size_t offset, size_t size, rpc_out_t* out) {
    tangram_issue_rpc_rma(AM_ID_QUERY_REQUEST, tf->filename, tfs.mpi_rank, 0, &offset, &size, 1, out);
}

int tfs_close(tfs_file_t* tf) {
    int res = TANGRAM_REAL_CALL(close)(tf->local_fd);

    // The tfs_file_t and its interval tree is not released
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
    // file in buffer directory and not exist in the backend file system.
    if ( strncmp(tfs.persist_dir, abs_path, strlen(tfs.persist_dir)) == 0 ) {
        if(TANGRAM_REAL_CALL(access)(filename, F_OK) != 0) {
            return true;
        }
    }

    return false;
}

int tangram_get_semantics() {
    return tfs.semantics;
}

tfs_file_t* tangram_get_tfs_file(const char* filename) {
    tfs_file_t *tf= NULL;
    HASH_FIND_STR(tfs_files, filename, tf);
    return tf;
}


/*
 * Read data locally to serve for the RMA request
 */
void* serve_rma_data(void* in_arg, size_t* size) {
    rpc_in_t* in = rpc_in_unpack(in_arg);

    tfs_file_t* tf = tangram_get_tfs_file(in->filename);
    assert(tf != NULL);

    *size = in->intervals[0].count;
    void* data = malloc(*size);

    size_t req_start = in->intervals[0].offset;
    size_t req_end = req_start + in->intervals[0].count - 1;

    size_t res = read_local(tf, data, req_start, req_end);
    assert(res == *size);

    return data;
}
