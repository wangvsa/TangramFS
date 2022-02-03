#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <assert.h>
#include <mpi.h>
#include "uthash.h"
#include "tangramfs.h"
#include "tangramfs-utils.h"
#include "tangramfs-posix-wrapper.h"

tfs_info_t tfs;
tfs_file_t* tfs_files;

void* serve_rma_data(void* in_arg, size_t* size);

void tfs_init() {

    if(tfs.initialized) {
        printf("double init!\n");
        return;
    }

    int flag;
    MPI_Initialized(&flag);
    if(!flag)
        MPI_Init(NULL, NULL);

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

    tfs.initialized = false;

    // Need to have a barrier here because we can not allow
    // server stoped before all other clients
    MPI_Barrier(tfs.mpi_comm);

    tangram_rma_service_stop();
    tangram_rpc_service_stop();


    // Clear all resources
    tfs_file_t *tf, *tmp;
    HASH_ITER(hh, tfs_files, tf, tmp) {

        // TODO need to make sure to avoid concurrent writes from all clients.
        // tfs_flush(tf);

        HASH_DEL(tfs_files, tf);
        tangram_free(tf, sizeof(tfs_file_t));
    }

    tangram_release_info(&tfs);

    // TODO:
    // Need to notify the server to delete all records
    // about me, in case others asking the file I was writing
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

    tfs_file_t *tf = NULL;
    HASH_FIND_STR(tfs_files, pathname, tf);

    if(tf) {
        tf->offset = 0;
    } else {
        tf         = tangram_malloc(sizeof(tfs_file_t));
        tf->stream = NULL;
        tf->fd     = -1;
        tf->offset = 0;
        strcpy(tf->filename, pathname);
        seg_tree_init(&tf->it2);

        remove(abs_filename);   // delete the local file first
        HASH_ADD_STR(tfs_files, filename, tf);
    }

    // open node-local buffer file
    tf->local_fd = TANGRAM_REAL_CALL(open)(abs_filename, O_CREAT|O_RDWR, S_IRWXU);
    return tf;
}

void tfs_stat(tfs_file_t* tf, struct stat* buf) {
    struct stat* tmp = NULL;
    tangram_issue_metadata_rpc(AM_ID_STAT_REQUEST, tf->filename, (void**)&tmp);
    memcpy(buf, tmp, sizeof(struct stat));
    free(tmp);
}

/*
 * Flush from local buffer file to PFS
 *
 */
void tfs_flush(tfs_file_t *tf) {
    size_t chunk_size = 4096;
    char* buf = tangram_malloc(chunk_size);

    seg_tree_rdlock(&tf->it2);
    struct seg_tree_node *node = NULL;

    while ((node = seg_tree_iter(&tf->it2, node))) {

        ssize_t n;  // cannot use size_t, as n can be -1
        ssize_t all = node->end - node->start + 1;

        ssize_t local_offset;
        ssize_t done = 0;

        while(done < all) {

            n = TANGRAM_REAL_CALL(pread)(tf->local_fd, buf, chunk_size, node->ptr+done);
            // something wrong, the file was deleted?
            if(n <= 0) break;

            done += n;

            TANGRAM_REAL_CALL(pwrite)(tf->fd, buf, n, node->start+done);
        }
    }

    seg_tree_unlock(&tf->it2);

    tangram_free(buf, chunk_size);
}


size_t tfs_write(tfs_file_t* tf, const void* buf, size_t size) {
    size_t local_offset = TANGRAM_REAL_CALL(lseek)(tf->local_fd, 0, SEEK_END);
    size_t res = TANGRAM_REAL_CALL(pwrite)(tf->local_fd, buf, size, local_offset);
    //TANGRAM_REAL_CALL(fsync)(tf->local_fd);

    int rc = seg_tree_add(&tf->it2, tf->offset, tf->offset+size-1, local_offset, tangram_rpc_get_client_addr(), false);
    assert(rc == 0);

    tf->offset += size;
    return res;
}

size_t tfs_read(tfs_file_t* tf, void* buf, size_t size) {
    tangram_uct_addr_t *self  = tangram_rpc_get_client_addr();
    tangram_uct_addr_t *owner = NULL;
    int res = tfs_query(tf, tf->offset, size, &owner);
    //printf("[tangramfs %d] read %s (%d, [%lu,%lu])\n", tfs.mpi_rank, tf->filename, out.rank, tf->offset, size);


    // Another client holds the latest data,
    // issue a RMA request to get the data
    if(res == 0 && tangram_uct_addr_compare(owner, self) != 0) {
        size_t offset = tf->offset;
        double t1 = MPI_Wtime();
        tangram_issue_rma(AM_ID_RMA_REQUEST, tf->filename, owner, &offset, &size, 1, buf);
        tf->offset += size;
        double t2 = MPI_Wtime();
        //printf("[tangramfs %d] rpc for read: %.6fseconds, %.3fMB/s\n", tfs.mpi_rank, (t2-t1), size/1024.0/1024.0/(t2-t1));

        if(owner)
            tangram_uct_addr_free(owner);

        return size;
    }

    // Otherwise, two cases:
    // 1. res != 0, server doesn't know, which means:
    //      (a) client (possibly myself) has not posted, or
    //      (b) the file already exists on PFS
    // 2. res = 0, but myself has the latest data
    // In both case, we read it locally
    if(res != 0 || tangram_uct_addr_compare(owner, self) == 0) {
        if(owner)
            tangram_uct_addr_free(owner);
        return tfs_read_lazy(tf, buf, size);
    }


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

    /*
     * If we can't fully satisfy the request,
     * flush first my local writes, then directly read from PFS
     */
    if(!have_local) {
        seg_tree_unlock(extents);
        tangram_debug("[tangramfs] read from PFS %s, [%ld, %ld]\n", tf->filename, req_start, req_end-req_start+1);

        // the original file is possibly not opened
        // when running on TANGRAM_PRELOAD=OFF mode
        if(tf->fd == -1)
            tf->fd = TANGRAM_REAL_CALL(open)(tf->filename, O_RDWR);

        // TODO opt possible: can we avoid the flush?
        tfs_flush(tf);
        return TANGRAM_REAL_CALL(pread)(tf->fd, buf, req_end-req_start+1, req_start);
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


/**
 * Like POSIX lseek()
 * this function shall not, by itself, extend the size of a file.
 */
size_t tfs_seek(tfs_file_t *tf, size_t offset, int whence) {
    if(whence == SEEK_SET)
        tf->offset = offset;
    if(whence == SEEK_CUR)
        tf->offset += offset;

    // TODO
    // now we assume the local file end offset
    // is the global end offset
    // we need to ask server for the EOF
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
    if(count <= 0 || offset < 0) return;

    // Check if this is a valid range,
    // we only allow commiting an exact previous write(offset, count)
    struct seg_tree_node* node = seg_tree_find_exact(&tf->it2, offset, offset+count-1);
    assert(node != NULL);

    int* ack;
    tangram_issue_rpc(AM_ID_POST_REQUEST, tf->filename, &offset, &count, 1, (void**)&ack);
    free(ack);

    seg_tree_wrlock(&tf->it2);
    seg_tree_set_posted_nolock(&tf->it2, node);
    seg_tree_coalesce_nolock(&tf->it2, node);
    seg_tree_unlock(&tf->it2);
}

void tfs_post_all(tfs_file_t* tf) {
    int num = 0;
    int i = 0;
    size_t *offsets = NULL;
    size_t *counts  = NULL;

    seg_tree_wrlock(&tf->it2);
    struct seg_tree_node *node = NULL;
    while ((node = seg_tree_iter(&tf->it2, node))) {
        if(!node->posted) num++;
    }

    offsets = tangram_malloc(sizeof(size_t) * num);
    counts  = tangram_malloc(sizeof(size_t) * num);

    node = NULL;
    while ((node = seg_tree_iter(&tf->it2, node))) {
        if(!seg_tree_posted_nolock(&tf->it2, node)) {
            offsets[i]  = node->start;
            counts[i++] = node->end - node->start + 1;
            seg_tree_set_posted_nolock(&tf->it2, node);
        }
    }

    // Coalesce all ranges in the tree
    seg_tree_coalesce_all_nolock(&tf->it2);

    int* ack;
    tangram_issue_rpc(AM_ID_POST_REQUEST, tf->filename, offsets, counts, num, (void**)&ack);
    free(ack);

    tangram_free(offsets, sizeof(size_t)*num);
    tangram_free(counts, sizeof(size_t)*num);

    seg_tree_unlock(&tf->it2);
}

int tfs_query(tfs_file_t* tf, size_t offset, size_t size, tangram_uct_addr_t** owner) {
    void* buf = NULL;
    tangram_issue_rpc(AM_ID_QUERY_REQUEST, tf->filename, &offset, &size, 1, &buf);
    if(buf) {
        *owner = malloc(sizeof(tangram_uct_addr_t));
        tangram_uct_addr_deserialize(buf, *owner);
        free(buf);
        return 0;
    } else {
        *owner = NULL;
    }

    return -1;
}

int tfs_close(tfs_file_t* tf) {
    int res = 0;
    if(tf->fd != -1) {
        TANGRAM_REAL_CALL(close)(tf->fd);
        tf->fd = -1;
    }
    if(tf->stream != NULL) {
        TANGRAM_REAL_CALL(fclose)(tf->stream);
        tf->stream = NULL;
    }
    if(tf->local_fd != -1) {
        res = TANGRAM_REAL_CALL(close)(tf->local_fd);
        tf->local_fd = -1;
    }

    // The tfs_file_t and its interval tree is not released
    // just like Linux page cache won't be cleared at close point
    // because the same file might be opened later for read.
    // We clean all resources at tfs_finalize();
    // TODO this assumes we have enough buffer space.
    return res;
}


// Fetch the entire file
size_t tfs_fetch(const char* filename, void* buf) {
    tfs_file_t* tf = tfs_open(filename);

    struct stat stat_buf;
    tfs_stat(tf, &stat_buf);
    size_t size = stat_buf.st_size;

    tangram_uct_addr_t *self  = tangram_rpc_get_client_addr();
    tangram_uct_addr_t *owner = NULL;

    int res = tfs_query(tf, 0, size, &owner);

    // Server knows who has the data
    if(res == 0 ) {
        // Other clients have a copy in their buffer
        if( tangram_uct_addr_compare(owner, self) != 0 ) {
            size_t offset = 0;
            double t1 = MPI_Wtime();
            tangram_issue_rma(AM_ID_RMA_REQUEST, tf->filename, owner, &offset, &size, 1, buf);
            tf->offset += size;
            double t2 = MPI_Wtime();
        }
        // I have read it before - I have a copy in my buffer
        else {
            tfs_read_lazy(tf, buf, size);
        }
    }

    // Server doesn't know, meaning no one has read it before
    // I am the first one reading this file.
    // Read from PFS then buffer it and notify the server.
    if(res != 0) {

        tf->offset = 0;
        tfs_read_lazy(tf, buf, size);

        tf->offset = 0;
        tfs_write(tf, buf, size);

        tfs_post(tf, 0, size);
    }
    printf("read, %s, size:%ld, remote_read:%d\n", filename, size, res==0);

    if(owner)
        tangram_uct_addr_free(owner);

    tfs_close(tf);
    return size;
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
        //if(TANGRAM_REAL_CALL(access)(filename, F_OK) != 0)
        //    return true;
        return true;
    }

    return false;
}

int tangram_get_semantics() {
    return tfs.semantics;
}


/*
 * Read data locally to serve for the RMA request
 */
void* serve_rma_data(void* in_arg, size_t* size) {
    rpc_in_t* in = rpc_in_unpack(in_arg);

    tfs_file_t* tf = NULL;
    HASH_FIND_STR(tfs_files, in->filename, tf);

    assert(tf != NULL);

    *size = in->intervals[0].count;
    void* data = malloc(*size);

    size_t req_start = in->intervals[0].offset;
    size_t req_end = req_start + in->intervals[0].count - 1;

    size_t res = read_local(tf, data, req_start, req_end);
    assert(res == *size);

    return data;
}
