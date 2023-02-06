#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <mpi.h>
#include <errno.h>
#include "uthash.h"
#include "sessionfs.h"
#include "tangramfs-utils.h"

typedef struct session_book {
    int num;
    size_t* offsets;
    size_t* sizes;
    tangram_uct_addr_t** owners;
} session_book_t;

static session_book_t g_session_book;

tfs_file_t* sessionfs_open(const char* pathname) {
    g_session_book.num = 0;
    g_session_book.offsets = NULL;
    g_session_book.sizes  = NULL;
    g_session_book.owners = NULL;
    return tfs_open(pathname);
}

void sessionfs_stat(tfs_file_t* tf, struct stat* buf) {
    tfs_stat(tf, buf);
}

void sessionfs_flush(tfs_file_t *tf) {
    tfs_flush(tf);
}

ssize_t sessionfs_write(tfs_file_t* tf, const void* buf, size_t size) {
    return tfs_write(tf, buf, size);
}

ssize_t sessionfs_read(tfs_file_t* tf, void* buf, size_t size) {
    tangram_uct_addr_t *self  = tangram_rpc_client_inter_addr();
    tangram_uct_addr_t *owner = NULL;
    bool found_owner = false;

    for(int i = 0; i < g_session_book.num; i++) {
        size_t o_start = g_session_book.offsets[i];
        size_t o_end   = o_start + g_session_book.sizes[i];
        size_t t_start = tf->offset;
        size_t t_end   = t_start + size;
        if(o_start<=t_start && o_end>=t_end) {
            found_owner = true;
            owner = g_session_book.owners[i];
            break;
        }
    }

    // Another client holds the latest data,
    // issue a RMA request to get the data
    if(found_owner && tangram_uct_addr_compare(owner, self) != 0) {
        return tfs_read_peer(tf, buf, size, owner);
    }

    // Otherwise, two cases:
    // 1. found_owner != true, server doesn't know, which means:
    //      (a) client (possibly myself) has not posted, or
    //      (b) the file already exists on PFS
    // 2. found_owner = true, but myself has the latest data
    // In both case, we read it locally
    if(!found_owner || tangram_uct_addr_compare(owner, self) == 0) {
        //printf("sessionfs_read(), found owner: %d\n", found_owner);
        return tfs_read_local(tf, buf, size);
    }

    return size;
}

size_t sessionfs_seek(tfs_file_t *tf, size_t offset, int whence) {
    return tfs_seek(tf, offset, whence);
}

size_t sessionfs_tell(tfs_file_t* tf) {
    return tfs_tell(tf);
}

int sessionfs_close(tfs_file_t* tf) {
    return tfs_close(tf);
}

int sessionfs_session_open(tfs_file_t* tf, size_t* offsets, size_t* sizes, int num) {
    g_session_book.num = num;
    g_session_book.owners = malloc(num * sizeof(tangram_uct_addr_t*));
    g_session_book.offsets = malloc(num * sizeof(size_t));
    g_session_book.sizes = malloc(num * sizeof(size_t));

    for(int i = 0; i < num; i++) {
        g_session_book.offsets[i] = offsets[i];
        g_session_book.sizes[i]   = sizes[i];
        g_session_book.owners[i]  = NULL;
        //printf("sessionfs_session_open() %d/%d, [%lu,%lu]\n", i, num, offsets[i]/1024, sizes[i]/1024);
    }

    tfs_query_many(tf, offsets, sizes, num, g_session_book.owners);
    return 0;
}

int sessionfs_session_close(tfs_file_t* tf) {

    tfs_post_file(tf);

    if(g_session_book.num > 0) {
        free(g_session_book.offsets);
        free(g_session_book.sizes);
        for(int i = 0; i < g_session_book.num; i++)
            tangram_uct_addr_free(g_session_book.owners[i]);
        free(g_session_book.owners);
    }

    return 0;
}
