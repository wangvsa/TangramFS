#include <stdio.h>
#include <assert.h>
#include "uthash.h"
#include "seg_tree.h"
#include "tangramfs-utils.h"
#include "tangramfs-metadata.h"

typedef struct seg_tree_table {
    char filename[256];
    struct seg_tree tree;
    UT_hash_handle hh;
} seg_tree_table_t;

// Hash Map <filename, seg_tree>
static seg_tree_table_t *g_stt = NULL;


void tangram_ms_init() {
}

void tangram_ms_finalize() {
    seg_tree_table_t * entry, *tmp;
    HASH_ITER(hh, g_stt, entry, tmp) {
        HASH_DEL(g_stt, entry);
        seg_tree_destroy(&entry->tree);
        tangram_free(entry, sizeof(seg_tree_table_t));
    }
}

void tangram_ms_handle_post(int rank, char* filename, size_t offset, size_t count) {
    seg_tree_table_t *entry = NULL;
    HASH_FIND_STR(g_stt, filename, entry);

    if(!entry) {
        entry = tangram_malloc(sizeof(seg_tree_table_t));
        seg_tree_init(&entry->tree);
        strcpy(entry->filename, filename);
        HASH_ADD_STR(g_stt, filename, entry);
    }

    // TODO should ask clients to post local offset as well
    seg_tree_add(&entry->tree, offset, offset+count-1, 0, rank);
}

bool tangram_ms_handle_query(char* filename, size_t req_start, size_t req_count, int *rank) {
    seg_tree_table_t *entry = NULL;
    HASH_FIND_STR(g_stt, filename, entry);

    assert(entry);

    struct seg_tree *extents = &g_stt->tree;

    seg_tree_rdlock(extents);

    /* can we fully satisfy this request? assume we can */
    int have_data = 1;

    /* this will point to the offset of the next byte we
     * need to account for */
    size_t req_end = req_start + req_count - 1;
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
            have_data = 0;
            break;
        }

        /* get the next element in the tree */
        next = seg_tree_iter(extents, next);
    }

    /* check that we account for the full request
     * up until the last byte */
    if (expected_start < req_end) {
        /* missing some bytes at the end of the request */
        have_data = 0;
    }

    // TODO now I assume that only one rank hols the
    // entire content in a single segment.
    if (have_data) {
        *rank = first->rank;
    }

    return have_data;
}

void tangram_ms_handle_stat(char* filename, struct stat *buf) {
    seg_tree_table_t *entry = NULL;
    HASH_FIND_STR(g_stt, filename, entry);

    size_t size = 0;

    if(entry)
        size = seg_tree_max(&entry->tree);

    buf->st_size = size;
}
