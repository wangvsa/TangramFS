#include <stdio.h>
#include "uthash.h"
#include "tangramfs-utils.h"
#include "tangramfs-metadata.h"
#include "tangramfs-global-interval-tree.h"

typedef struct GlobalIntervalTreeEntry_t {
    char filename[256];
    GlobalIntervalTree global_it;
    UT_hash_handle hh;
} GlobalIntervalTreeEntry;


// Hash Map <filename, global interval tree>
static GlobalIntervalTreeEntry *global_it_table;

void tangram_ms_init() {
}

void tangram_ms_finalize() {
    GlobalIntervalTreeEntry* entry, *tmp;
    HASH_ITER(hh, global_it_table, entry, tmp) {
        HASH_DEL(global_it_table, entry);
        tangram_global_it_finalize(&(entry->global_it));
        tangram_free(entry, sizeof(GlobalIntervalTreeEntry));
    }
}

void tangram_ms_handle_post(int rank, char* filename, size_t offset, size_t count) {
    GlobalIntervalTreeEntry *entry = NULL;
    HASH_FIND_STR(global_it_table, filename, entry);

    if(entry) {

    } else {
        entry = tangram_malloc(sizeof(GlobalIntervalTreeEntry));
        tangram_global_it_init(&(entry->global_it));
        strcpy(entry->filename, filename);
        HASH_ADD_STR(global_it_table, filename, entry);
    }

    GlobalInterval *new = tangram_global_it_new(offset, count, rank);
    GlobalIntervalTree *global_it = &(entry->global_it);

    int num_overlaps = 0;
    GlobalInterval** overlaps = tangram_global_it_overlaps(global_it, new, &num_overlaps);
    int i;
    for(i = 0; i < num_overlaps ; i++) {
        GlobalInterval *old = overlaps[i];
        if(rank != old->rank) {
            // Case 1. new one fully covers the old one
            // Delete the old one
            if(offset <= old->offset && offset+count >= old->offset+old->count) {
                tangram_global_it_delete(global_it, old);
            }
            // Case 2. new one covers the left part of the old one
            // Increase the offset of the old one
            else if(offset+count <= old->offset+old->count) {
                old->offset = offset+count;
            }
            // Casse 3. new one covers the right part of the old one
            // Reduce the count of the old one
            else if(offset >= old->offset) {
                old->count = offset - old->offset;
            }
        }
    }

    tangram_global_it_insert(global_it, new);
}

bool tangram_ms_handle_query(char* filename, size_t offset, size_t count, int *rank) {
    GlobalIntervalTreeEntry *entry = NULL;
    HASH_FIND_STR(global_it_table, filename, entry);
    if(entry)
        return tangram_global_it_query(&(entry->global_it), offset, count, rank);

    return false;
}

void tangram_ms_handle_stat(char* filename, struct stat *buf) {
    GlobalIntervalTreeEntry *entry = NULL;
    HASH_FIND_STR(global_it_table, filename, entry);
    size_t size = 0;
    if(entry) {
        GlobalInterval *i;
        LL_FOREACH(entry->global_it.head, i) {
            if((i->offset+i->count) > size)
                size = i->offset+i->count;
        }
    }
    buf->st_size = size;
}
