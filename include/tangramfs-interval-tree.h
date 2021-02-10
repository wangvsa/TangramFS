#ifndef __TANGRAMFS_INTERVAL_TREE__
#define __TANGRAMFS_INTERVAL_TREE__
#include <stdlib.h>
#include <stdbool.h>
#include "utlist.h"

typedef struct Interval_t {
    size_t offset;               // offset of the target file
    size_t count;                // count of bytes
    size_t local_offset;         // offset of the local file
    struct Interval_t *next;
} Interval;


typedef struct IntervalTree_T {
    Interval* head;
} IntervalTree;

void tfs_it_init(IntervalTree *it) {
    it->head = NULL;
}

void tfs_it_destroy(IntervalTree *it) {
    Interval *elt, *tmp;
    LL_FOREACH_SAFE(it->head, elt, tmp) {
        LL_DELETE(it->head, elt);
        free(elt);
    }
}

void tfs_it_insert(IntervalTree* it, size_t offset, size_t count, size_t local_offset) {
    Interval *interval = malloc(sizeof(Interval));
    interval->offset = offset;
    interval->count = count;
    interval->local_offset = local_offset;
    LL_PREPEND(it->head, interval);
}

bool tfs_it_query(IntervalTree *it, size_t offset, size_t count, size_t *local_offset) {
    bool found = false;
    Interval *interval;
    LL_FOREACH(it->head, interval) {
        if( (interval->offset == offset) &&
                (interval->count == count)) {
            found = true;
            break;
        }
    }

    if(found)
        *local_offset = interval->local_offset;
    return found;
}

#endif
