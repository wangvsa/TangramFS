#ifndef __TANGRAMFS_INTERVAL_TREE__
#define __TANGRAMFS_INTERVAL_TREE__
#include <stdlib.h>
#include <stdbool.h>
#include "utlist.h"

#define IT_NO_OVERLAP       0
#define IT_COVERED_BY_ONE   1
#define IT_COVERS_ALL       2
#define IT_PARTIAL_COVERED  3



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

inline bool is_overlap(Interval *i1, Interval *i2) {
    if((i1->offset+i1->count > i2->offset) &&
            (i1->offset <= i2->offset))
        return true;
    if((i2->offset+i2->count > i1->offset) &&
            (i2->offset <= i1->offset))
        return true;
    return false;
}

// Return if i1 is fully covered by i2
inline bool is_covered(Interval *i1, Interval *i2) {
    if((i2->offset <= i1->offset) &&
        (i2->offset+i2->count >= i1->offset+i1->count) )
        return true;
    return false;
}

void tfs_it_delete(IntervalTree* it, Interval* interval) {
    LL_DELETE(it->head, interval);
    free(interval);
}

void tfs_it_insert(IntervalTree* it, Interval* interval) {
    LL_PREPEND(it->head, interval);
}

Interval* tfs_it_new(size_t offset, size_t count, size_t local_offset) {
    Interval* interval = malloc(sizeof(Interval));
    interval->offset = offset;
    interval->count = count;
    interval->local_offset = local_offset;
    return interval;
}

Interval** tfs_it_overlaps(IntervalTree* it, Interval* interval, int *res, int *num_overlaps) {
    *num_overlaps = 0;
    Interval *i1, *i2;
    i2 = interval;
    LL_FOREACH(it->head, i1) {
        *num_overlaps += is_overlap(i1, i2);
    }

    if(*num_overlaps == 0) {
        *res = IT_NO_OVERLAP;
        return NULL;
    }

    // Do a second pass to retrive all overlaps
    // We are sure the intervals in the interval tree will have no overlaps.
    Interval *current = i2;
    Interval **overlaps = malloc(sizeof(Interval*) * (*num_overlaps));
    int i = 0;
    LL_FOREACH(it->head, i1) {
        if(is_overlap(i1, i2))
            overlaps[i++] = i1;
    }

    // 1. Only one overlap and it fully covers the current one
    //    No need to insert the new interval, just update the old content
    if(*num_overlaps == 1) {
        if(is_covered(current, overlaps[0])) {
            *res = IT_COVERED_BY_ONE;
            return overlaps;
        }
    }

    // 2. Fully covers multiple old ones
    //    Delete all old ones and insert the new one
    bool covers_all = true;
    for(i = 0; i < *num_overlaps; i++) {
        if(!is_covered(overlaps[i], current)) {
            covers_all = false;
            break;
        }
    }
    if(covers_all) {
        *res = IT_COVERS_ALL;
        return overlaps;
    }

    // 3. Partial covered
    //    Merge them to create a new interval, then delete
    //    the old ones.
    //
    *res = IT_PARTIAL_COVERED;
    return overlaps;
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
