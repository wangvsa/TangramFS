#include <stdlib.h>
#include <stdbool.h>
#include "utlist.h"
#include "tangramfs-utils.h"
#include "tangramfs-global-interval-tree.h"


void tangram_global_it_init(GlobalIntervalTree *it) {
    it->head = NULL;
}

void tangram_global_it_finalize(GlobalIntervalTree *it) {
    GlobalInterval *elt, *tmp;
    LL_FOREACH_SAFE(it->head, elt, tmp) {
        LL_DELETE(it->head, elt);
        tangram_free(elt, sizeof(GlobalInterval));
    }
}

inline bool is_overlap(GlobalInterval *i1, GlobalInterval *i2) {
    if((i1->offset+i1->count > i2->offset) &&
            (i1->offset <= i2->offset))
        return true;
    if((i2->offset+i2->count > i1->offset) &&
            (i2->offset <= i1->offset))
        return true;
    return false;
}

// Return if i1 is fully covered by i2
inline bool is_covered(GlobalInterval *i1, GlobalInterval *i2) {
    if((i2->offset <= i1->offset) &&
        (i2->offset+i2->count >= i1->offset+i1->count) )
        return true;
    return false;
}

void tangram_global_it_delete(GlobalIntervalTree* it, GlobalInterval* interval) {
    LL_DELETE(it->head, interval);
    tangram_free(interval, sizeof(GlobalInterval));
}

void tangram_global_it_insert(GlobalIntervalTree* it, GlobalInterval* interval) {
    LL_PREPEND(it->head, interval);
}

GlobalInterval* tangram_global_it_new(size_t offset, size_t count, int rank) {
    GlobalInterval* interval = tangram_malloc(sizeof(GlobalInterval));
    interval->offset = offset;
    interval->count = count;
    interval->rank = rank;
    return interval;
}

// TODO: Need to lock the interval tree
GlobalInterval** tangram_global_it_overlaps(GlobalIntervalTree* it, GlobalInterval* interval, int *num_overlaps) {
    *num_overlaps = 0;
    GlobalInterval *i1, *i2;
    i2 = interval;
    LL_FOREACH(it->head, i1) {
        *num_overlaps += is_overlap(i1, i2);
    }

    if(*num_overlaps == 0) {
        return NULL;
    }

    // Do a second pass to retrive all overlaps
    // We are sure the existing intervals in the interval tree will have no overlaps.
    GlobalInterval *current = i2;
    GlobalInterval **overlaps = tangram_malloc(sizeof(GlobalInterval*) * (*num_overlaps));
    int i = 0;
    LL_FOREACH(it->head, i1) {
        if(is_overlap(i1, i2))
            overlaps[i++] = i1;
    }

    return overlaps;
}

bool tangram_global_it_query(GlobalIntervalTree *it, size_t offset, size_t count, int *rank) {
    bool found = false;
    GlobalInterval *interval;
    LL_FOREACH(it->head, interval) {
        if( (interval->offset <= offset) &&
                (interval->offset+interval->count >= offset+count)) {
            found = true;
            break;
        }
    }

    if(found)
        *rank = interval->rank;
    return found;
}
