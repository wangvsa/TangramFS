#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include "utlist.h"
#include "tangramfs-utils.h"
#include "tangramfs-interval-tree.h"

void tangram_it_init(IntervalTree *it) {
    it->head = NULL;
}

void tangram_it_finalize(IntervalTree *it) {
    Interval *elt, *tmp;
    LL_FOREACH_SAFE(it->head, elt, tmp) {
        LL_DELETE(it->head, elt);
        tangram_free(elt, sizeof(Interval));
    }
}

bool is_overlap(Interval *i1, Interval *i2) {
    if((i1->offset+i1->count > i2->offset) &&
            (i1->offset <= i2->offset))
        return true;
    if((i2->offset+i2->count > i1->offset) &&
            (i2->offset <= i1->offset))
        return true;
    return false;
}

// Return if i1 is fully covered by i2
bool is_covered(Interval *i1, Interval *i2) {
    if((i2->offset <= i1->offset) &&
        (i2->offset+i2->count >= i1->offset+i1->count) )
        return true;
    return false;
}

void tangram_it_delete(IntervalTree* it, Interval* interval) {
    LL_DELETE(it->head, interval);
    tangram_free(interval, sizeof(Interval));
}

void tangram_it_insert(IntervalTree* it, Interval* interval) {
    LL_PREPEND(it->head, interval);
}

Interval* tangram_it_new(size_t offset, size_t count, size_t local_offset) {
    Interval* interval = tangram_malloc(sizeof(Interval));
    interval->offset = offset;
    interval->count = count;
    interval->local_offset = local_offset;
    interval->posted = false;
    return interval;
}

Interval** tangram_it_overlaps(IntervalTree* it, Interval* interval, int *res, int *num_overlaps) {
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
    Interval **overlaps = tangram_malloc(sizeof(Interval*) * (*num_overlaps));
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

Interval** tangram_it_unposted(IntervalTree *it, int *num) {
    int count = 0;
    Interval* interval;
    LL_FOREACH(it->head, interval) {
        if(!interval->posted)
            count++;
    }

    int i = 0;
    *num = count;
    Interval** unposted = tangram_malloc(sizeof(Interval*) * count);
    LL_FOREACH(it->head, interval) {
        if(!interval->posted) {
            unposted[i++] = interval;
        }
    }

    return unposted;
}

bool tangram_it_query(IntervalTree *it, size_t offset, size_t count, size_t *local_offset) {
    bool found = false;
    Interval *interval;
    LL_FOREACH(it->head, interval) {
        if( (interval->offset <= offset) &&
                (interval->offset+interval->count >= offset+count)) {
            found = true;
            break;
        }
    }

    if(found) {
        *local_offset = interval->local_offset + (offset-interval->offset);
    }
    return found;
}


Interval** tangram_it_covers(IntervalTree *it, size_t offset, size_t count, int *num_covered) {
    Interval i2 = {
        .offset = offset,
        .count = count,
    };

    int n = 0; int i = 0;
    Interval* i1;
    LL_FOREACH(it->head, i1) {
        if(is_covered(i1, &i2))
            n++;
    }

    *num_covered = n;
    Interval** covered = tangram_malloc(sizeof(Interval*) * n);

    LL_FOREACH(it->head, i1) {
        if(is_covered(i1, &i2))
            covered[i++] = i1;
    }

    return covered;
}
