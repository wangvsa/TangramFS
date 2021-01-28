#ifndef __TANGRAMFS_INTERVAL_TREE__
#define __TANGRAMFS_INTERVAL_TREE__
#include <stdlib.h>
#include "utlist.h"

typedef struct Interval_t {
    size_t start;
    size_t count;
    size_t local_start;
    struct Interval_t *next;
} Interval;

typedef struct IntervalTree_T {
    Interval* head;
} IntervalTree;


void tfs_it_insert(IntervalTree* it, size_t start, size_t count, size_t local_start) {
    Interval *interval = malloc(sizeof(Interval));
    interval->start = start;
    interval->count = count;
    interval->local_start = local_start;

    LL_PREPEND(it->head, interval);
}


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


#endif
