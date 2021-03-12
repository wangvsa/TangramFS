#ifndef __TANGRAMFS_INTERVAL_TREE__
#define __TANGRAMFS_INTERVAL_TREE__
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
    bool posted;
} Interval;


typedef struct IntervalTree_T {
    Interval* head;
} IntervalTree;

void tangram_it_init(IntervalTree *it);

void tangram_it_finalize(IntervalTree *it);


/**
 * Delete and free an interval from the tree
 */
void tangram_it_delete(IntervalTree* it, Interval* interval);


/**
 * Insert an interval to the tree
 * The caller is responsible to guarantee the interval
 * is not overlapped with any existing one.
 */
void tangram_it_insert(IntervalTree* it, Interval* interval);


/**
 * Create a new interval
 */
Interval* tangram_it_new(size_t offset, size_t count, size_t local_offset);


/**
 * Return the overlaps found in the tree
 */
Interval** tangram_it_overlaps(IntervalTree* it, Interval* interval, int *res, int *num_overlaps);

/**
 * Return a list of unposted intervals
 */
Interval** tangram_it_unposted(IntervalTree* it, int* num_unposted);

/**
 * Query an range
 */
bool tangram_it_query(IntervalTree *it, size_t offset, size_t count, size_t *local_offset);

#endif
