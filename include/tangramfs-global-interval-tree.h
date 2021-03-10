#ifndef __TANGRAMFS_GLOBAL_INTERVAL_TREE__
#define __TANGRAMFS_GLOBAL_INTERVAL_TREE__
#include <stdbool.h>
#include "utlist.h"


typedef struct GlobalInterval_t {
    size_t offset;               // offset of the target file
    size_t count;                // count of bytes
    size_t rank;                 // rank
    struct GlobalInterval_t *next;
} GlobalInterval;

typedef struct GlobalIntervalTree_T {
    GlobalInterval* head;
} GlobalIntervalTree;

void tangram_global_it_init(GlobalIntervalTree *it);

void tangram_global_it_finalize(GlobalIntervalTree *it);

/**
 * Delete and free an interval from the tree
 */
void tangram_global_it_delete(GlobalIntervalTree* it, GlobalInterval* interval);


/**
 * Insert an interval to the tree
 * The caller is responsible to guarantee the interval
 * is not overlapped with any existing one.
 */
void tangram_global_it_insert(GlobalIntervalTree* it, GlobalInterval* interval);


/**
 * Create a new interval
 */
GlobalInterval* tangram_global_it_new(size_t offset, size_t count, int rank); 


/**
 * Return the overlaps found in the tree
 */
GlobalInterval** tangram_global_it_overlaps(GlobalIntervalTree* it, GlobalInterval* interval, int *num_overlaps);


/**
 * Query an range
 */
bool tangram_global_it_query(GlobalIntervalTree *it, size_t offset, size_t count, int* rank);

#endif
