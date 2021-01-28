/*
 * This file implements the core primitives
 *
 * Author: Chen Wang (chenw5@illinois.edu)
 *
 * High-Performance Computing Group
 * University of Illinois at Urbana-Chamapign
 *
 */
#include <stdio.h>


/*
 * Write to a local file (on SSD or NVRAM)
 * Its the callers responsibility to check
 * that the file exists and we have permission
 * to access it.
 */
void tfsp_write(FILE* f, const void *data, size_t count) {
    fwrite(data, 1, count, f);
}

/*
 * Read from a local file (on SSD or NVRAM)
 * Its the callers responsibility to check
 * that the file exists and we have permission
 * to access it.
 */
void tfsp_read(FILE *f, const void *data, size_t count) {
    fread(data, 1, count, f);
}

/*
 * Check locally if we have the required range of data.
 * Note return true doesn't guarantee that the data we
 * hold is the most up-to-date version. It only means
 * that we have the data in our buffer.
 */
int tfsp_query(FILE* f, size_t start, size_t end) {
    return 1;
}


/*
 * Notify metadata server
 * Send an interval to metadata server.
 */
void tfsp_notify() {
}

