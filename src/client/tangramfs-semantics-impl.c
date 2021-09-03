#include <stdio.h>
#include "tangramfs.h"

size_t tangram_write_impl(tfs_file_t* tf, const void* buf, size_t count) {
    int semantics = tangram_get_semantics();
    size_t offset = tf->offset;
    size_t res = tfs_write(tf, buf, count);

    if(semantics == TANGRAM_STRONG_SEMANTICS)
        tfs_post(tf, offset, count);

    return res;
}

size_t tangram_read_impl(tfs_file_t *tf, void* buf, size_t count) {
    // All three semantics use tfs_read()
    return tfs_read_lazy(tf, buf, count);
}

int tangram_commit_impl(tfs_file_t* tf) {
    int semantics = tangram_get_semantics();
    if(semantics == TANGRAM_STRONG_SEMANTICS ||
        semantics == TANGRAM_COMMIT_SEMANTICS) {
        tfs_post_all(tf);
    }
    // TODO return value of fsync?
    return 0;
}


int tangram_close_impl(tfs_file_t *tf) {
    // For all three semantics
    int semantics = tangram_get_semantics();
    if (semantics != TANGRAM_STRONG_SEMANTICS)
        tfs_post_all(tf);
    return tfs_close(tf);
}
