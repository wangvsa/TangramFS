#include <stdio.h>
#include "tangramfs.h"

size_t tangram_write_impl(TFS_File* tf, const void* buf, size_t count) {
    int semantics = tangram_get_semantics();
    size_t offset = tf->offset;
    size_t res = tfs_write(tf, buf, count);

    if(semantics == TANGRAM_STRONG_SEMANTICS)
        tfs_notify(tf, offset, count);

    return res;
}

size_t tangram_read_impl(TFS_File *tf, void* buf, size_t count) {
    // All three semantics use tfs_read()
    return tfs_read(tf, buf, count);
}

void tangram_commit_impl(TFS_File* tf) {
    int semantics = tangram_get_semantics();
    if(semantics == TANGRAM_STRONG_SEMANTICS ||
        semantics == TANGRAM_COMMIT_SEMANTICS) {
        tfs_post_all(tf);
    }
}


int tangram_close_impl(TFS_File *tf) {
    // For all three semantics
    tfs_post_all(tf);
    return tfs_close(tf);
}
