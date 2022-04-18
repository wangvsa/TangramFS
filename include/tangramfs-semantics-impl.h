#ifndef _TANGRAMFS_SEMANTICS_IMPL_H_
#define _TANGRAMFS_SEMANTICS_IMPL_H_

ssize_t tangram_write_impl(tfs_file_t* tf, const void* buf, size_t count);

ssize_t tangram_read_impl(tfs_file_t *tf, void* buf, size_t count);

int tangram_commit_impl(tfs_file_t* tf);

int tangram_close_impl(tfs_file_t *tf);

#endif
