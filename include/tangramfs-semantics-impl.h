#ifndef _TANGRAMFS_SEMANTICS_IMPL_H_
#define _TANGRAMFS_SEMANTICS_IMPL_H_

size_t tangram_write_impl(TFS_File* tf, const void* buf, size_t count);

size_t tangram_read_impl(TFS_File *tf, void* buf, size_t count);

int tangram_commit_impl(TFS_File* tf);

int tangram_close_impl(TFS_File *tf);

#endif
