bin_PROGRAMS = main.out
lib_LTLIBRARIES = libtangramfs.la

AM_CPPFLAGS = -I$(top_srcdir)/include -Wno-unused-result -D_LARGEFILE64_SOURCE -DTANGRAM_PRELOAD

libtangramfs_la_SOURCES = tangramfs.c tangramfs-interval-tree.c \
						  tangramfs-rpc-client.c \
						  tangramfs-rma-client.c tangramfs-rma-server.c \
						  tangramfs-semantics-impl.c tangramfs-posix-wrapper.c \
						  $(top_srcdir)/src/common/tangramfs-utils.c

libtangramfs_la_CFLAGS  = -I${MERCURY_DIR}/include
libtangramfs_la_LDFLAGS = -Wl,-rpath,${MERCURY_DIR}/lib -L${MERCURY_DIR}/lib
libtangramfs_la_LDFLAGS += -lmercury -pthread -lrt -version-info 0:0:0

main_out_CFLAGS = -std=c99
main_out_SOURCES = main.c
main_out_LDADD = libtangramfs.la
