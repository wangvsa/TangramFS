ACLOCAL_AMFLAGS = -I m4

bin_PROGRAMS = main.out server.out test.out
lib_LTLIBRARIES = libtangramfs.la

AM_CPPFLAGS = -fPIC -I./include -Wno-unused-result -D_LARGEFILE64_SOURCE -DTANGRAM_PRELOADHH

libtangramfs_la_SOURCES = src/client/tangramfs.c \
						  src/client/tangramfs-interval-tree.c \
						  src/client/tangramfs-rpc-client.c \
						  src/client/tangramfs-rma-client.c \
						  src/client/tangramfs-rma-server.c \
						  src/client/tangramfs-posix-wrapper.c \
						  src/client/tangramfs-semantics-impl.c  \
						  src/common/tangramfs-utils.c

libtangramfs_la_CFLAGS  = -I${MERCURY_DIR}/include
libtangramfs_la_LDFLAGS = -Wl,-rpath,${MERCURY_DIR}/lib -L${MERCURY_DIR}/lib
libtangramfs_la_LDFLAGS += -L${MERCURY_DIR}/lib -lmercury -pthread -lrt -version-info 0:0:0

main_out_CFLAGS = -I./include -std=c99
main_out_SOURCES = src/client/main.c
main_out_LDADD = libtangramfs.la


server_out_SOURCES = src/server/tangramfs-rpc-server.c \
					 src/server/tangramfs-metadata.c \
					 src/server/tangramfs-global-interval-tree.c \
				 	 src/server/tangramfs-server.c \
					 src/common/tangramfs-utils.c
server_out_CFLAGS  = -I./include -I${MERCURY_DIR}/include -std=c99
server_out_LDADD   = -L${MERCURY_DIR}/lib -lmercury


test_out_CFLAGS = -I./test -I${LIBFABRIC_DIR}/include -std=c99
test_out_SOURCES = ./test/test_libfabric.c
test_out_LDADD = -L${LIBFABRIC_DIR}/lib -lfabric
