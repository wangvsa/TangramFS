ACLOCAL_AMFLAGS = -I m4

EXTRA_DIST = autogen.sh
EXTRA_PROGRAMS =

noinst_HEADERS =
include_HEADERS =
noinst_PROGRAMS =
nodist_noinst_SCRIPTS =
dist_noinst_SCRIPTS = autogen.sh


bin_PROGRAMS = main.out server.out server.out
lib_LTLIBRARIES = libtangramfs.la

AM_CPPFLAGS = -fPIC -I./include -Wno-unused-result -D_LARGEFILE64_SOURCE -DTANGRAM_PRELOAD

libtangramfs_la_SOURCES = src/client/tangramfs.c \
						  src/client/tangramfs-rpc.c \
						  src/client/tangramfs-posix-wrapper.c \
						  src/client/tangramfs-semantics-impl.c  \
						  src/common/tangramfs-utils.c \
						  src/ucx/tangramfs-ucx-comm.c \
						  src/ucx/tangramfs-ucx-server.c \
						  src/ucx/tangramfs-ucx-client.c \
					 	  src/ucx/tangramfs-ucx-rma.c	\
					 	  src/common/seg_tree.c

libtangramfs_la_CFLAGS  = -I./include -I${UCX_DIR}/include -std=gnu99
libtangramfs_la_LDFLAGS = -Wl,-rpath,-L${UCX_DIR}/lib
libtangramfs_la_LDFLAGS += -L${UCX_DIR}/lib -lucp -lucs -luct -pthread -lrt -version-info 0:0:0


server_out_SOURCES = src/server/tangramfs-metadata.c 				\
				 	 src/server/tangramfs-server.c 					\
					 src/common/tangramfs-utils.c 					\
				  	 src/common/seg_tree.c							\
					 src/ucx/tangramfs-ucx-comm.c 					\
					 src/ucx/tangramfs-ucx-server.c 				\
					 src/ucx/tangramfs-ucx-client.c 				\
					 src/ucx/tangramfs-ucx-rma.c 					\
					 src/client/tangramfs-rpc.c

server_out_CFLAGS  = -I./include -I${UCX_DIR}/include -std=gnu99
server_out_LDADD   = -L${UCX_DIR}/lib -lucp -lucs -luct -ldl


main_out_CFLAGS = -I./include -I${UCX_DIR}/include -std=gnu99
main_out_SOURCES = src/client/main.c
main_out_LDADD = libtangramfs.la
