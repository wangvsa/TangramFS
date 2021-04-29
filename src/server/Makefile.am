bin_PROGRAMS = server.out

server_out_SOURCES = tangramfs-rpc-server.c tangramfs-metadata.c tangramfs-global-interval-tree.c \
				 tangramfs-server.c $(top_srcdir)/src/common/tangramfs-utils.c
 
server_out_CFLAGS  = -I$(top_srcdir)/include -I${MERCURY_DIR}/include -std=c99
server_out_LDADD = -L${MERCURY_DIR}/lib -lmercury
