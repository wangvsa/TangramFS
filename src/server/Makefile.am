bin_PROGRAMS = server

server_SOURCES = tangramfs-rpc-server.c tangramfs-metadata.c tangramfs-global-interval-tree.c \
				 tangramfs-server.c $(top_srcdir)/src/common/tangramfs-utils.c
 
server_CFLAGS  = -I$(top_srcdir)/include -I${MERCURY_DIR}/include -std=c99
server_LDADD = -L${MERCURY_DIR}/lib -lmercury
