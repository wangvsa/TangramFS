bin_PROGRAMS = main.out
lib_LTLIBRARIES = libtangramfs.la

AM_CPPFLAGS = -I$(top_srcdir)/include -Wno-unused-result -D_LARGEFILE64_SOURCE -DTANGRAM_PRELOADHH


libtangramfs_la_SOURCES = tangramfs.c tangramfs-interval-tree.c tangramfs-global-interval-tree.c\
						  tangramfs-rpc-server.c tangramfs-rpc-client.c tangramfs-metadata.c\
						  tangramfs-utils.c tangramfs-posix-wrapper.c

libtangramfs_la_CFLAGS  = -I/home/wangchen/softwares/mercury-2.0.0/install/include 
libtangramfs_la_LDFLAGS = -Wl,-rpath,/home/wangchen/softwares/mercury-2.0.0/install/lib -L/home/wangchen/softwares/mercury-2.0.0/install/lib
libtangramfs_la_LDFLAGS += -lmercury -pthread -lrt -version-info 0:0:0


main_out_SOURCES = main.c
main_out_LDADD = libtangramfs.la
