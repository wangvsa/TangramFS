bin_PROGRAMS = main.out
lib_LTLIBRARIES = libtangramfs.la

AM_CPPFLAGS = -I$(top_srcdir)/include -Wno-unused-result -D_LARGEFILE64_SOURCE


libtangramfs_la_SOURCES = tangramfs.c tangramfs-interval-tree.c tangramfs-meta-server.c
libtangramfs_la_CFLAGS  = -I/home/wangchen/Sources/mercury-2.0.0/install/include 
libtangramfs_la_LDFLAGS = -L/home/wangchen/Sources/mercury-2.0.0/install/lib -lmercury_hl -lmercury -lmchecksum -lna -lmercury_util -pthread -lrt -version-info 0:0:0 -static


main_out_SOURCES = main.c
main_out_LDADD = libtangramfs.la
