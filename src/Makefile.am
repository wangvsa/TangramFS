bin_PROGRAMS = main.out
lib_LTLIBRARIES = libtangramfs.la

AM_CPPFLAGS = -I$(top_srcdir)/include -Wno-unused-result -D_LARGEFILE64_SOURCE

libtangramfs_la_SOURCES = tangramfs.c
libtangramfs_la_LDFLAGS = -lpthread -version-info 0:0:0

main_out_SOURCES = main.c
main_out_LDADD = libtangramfs.la
