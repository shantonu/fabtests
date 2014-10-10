AM_CFLAGS = -g -Wall -D_GNU_SOURCE -I$(srcdir)/include

bin_PROGRAMS = \
	simple/fi_pingpong \
	simple/fi_rma

simple_fi_pingpong_SOURCES = \
	simple/pingpong.c \
	common/shared.c

simple_fi_rma_SOURCES = \
	simple/rma.c \
	common/shared.c

man_MANS = man/fabtests.7

EXTRA_DIST = common/shared.h $(man_MANS)
