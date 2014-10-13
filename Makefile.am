AM_CFLAGS = -g -Wall -D_GNU_SOURCE -I$(srcdir)/include

bin_PROGRAMS = \
	simple/fi_msg_pingpong \
	simple/fi_msg_rma

simple_fi_msg_pingpong_SOURCES = \
	simple/msg_pingpong.c \
	common/shared.c

simple_fi_msg_rma_SOURCES = \
	simple/msg_rma.c \
	common/shared.c

man_MANS = man/fabtests.7

EXTRA_DIST = common/shared.h $(man_MANS)
