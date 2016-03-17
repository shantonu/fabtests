/*
 * Copyright (c) 2015 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under the BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AWV
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#ifndef _FABTEST_H_
#define _FABTEST_H_

#include <stdlib.h>

#include <rdma/fabric.h>
#include <rdma/fi_atomic.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_prov.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_tagged.h>
#include <shared.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Timeouts in milliseconds */
#define FT_SREAD_TO 		10000
#define FT_COMP_TO 		10000
#define FT_DGRAM_POLL_TO	1

extern int listen_sock, sock;

//extern struct fid_wait	 *waitset;
//extern struct fid_poll	 *pollset;
//extern struct fid_stx	 *stx;
//extern struct fid_sep	 *sep;

extern struct ft_info test_info;
extern struct fi_info *fabric_info;

extern size_t sm_size_array[];
extern size_t med_size_array[];
extern size_t lg_size_array[];
extern size_t size_array[];
extern const unsigned int sm_size_cnt;
extern const unsigned int med_size_cnt;
extern const unsigned int lg_size_cnt;

struct ft_xcontrol {
	/* Removing for now, later might be needed for scalable ep */
	//struct fid_ep		*ep;
	void			*buf;
	struct fid_mr		*mr;
	void			*memdesc;
	struct iovec		*iov;
	void			**iov_desc;
	int			iov_iter;
	size_t			msg_size;
	size_t			credits;
	size_t			max_credits;
	fi_addr_t		addr;
	uint64_t		tag;
	uint8_t			seqno;
	enum fi_cq_format	cq_format;
	enum fi_wait_obj	comp_wait;  /* must be NONE */
	uint64_t		remote_cq_data;
};

struct ft_control {
	size_t			*size_array;
	int			size_cnt;
	size_t			*iov_array;
	int			iov_cnt;
	int			inc_step;
	int			xfer_iter;
	int			error;
};

extern struct ft_xcontrol ft_rx_ctrl, ft_tx_ctrl;
extern struct ft_control ft_ctrl;

enum {
	FT_MAX_CAPS		= 64,
	FT_MAX_EP_TYPES		= 8,
	FT_MAX_AV_TYPES		= 3,
	FT_MAX_PROV_MODES	= 4,
	FT_MAX_WAIT_OBJ		= 5,
	FT_DEFAULT_CREDITS	= 128,
	FT_COMP_BUF_SIZE	= 256,
};

enum ft_comp_type {
	FT_COMP_UNSPEC,
	FT_COMP_QUEUE,
//	FT_COMP_COUNTER,
	FT_MAX_COMP
};

enum ft_test_type {
	FT_TEST_UNSPEC,
	FT_TEST_LATENCY,
	FT_TEST_BANDWIDTH,
	FT_MAX_TEST
};

enum ft_class_function {
	FT_FUNC_UNSPEC,
	FT_FUNC_SEND,
	FT_FUNC_SENDV,
	FT_FUNC_SENDMSG,
	FT_FUNC_INJECT,
	FT_FUNC_INJECTDATA,
	FT_MAX_FUNCTIONS
};

#define FT_FLAG_QUICKTEST	(1ULL << 0)

struct ft_set {
	char			node[FI_NAME_MAX];
	char			service[FI_NAME_MAX];
	char			prov_name[FI_NAME_MAX];
	enum ft_test_type	test_type[FT_MAX_TEST];
	enum ft_class_function	class_function[FT_MAX_FUNCTIONS];
	enum fi_ep_type		ep_type[FT_MAX_EP_TYPES];
	enum fi_av_type		av_type[FT_MAX_AV_TYPES];
	enum ft_comp_type	comp_type[FT_MAX_COMP];
	enum fi_wait_obj	eq_wait_obj[FT_MAX_WAIT_OBJ];
	enum fi_wait_obj	cq_wait_obj[FT_MAX_WAIT_OBJ];
	uint64_t		mode[FT_MAX_PROV_MODES];
	uint64_t		caps[FT_MAX_CAPS];
	uint64_t		test_flags;
};

struct ft_series {
	struct ft_set		*sets;
	int			nsets;
	int			test_count;
	int			test_index;
	int			cur_set;
	int			cur_type;
	int			cur_func;
	int			cur_ep;
	int			cur_av;
	int			cur_comp;
	int			cur_eq_wait_obj;
	int			cur_cq_wait_obj;
	int			cur_mode;
	int			cur_caps;
};

struct ft_info {
	enum ft_test_type	test_type;
	int			test_index;
	int			test_subindex;
	enum ft_class_function	class_function;
	uint64_t		test_flags;
	uint64_t		caps;
	uint64_t		mode;
	enum fi_av_type		av_type;
	enum fi_ep_type		ep_type;
	enum ft_comp_type	comp_type;
	enum fi_wait_obj	eq_wait_obj;
	enum fi_wait_obj	cq_wait_obj;
	uint32_t		protocol;
	uint32_t		protocol_version;
	char			node[FI_NAME_MAX];
	char			service[FI_NAME_MAX];
	char			prov_name[FI_NAME_MAX];
	char			fabric_name[FI_NAME_MAX];
};


struct ft_series * fts_load(char *filename);
void fts_close(struct ft_series *series);
void fts_start(struct ft_series *series, int index);
void fts_next(struct ft_series *series);
int  fts_end(struct ft_series *series, int index);
void fts_cur_info(struct ft_series *series, struct ft_info *info);


struct ft_msg {
	uint32_t	len;
	uint8_t		data[124];
};

int ft_fw_send(int fd, void *msg, size_t len);
int ft_fw_recv(int fd, void *msg, size_t len);


int ft_open_control();
ssize_t ft_get_event(uint32_t *event, void *buf, size_t len,
		     uint32_t event_check, size_t len_check);
int ft_open_comp();
int ft_bind_comp(struct fid_ep *ep, uint64_t flags);
int ft_comp_rx();
int ft_comp_tx();

int ft_open_active();
int ft_open_passive();
int ft_enable_comm();
int ft_post_recv_bufs();
void ft_format_iov(struct iovec *iov, size_t cnt, char *buf, size_t len);
void ft_next_iov_cnt(struct ft_xcontrol *ctrl, size_t max_iov_cnt);

int ft_recv_msg();
int ft_send_msg();
int ft_send_dgram();
int ft_send_dgram_done();
int ft_recv_dgram();
int ft_recv_dgram_flood(size_t *recv_cnt);
int ft_send_dgram_flood();
int ft_sendrecv_dgram();

int ft_run_test();
int ft_reset_ep();
void ft_record_error(int error);


#ifdef __cplusplus
}
#endif

#endif /* _FABTEST_H_ */
