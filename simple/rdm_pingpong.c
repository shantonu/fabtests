/*
 * Copyright (c) 2013-2014 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under the OpenIB.org BSD license
 * below:
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <netdb.h>
#include <fcntl.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <shared.h>
#include "pushtotalk.h"


static int custom;
static int size_option;
static int iterations = 1000;
static int transfer_size = 1000;
static int max_credits = 128;
static int credits = 128;
static char test_name[10] = "custom";
static struct timeval start, end;
static void *buf;
static size_t buffer_size;

static struct fi_info hints;
static struct fi_domain_attr domain_hints;
static struct fi_ep_attr ep_hints;
static char *dst_addr, *src_addr;
static char *port = "9228";

static struct fid_fabric *fab;
static struct fid_domain *dom;
static struct fid_ep *ep;
static struct fid_cq *rcq, *scq;
static struct fid_av *av;
static struct fid_mr *mr;
static char local_addr[8], remote_addr[8];
static size_t addrlen;
static fi_addr_t remote_fi_addr;
struct fi_context fi_ctx_send;
struct fi_context fi_ctx_recv;

static int poll_all_sends(void)
{
	struct fi_cq_entry comp;
	int ret;

	do {
		ret = fi_cq_read(scq, &comp, sizeof comp);
		if (ret > 0) {
			credits++;
		} else if (ret < 0) {
			printf("Completion queue read %d (%s)\n", ret, fi_strerror(-ret));
			return ret;
		}
	} while (ret);
	return 0;
}

static int send_xfer(int size)
{
	struct fi_cq_entry comp;
	int ret;

	while (!credits) {
		ret = fi_cq_read(scq, &comp, sizeof comp);
		if (ret > 0) {
			goto post;
		} else if (ret < 0) {
			printf("Completion queue read %d (%s)\n", ret, fi_strerror(-ret));
			return ret;
		}
	}

	credits--;
post:
	ret = fi_sendto(ep, buf, (size_t) size, fi_mr_desc(mr), remote_fi_addr, &fi_ctx_send);
	if (ret)
		printf("fi_sendto %d (%s)\n", ret, fi_strerror(-ret));

	return ret;
}

static int recv_xfer(int size)
{
	struct fi_cq_entry comp;
	int ret;

	do {
		ret = fi_cq_read(rcq, &comp, sizeof comp);
		if (ret < 0) {
			printf("Completion queue read %d (%s)\n", ret, fi_strerror(-ret));
			return ret;
		}
	} while (!ret);

	ret = fi_recvfrom(ep, buf, buffer_size, fi_mr_desc(mr), remote_fi_addr, &fi_ctx_recv);
	if (ret)
		printf("fi_recvfrom %d (%s)\n", ret, fi_strerror(-ret));

	return ret;
}

static int sync_test(void)
{
	int ret;

	while (credits < max_credits)
		poll_all_sends();

	ret = dst_addr ? send_xfer(16) : recv_xfer(16);
	if (ret)
		return ret;

	return dst_addr ? recv_xfer(16) : send_xfer(16);
}

static int run_test(void)
{
	int ret, i;

	ret = sync_test();
	if (ret)
		goto out;

	gettimeofday(&start, NULL);
	for (i = 0; i < iterations; i++) {
		ret = dst_addr ? send_xfer(transfer_size) :
				 recv_xfer(transfer_size);
		if (ret)
			goto out;

		ret = dst_addr ? recv_xfer(transfer_size) :
				 send_xfer(transfer_size);
		if (ret)
			goto out;
	}
	gettimeofday(&end, NULL);
	show_perf(start, end, transfer_size, iterations, test_name, 1);
	ret = 0;

out:
	return ret;
}

static void free_ep_res(void)
{
	fi_close(&mr->fid);
	fi_close(&rcq->fid);
	fi_close(&scq->fid);
	free(buf);
}

static int alloc_ep_res(struct fi_info *fi)
{
	struct fi_cq_attr cq_attr;
	struct fi_av_attr av_attr;
	int ret;

	buffer_size = !custom ? test_size[TEST_CNT - 1].size : transfer_size;
	buf = malloc(buffer_size);
	if (!buf) {
		perror("malloc");
		return -1;
	}

	memset(&cq_attr, 0, sizeof cq_attr);
	cq_attr.format = FI_CQ_FORMAT_CONTEXT;
	cq_attr.wait_obj = FI_WAIT_NONE;
	cq_attr.size = max_credits << 1;
	ret = fi_cq_open(dom, &cq_attr, &scq, NULL);
	if (ret) {
		printf("fi_eq_open send comp %s\n", fi_strerror(-ret));
		goto err1;
	}

	ret = fi_cq_open(dom, &cq_attr, &rcq, NULL);
	if (ret) {
		printf("fi_eq_open recv comp %s\n", fi_strerror(-ret));
		goto err2;
	}

	ret = fi_mr_reg(dom, buf, buffer_size, 0, 0, 0, 0, &mr, NULL);
	if (ret) {
		printf("fi_mr_reg %s\n", fi_strerror(-ret));
		goto err3;
	}

	memset(&av_attr, 0, sizeof av_attr);
	av_attr.type = FI_AV_MAP;
	av_attr.count = 1;
	av_attr.name = "addr to fi_addr map";

	ret = fi_av_open(dom, &av_attr, &av, NULL);
	if (ret) {
		printf("fi_av_open %s\n", fi_strerror(-ret));
		goto err4;
	}

	return 0;

err4:
	fi_close(&mr->fid);
err3:
	fi_close(&rcq->fid);
err2:
	fi_close(&scq->fid);
err1:
	free(buf);
	return ret;
}

static int bind_fid( fid_t ep, fid_t res, uint64_t flags)
{
	int ret;

	ret = fi_bind(ep, res, flags);
	if (ret)
		printf("fi_bind %s\n", fi_strerror(-ret));
	return ret;
}

static int bind_ep_res(void)
{
	int ret;

	ret = bind_fid(&ep->fid, &scq->fid, FI_SEND);
	if (ret)
		return ret;

	ret = bind_fid(&ep->fid, &rcq->fid, FI_RECV);
	if (ret)
		return ret;

	ret = bind_fid(&ep->fid, &av->fid, 0);
	if (ret)
		return ret;

	ret = fi_enable(ep);
	if (ret)
		return ret;

	return ret;
}

static int init_fabric(void)
{
	struct fi_info *fi;
	char *node;
	int ret;

	if (src_addr) {
		ret = getaddr(src_addr, NULL, (struct sockaddr **) &hints.src_addr,
			      (socklen_t *) &hints.src_addrlen);
		if (ret)
			fprintf(stderr, "source address error %s\n", gai_strerror(ret));
	}

	node = dst_addr ? dst_addr : src_addr;

	ret = fi_getinfo(FI_VERSION(1, 0), node, port, 0, &hints, &fi);
	if (ret) {
		fprintf(stderr, "fi_getinfo %s\n", strerror(-ret));
		return ret;
	}

	/* Get remote address. For PSM provider, SFI_PSM_NAME_SERVER env variable needs to be set to 1 */
	if (dst_addr) {
		addrlen = fi->dest_addrlen;
		memcpy(&remote_addr, fi->dest_addr, addrlen);
	} else {
		addrlen = sizeof(local_addr);
	}

	ret = fi_fabric(fi->fabric_attr, &fab, NULL);
	if (ret) {
		fprintf(stderr, "fi_fabric %s\n", fi_strerror(-ret));
		goto err0;
	}

	ret = fi_fdomain(fab, fi->domain_attr, &dom, NULL);
	if (ret) {
		fprintf(stderr, "fi_fdomain %s %s\n", fi_strerror(-ret),
			fi->domain_attr->name);
		goto err1;
	}

	ret = fi_endpoint(dom, fi, &ep, NULL);
	if (ret) {
		fprintf(stderr, "fi_endpoint %s\n", fi_strerror(-ret));
		goto err2;
	}

	ret = alloc_ep_res(fi);
	if (ret)
		goto err3;

	ret = bind_ep_res();
	if (ret)
		goto err4;

	return 0;

err4:
	free_ep_res();
err3:
	fi_close(&ep->fid);
err2:
	fi_close(&dom->fid);
err1:
	fi_close(&fab->fid);
err0:
	fi_freeinfo(fi);

	return ret;
}

static int populate_av(void)
{
	int ret;

	ret = fi_av_insert(av, &remote_addr, 1, &remote_fi_addr, 0);
	if (ret) {
		fprintf(stderr, "fi_av_insert %s\n", fi_strerror(-ret));
		return ret;
	}

	return 0;
}


static int exchange_params(void)
{
	struct fi_cq_entry comp;
	int ret;

	/* Get local address blob */
	ret = fi_getname(&ep->fid, (void *)&local_addr, &addrlen);
	if (ret) {
		fprintf(stderr, "fi_getname %s\n", fi_strerror(-ret));
		return ret;
	}

	if (dst_addr) {
		ret = populate_av();
		if (ret)
			return ret;

		memcpy(buf, &local_addr, addrlen);
		ret = fi_sendto(ep, buf, addrlen, fi_mr_desc(mr), remote_fi_addr, &fi_ctx_send);
		if (ret) {
			printf("fi_sendto %d (%s)\n", ret, fi_strerror(-ret));
			return ret;
		}
	} else {
		ret = fi_recv(ep, buf, addrlen, fi_mr_desc(mr), &fi_ctx_recv);
		if (ret) {
			printf("fi_recvfrom %d (%s)\n", ret, fi_strerror(-ret));
			return ret;
		}

		do {
			ret = fi_cq_read(rcq, &comp, sizeof comp);
			if (ret < 0) {
				printf("Completion queue read %d (%s)\n", ret, fi_strerror(-ret));
				return ret;
			}
		} while (!ret);

		memcpy(&remote_addr, buf, addrlen);

		ret = populate_av();
		if (ret)
			return ret;
	}

	ret = fi_recvfrom(ep, buf, buffer_size, fi_mr_desc(mr), remote_fi_addr, &fi_ctx_recv);
	if (ret)
		printf("fi_recvfrom %d (%s)\n", ret, fi_strerror(-ret));

	return ret;
}

static int run(void)
{
	int i, ret = 0;

	ret = init_fabric();
	if (ret)
		return ret;

	printf("%-10s%-8s%-8s%-8s%-8s%8s %10s%13s\n",
	       "name", "bytes", "xfers", "iters", "total", "time", "Gb/sec", "usec/xfer");

	ret = exchange_params();
	if (ret)
		return ret;

	if (!custom) {
		for (i = 0; i < TEST_CNT; i++) {
			if (test_size[i].option > size_option)
				continue;
			init_test(test_size[i].size, test_name, &transfer_size, &iterations);
			run_test();
		}
	} else {
		ret = run_test();
	}

	while (credits < max_credits)
		poll_all_sends();

	fi_shutdown(ep, 0);
	fi_close(&ep->fid);
	free_ep_res();
	fi_close(&dom->fid);
	fi_close(&fab->fid);
	return ret;
}

int main(int argc, char **argv)
{
	int op, ret;

	while ((op = getopt(argc, argv, "d:n:p:s:C:I:S:")) != -1) {
		switch (op) {
		case 'd':
			dst_addr = optarg;
			break;
		case 'n':
			domain_hints.name = optarg;
			break;
		case 'p':
			port = optarg;
			break;
		case 's':
			src_addr = optarg;
			break;
		case 'I':
			custom = 1;
			iterations = atoi(optarg);
			break;
		case 'S':
			if (!strncasecmp("all", optarg, 3)) {
				size_option = 1;
			} else {
				custom = 1;
				transfer_size = atoi(optarg);
			}
			break;
		default:
			printf("usage: %s\n", argv[0]);
			printf("\t[-d destination_address]\n");
			printf("\t[-n domain_name]\n");
			printf("\t[-p port_number]\n");
			printf("\t[-s source_address]\n");
			printf("\t[-I iterations]\n");
			printf("\t[-S transfer_size or 'all']\n");
			exit(1);
		}
	}

	hints.domain_attr = &domain_hints;
	hints.ep_attr = &ep_hints;
	hints.type = FI_EP_RDM;
	hints.ep_cap = FI_MSG | FI_BUFFERED_RECV;
	//domain_hints.caps = FI_LOCAL_MR;
	hints.addr_format = FI_ADDR_PROTO;

	ret = run();
	return ret;
}
