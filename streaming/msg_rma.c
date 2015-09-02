/*
 * Copyright (c) 2013-2015 Intel Corporation.  All rights reserved.
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

#include <assert.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <netdb.h>

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>
#include <shared.h>

static enum ft_rma_opcodes op_type = FT_RMA_WRITE;
static char test_name[10] = "custom";
static struct timespec start, end;
struct fi_rma_iov remote;
static uint64_t cq_data = 1;
static enum fi_mr_mode mr_mode;

static int send_xfer(int size)
{
	struct fi_cq_data_entry comp;
	int ret;

	while (!tx_credits) {
		ret = fi_cq_read(txcq, &comp, 1);
		if (ret > 0) {
			goto post;
		} else if (ret < 0 && ret != -FI_EAGAIN) {
			if (ret == -FI_EAVAIL) {
				cq_readerr(txcq, "txcq");
			} else {
				FT_PRINTERR("fi_cq_read", ret);
			}
			return ret;
		}
	}

	tx_credits--;
post:
	ret = fi_send(ep, buf, (size_t) size, fi_mr_desc(mr), 0, ep);
	if (ret)
		FT_PRINTERR("fi_send", ret);

	return ret;
}

static int recv_xfer(int size)
{
	struct fi_cq_data_entry comp;
	int ret;

	do {
		ret = fi_cq_read(rxcq, &comp, 1);
		if (ret < 0 && ret != -FI_EAGAIN) {
			if (ret == -FI_EAVAIL) {
				cq_readerr(rxcq, "rxcq");
			} else {
				FT_PRINTERR("fi_cq_read", ret);
			}
			return ret;
		}
	} while (ret == -FI_EAGAIN);

	ret = fi_recv(ep, buf, rx_size, fi_mr_desc(mr), 0, buf);
	if (ret)
		FT_PRINTERR("fi_recv", ret);

	return ret;
}

static int read_data(size_t size)
{
	int ret;

	ret = fi_read(ep, buf, size, fi_mr_desc(mr),
		      0, remote.addr, remote.key, ep);
	if (ret) {
		FT_PRINTERR("fi_read", ret);
		return ret;
	}

	return 0;
}

static int write_data_with_cq_data(size_t size)
{
	int ret;

	ret = fi_writedata(ep, buf, size, fi_mr_desc(mr),
		       cq_data, 0, remote.addr, remote.key, ep);
	if (ret) {
		FT_PRINTERR("fi_writedata", ret);
		return ret;
	}
	return 0;
}

static int write_data(size_t size)
{
	int ret;

	ret = fi_write(ep, buf, size, fi_mr_desc(mr),
		       0, remote.addr, remote.key, ep);
	if (ret) {
		FT_PRINTERR("fi_write", ret);
		return ret;
	}
	return 0;
}

static int sync_test(void)
{
	int ret;

	ret = ft_wait_for_comp(txcq, fi->tx_attr->size - tx_credits);
	if (ret) {
		return ret;
	}
	tx_credits = fi->tx_attr->size;

	ret = opts.dst_addr ? send_xfer(16) : recv_xfer(16);
	if (ret) {
		return ret;
	}

	return opts.dst_addr ? recv_xfer(16) : send_xfer(16);
}

static int wait_remote_writedata_completion(void)
{
	struct fi_cq_data_entry comp;
	int ret;

	do {
		ret = fi_cq_read(rxcq, &comp, 1);
		if (ret < 0 && ret != -FI_EAGAIN) {
			if (ret == -FI_EAVAIL) {
				cq_readerr(rxcq, "rxcq");
			} else {
				FT_PRINTERR("fi_cq_read", ret);
			}
			return ret;
		}
	} while (ret == -FI_EAGAIN);

	ret = 0;
	if (comp.data != cq_data) {
		fprintf(stderr, "Got unexpected completion data %" PRIu64 "\n",
			comp.data);
	}
	assert(comp.op_context == buf || comp.op_context == NULL);
	if (comp.op_context == buf) {
		/* We need to repost the receive */
		ret = fi_recv(ep, buf, rx_size, fi_mr_desc(mr), 0, buf);
		if (ret)
			FT_PRINTERR("fi_recv", ret);
	}

	return ret;
}

static int run_test(void)
{
	int ret, i;

	ret = sync_test();
	if (ret)
		return ret;

	clock_gettime(CLOCK_MONOTONIC, &start);
	for (i = 0; i < opts.iterations; i++) {
		switch (op_type) {
		case FT_RMA_WRITE:
			ret = write_data(opts.transfer_size);
			break;
		case FT_RMA_WRITEDATA:
			ret = write_data_with_cq_data(opts.transfer_size);
			if (ret)
				return ret;
			ret = wait_remote_writedata_completion();
			break;
		case FT_RMA_READ:
			ret = read_data(opts.transfer_size);
			break;
		}
		if (ret)
			return ret;

		ret = ft_wait_for_comp(txcq, 1);
		if (ret)
			return ret;
	}
	clock_gettime(CLOCK_MONOTONIC, &end);

	if (opts.machr)
		show_perf_mr(opts.transfer_size, opts.iterations, &start, &end,
				1, opts.argc, opts.argv);
	else
		show_perf(test_name, opts.transfer_size, opts.iterations,
				&start, &end, 1);

	return 0;
}

static int alloc_ep_res(struct fi_info *fi)
{
	uint64_t access_mode;
	int ret;

	ret = ft_alloc_active_res(fi);
	if (ret)
		return ret;

	switch (op_type) {
	case FT_RMA_READ:
		access_mode = FI_REMOTE_READ;
		break;
	case FT_RMA_WRITE:
	case FT_RMA_WRITEDATA:
		access_mode = FI_REMOTE_WRITE;
		break;
	default:
		assert(0);
		return -FI_EINVAL;
	}
	ret = fi_mr_reg(domain, buf, buf_size,
			access_mode, 0, FT_MR_KEY, 0, &mr, NULL);
	if (ret) {
		FT_PRINTERR("fi_mr_reg", ret);
		return ret;
	}

	return 0;
}

static int server_connect(void)
{
	struct fi_eq_cm_entry entry;
	uint32_t event;
	struct fi_info *info = NULL;
	ssize_t rd;
	int ret;

	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		FT_PROCESS_EQ_ERR(rd, eq, "fi_eq_sread", "listen");
		return (int) rd;
	}

	info = entry.info;
	if (event != FI_CONNREQ) {
		fprintf(stderr, "Unexpected CM event %d\n", event);
		ret = -FI_EOTHER;
		goto err;
	}

	mr_mode = info->domain_attr->mr_mode;
	ret = fi_domain(fabric, info, &domain, NULL);
	if (ret) {
		FT_PRINTERR("fi_domain", ret);
		goto err;
	}

	ret = alloc_ep_res(info);
	if (ret)
		 goto err;

	ret = ft_init_ep();
	if (ret)
		goto err;

	ret = fi_accept(ep, NULL, 0);
	if (ret) {
		FT_PRINTERR("fi_accept", ret);
		goto err;
	}

	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		FT_PROCESS_EQ_ERR(rd, eq, "fi_eq_sread", "accept");
		ret = (int) rd;
		goto err;
	}

	if (event != FI_CONNECTED || entry.fid != &ep->fid) {
		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
			event, entry.fid, ep);
 		ret = -FI_EOTHER;
 		goto err;
 	}

 	fi_freeinfo(info);
 	return 0;

err:
 	fi_reject(pep, info->handle, NULL, 0);
 	fi_freeinfo(info);
 	return ret;
}

static int client_connect(void)
{
	struct fi_eq_cm_entry entry;
	uint32_t event;
	ssize_t rd;
	int ret;

	ret = ft_getsrcaddr(opts.src_addr, opts.src_port, hints);
	if (ret)
		return ret;

	ret = fi_getinfo(FT_FIVERSION, opts.dst_addr, opts.dst_port, 0, hints, &fi);
	if (ret) {
		FT_PRINTERR("fi_getinfo", ret);
		return ret;
	}

	ret = ft_open_fabric_res();
	if (ret)
		return ret;

	mr_mode = fi->domain_attr->mr_mode;
 	ret = fi_domain(fabric, fi, &domain, NULL);
	if (ret) {
		FT_PRINTERR("fi_domain", ret);
		return ret;
	}

	ret = alloc_ep_res(fi);
	if (ret)
		return ret;

	ret = ft_init_ep();
	if (ret)
		return ret;

	ret = fi_connect(ep, fi->dest_addr, NULL, 0);
	if (ret) {
		FT_PRINTERR("fi_connect", ret);
		return ret;
	}

 	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		FT_PROCESS_EQ_ERR(rd, eq, "fi_eq_sread", "connect");
		return (int) rd;
	}

 	if (event != FI_CONNECTED || entry.fid != &ep->fid) {
 		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
 			event, entry.fid, ep);
 		return -FI_EOTHER;
 	}

	return 0;
}

static int exchange_addr_key(void)
{
	struct fi_rma_iov *rma_iov;

	rma_iov = buf;
	if (opts.dst_addr) {
		rma_iov->addr = mr_mode == FI_MR_SCALABLE ?
				0 : (uintptr_t) buf;
		rma_iov->key = fi_mr_key(mr);
		send_xfer(sizeof *rma_iov);
		recv_xfer(sizeof *rma_iov);
		remote = *rma_iov;
	} else {
		recv_xfer(sizeof *rma_iov);
		remote = *rma_iov;

		rma_iov->addr = mr_mode == FI_MR_SCALABLE ?
				0 : (uintptr_t) buf;
		rma_iov->key = fi_mr_key(mr);
		send_xfer(sizeof *rma_iov);
	}

	return 0;
}

static int run(void)
{
	int i, ret = 0;

	if (!opts.dst_addr) {
		ret = ft_start_server();
		if (ret)
			return ret;
	}

	ret = opts.dst_addr ? client_connect() : server_connect();
	if (ret)
		return ret;

	ret = exchange_addr_key();
	if (ret)
		return ret;

	if (!(opts.options & FT_OPT_SIZE)) {
		for (i = 0; i < TEST_CNT; i++) {
			if (test_size[i].option > opts.size_option)
				continue;
			opts.transfer_size = test_size[i].size;
			init_test(&opts, test_name, sizeof(test_name));
			ret = run_test();
			if (ret)
				goto out;
		}
	} else {
		init_test(&opts, test_name, sizeof(test_name));
		ret = run_test();
		if (ret)
			goto out;
	}

	sync_test();
	ft_wait_for_comp(txcq, fi->tx_attr->size - tx_credits);
	/* Finalize before closing ep */
	ft_finalize(fi, ep, txcq, rxcq, FI_ADDR_UNSPEC);
out:
	fi_shutdown(ep, 0);
	return ret;
}

int main(int argc, char **argv)
{
	int op, ret;

	opts = INIT_OPTS;

	hints = fi_allocinfo();
	if (!hints)
		return EXIT_FAILURE;

	while ((op = getopt(argc, argv, "ho:" CS_OPTS INFO_OPTS)) != -1) {
		switch (op) {
		case 'o':
			if (!strcmp(optarg, "read")) {
				op_type = FT_RMA_READ;
			} else if (!strcmp(optarg, "writedata")) {
				op_type = FT_RMA_WRITEDATA;
			} else if (!strcmp(optarg, "write")) {
				op_type = FT_RMA_WRITE;
			} else {
				ft_csusage(argv[0], NULL);
				fprintf(stderr, "  -o <op>\tselect operation type (read or write)\n");
				return EXIT_FAILURE;
			}
			break;
		default:
			ft_parseinfo(op, optarg, hints);
			ft_parsecsopts(op, optarg, &opts);
			break;
		case '?':
		case 'h':
			ft_csusage(argv[0], "Ping pong client and server using message RMA.");
			fprintf(stderr, "  -o <op>\trma op type: read|write|writedata (default: write)]\n");
			return EXIT_FAILURE;
		}
	}

	if (optind < argc)
		opts.dst_addr = argv[optind];

	hints->ep_attr->type = FI_EP_MSG;
	hints->caps = FI_MSG | FI_RMA;
	hints->mode = FI_LOCAL_MR | FI_RX_CQ_DATA;
	hints->addr_format = FI_SOCKADDR;

	ret = run();

	ft_free_res();
	return -ret;
}
