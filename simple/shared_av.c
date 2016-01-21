/*
 * Copyright (c) 2013-2015 Intel Corporation.  All rights reserved.
 *
 * This software is available to you under the BSD license
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
#include <getopt.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>

#include "shared.h"

static int parent;
static int pair[2];
static int ready;

static int init_fabric(void)
{
	char *node, *service;
	uint64_t flags = 0;
	int ret;

	ret = ft_read_addr_opts(&node, &service, hints, &flags, &opts);
	if (ret)
		return ret;

	/* Get fabric info */
	ret = fi_getinfo(FT_FIVERSION, node, service, flags, hints, &fi);
	if (ret) {
		FT_PRINTERR("fi_getinfo", ret);
		return ret;
	}

	ret = ft_open_fabric_res();
	if (ret)
		return ret;

	if (opts.dst_addr && !parent) {
		/* child waits until parent is done creating AV */
		ret = read(pair[0], &ready, sizeof(int));
		if (ret < 0)
			FT_PRINTERR("read", errno);
	}
	ret = ft_alloc_active_res(fi);
	if (ret)
		return ret;

	if (opts.dst_addr && parent) {
		/* parent lets child know that AV is created */
		ret = write(pair[1], &ready, sizeof(int));
		if (ret < 0)
			FT_PRINTERR("write", errno);
	}

	ret = ft_init_ep();
	if (ret)
		return ret;

	return 0;
}

static int send_recv()
{
	struct fi_cq_entry comp;
	int ret, num_msgs = 2;
	char *msg;

	if (opts.dst_addr) {
		fprintf(stdout, "Sending message...\n");
		if (parent)
			msg = "Hello from Parent Client!";
		else
			msg = "Hello from Child Client!";

		sprintf(buf, msg);

		ret = fi_send(ep, buf, sizeof(msg),
				fi_mr_desc(mr), remote_fi_addr, &tx_ctx);
		if (ret) {
			FT_PRINTERR("fi_send", ret);
			return ret;
		}

		do {
			ret = fi_cq_read(txcq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Send completion received\n");
	} else {
		fprintf(stdout, "Waiting for message from client...\n");
		do {
			do {
				ret = fi_cq_read(rxcq, &comp, 1);
				if (ret < 0 && ret != -FI_EAGAIN) {
					FT_PRINTERR("fi_cq_read", ret);
					return ret;
				}
			} while (ret == -FI_EAGAIN);

			fprintf(stdout, "Received data from client: %s\n", (char *)buf);
			num_msgs--;
		} while (num_msgs);
	}

	return 0;
}

static int run(void)
{
	int ret;

	ret = init_fabric();
	if (ret)
		return ret;

	if (opts.dst_addr && !parent) {
		/* child waits for parent to complete av_insert */
		ret = read(pair[0], &ready, sizeof(int));
		if (ret < 0)
			FT_PRINTERR("read", errno);

		remote_fi_addr = ((fi_addr_t *)av_attr.map_addr)[0];
	} else {
		ret = ft_init_av();
		if (ret)
			return ret;
		if (opts.dst_addr) {
			/* parent lets child know that AV insert is complete */
			ret = write(pair[1], &ready, sizeof(int));
			if (ret < 0)
				FT_PRINTERR("write", errno);
		}
	}

	return send_recv();
}

int main(int argc, char **argv)
{
	int op, ret;
	pid_t child;

	opts = INIT_OPTS;
	opts.options |= FT_OPT_SIZE;

	hints = fi_allocinfo();
	if (!hints)
		return EXIT_FAILURE;

	while ((op = getopt(argc, argv, "ha:" ADDR_OPTS INFO_OPTS)) != -1) {
		switch (op) {
		case 'a':
                	opts.av_name = optarg;
			break;
		default:
			ft_parse_addr_opts(op, optarg, &opts);
			ft_parseinfo(op, optarg, hints);
			ft_parsecsopts(op, optarg, &opts);
			break;
		case '?':
		case 'h':
			ft_usage(argv[0], "A shared AV client-sever example.");
			return EXIT_FAILURE;
		}
	}

	if (optind < argc)
		opts.dst_addr = argv[optind];

	if (opts.dst_addr) {
		ret = socketpair(AF_LOCAL, SOCK_STREAM, 0, pair);
		if (ret)
			FT_PRINTERR("socketpair", errno);
		child = fork();
		if (child < 0)
			FT_PRINTERR("fork", child);
		if (child) {
			parent = 1;
			ret = close(pair[0]);
			if (ret)
				FT_PRINTERR("close", errno);
		} else {
			close(pair[1]);
			if (ret)
				FT_PRINTERR("close", errno);
		}
	}

	hints->ep_attr->type	= FI_EP_RDM;
	hints->caps		= FI_MSG;
	hints->mode		= FI_CONTEXT | FI_LOCAL_MR;

	ret = run();

	ft_free_res();

	if (opts.dst_addr && parent) {
		if (waitpid(child, NULL, WCONTINUED) < 0) {
			FT_PRINTERR("waitpid", errno);
		}
	}
	return -ret;
}
