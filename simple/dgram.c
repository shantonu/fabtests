/*
 * Copyright (c) 2013-2014 Intel Corporation.  All rights reserved.
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
#include <string.h>
#include <getopt.h>
#include <time.h>
#include <netdb.h>
#include <unistd.h>

#include <rdma/fabric.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>
#include <shared.h>

static struct cs_opts opts;
static void *buf;
static size_t buffer_size = 1024;
static int rx_depth = 512;

static void *remote_addr;
static size_t addrlen = 0;
static fi_addr_t remote_fi_addr;

static struct fi_info *fi, *hints;
static struct fid_fabric *fab;
static struct fid_domain *dom;
static struct fid_ep *ep;
static struct fid_av *av;
static struct fid_cq *rcq, *scq;
static struct fid_mr *mr;
struct fi_context fi_ctx_send;
struct fi_context fi_ctx_recv;
struct fi_context fi_ctx_av;

static void free_ep_res(void)
{
	fi_close(&ep->fid);
	fi_close(&av->fid);
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

	buf = malloc(buffer_size);
	if (!buf) {
		perror("malloc");
		return -1;
	}

	memset(&cq_attr, 0, sizeof cq_attr);
	cq_attr.format = FI_CQ_FORMAT_CONTEXT;
	cq_attr.wait_obj = FI_WAIT_NONE;
	cq_attr.size = rx_depth;

	/* Open completion queue for send completions */
	ret = fi_cq_open(dom, &cq_attr, &scq, NULL);
	if (ret) {
		FT_PRINTERR("fi_cq_open", ret);
		goto err1;
	}

	/* Open completion queue for recv completions */
	ret = fi_cq_open(dom, &cq_attr, &rcq, NULL);
	if (ret) {
		FT_PRINTERR("fi_cq_open", ret);
		goto err2;
	}

	/* Register memory */
	ret = fi_mr_reg(dom, buf, buffer_size, 0, 0, 0, 0, &mr, NULL);
	if (ret) {
		FT_PRINTERR("fi_mr_reg", ret);
		goto err3;
	}

	memset(&av_attr, 0, sizeof av_attr);
	av_attr.type = fi->domain_attr->av_type ?
			fi->domain_attr->av_type : FI_AV_MAP;
	av_attr.count = 1;
	av_attr.name = NULL;

	/* Open address vector (AV) for mapping address */
	ret = fi_av_open(dom, &av_attr, &av, NULL);
	if (ret) {
		FT_PRINTERR("fi_av_open", ret);
		 goto err4;
	 }

	ret = fi_endpoint(dom, fi, &ep, NULL);
	if (ret) {
		FT_PRINTERR("fi_endpoint", ret);
		goto err5;
	}

	return 0;

err5:
	fi_close(&av->fid);
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

static int bind_ep_res(void)
{
	int ret;

	/* Bind Send CQ with endpoint to collect send completions */
	ret = fi_ep_bind(ep, &scq->fid, FI_SEND);
	if (ret) {
		FT_PRINTERR("fi_ep_bind", ret);
		return ret;
	}

	/* Bind Recv CQ with endpoint to collect recv completions */
	ret = fi_ep_bind(ep, &rcq->fid, FI_RECV);
	if (ret) {
		FT_PRINTERR("fi_ep_bind", ret);
		return ret;
	}
	
	/* Bind AV with the endpoint to map addresses */
	ret = fi_ep_bind(ep, &av->fid, 0);
	if (ret) {
		FT_PRINTERR("fi_ep_bind", ret);
		return ret;
	}
	ret = fi_enable(ep);
	if (ret) {
		FT_PRINTERR("fi_enable", ret);
		return ret;
	 }

	return ret;
}

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
		goto err0;
	}
	
	/* Get remote address of the server */
	if (opts.dst_addr) {
		addrlen = fi->dest_addrlen;
		remote_addr = malloc(addrlen);
		memcpy(remote_addr, fi->dest_addr, addrlen);
	}

	/* Open fabric */
	ret = fi_fabric(fi->fabric_attr, &fab, NULL);
	if (ret) {
		FT_PRINTERR("fi_fabric", ret);
		goto err1;
	}

	/* Open domain */
	ret = fi_domain(fab, fi, &dom, NULL);
	if (ret) {
		FT_PRINTERR("fi_domain", ret);
		goto err2;
	}
	
	ret = alloc_ep_res(fi);
	if (ret)
		goto err4;

	ret = bind_ep_res();
	if (ret)
		goto err5;
	
	if (opts.dst_addr) {
		/* Insert address to the AV and get the fabric address back */ 	
		ret = fi_av_insert(av, remote_addr, 1, &remote_fi_addr, 0, 
				&fi_ctx_av);
		if (ret != 1) {
			FT_PRINTERR("fi_av_insert", ret);
			return ret;
		}
	}

	fi_freeinfo(fi);
	return 0;

err5:
	free_ep_res();
err4:
	fi_close(&dom->fid);
err2:
	fi_close(&fab->fid);
err1:
	fi_freeinfo(fi);
err0:
	return ret;
}

static int send_recv()
{
	struct fi_cq_entry comp;
	int ret;

	if (opts.dst_addr) {
		/* Client */
		fprintf(stdout, "Posting a send...\n");
		sprintf(buf, "Hello from Client!"); 
		ret = fi_send(ep, buf, sizeof("Hello from Client!"), 
				fi_mr_desc(mr), remote_fi_addr, &fi_ctx_send);
		if (ret) {
			FT_PRINTERR("fi_send", ret);
			return ret;
		}

		/* Read send queue */
		do {
			ret = fi_cq_read(scq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Send completion received\n");
		
		fprintf(stdout, "Posting a send...\n");
		sprintf(buf, "MSG dropped!"); 

		//another send
		ret = fi_send(ep, buf, sizeof("MSG dropped!"), 
				fi_mr_desc(mr), remote_fi_addr, &fi_ctx_send);
		if (ret) {
			FT_PRINTERR("fi_send", ret);
			return ret;
		}
	
		fprintf(stdout, "Posting a send...\n");
		sprintf(buf, "MSG re-sent!"); 

		//another send
		ret = fi_send(ep, buf, sizeof("MSG re-sent!"), 
				fi_mr_desc(mr), remote_fi_addr, &fi_ctx_send);
		if (ret) {
			FT_PRINTERR("fi_send", ret);
			return ret;
		}

		/* Read send queue */
		do {
			ret = fi_cq_read(scq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Send completion received\n");

	} else {
		/* Server */
		fprintf(stdout, "Posting a recv...\n");
		ret = fi_recv(ep, buf, buffer_size, fi_mr_desc(mr), 0, 
				&fi_ctx_recv);
		if (ret) {
			FT_PRINTERR("fi_recv", ret);
			return ret;
		}

		/* Read recv queue */
		fprintf(stdout, "Waiting for client...\n");
		do {
			ret = fi_cq_read(rcq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Received data from client: %s\n", (char *)buf);
		
		fprintf(stdout, "Posting a recv...\n");
		ret = fi_recv(ep, buf, buffer_size, fi_mr_desc(mr), 0, 
				&fi_ctx_recv);
		if (ret) {
			FT_PRINTERR("fi_recv", ret);
			return ret;
		}
		
		/* Read recv queue */
		fprintf(stdout, "Waiting for client...\n");
		do {
			ret = fi_cq_read(rcq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Received data from client: %s\n", (char *)buf);
	}

	return 0;
}

int main(int argc, char **argv)
{
	int op, ret;
	opts = INIT_OPTS;
	
	hints = fi_allocinfo();
	if (!hints)
		return EXIT_FAILURE;

	while ((op = getopt(argc, argv, "h" ADDR_OPTS INFO_OPTS)) != -1) {
		switch (op) {
		default:
			ft_parse_addr_opts(op, optarg, &opts);
			ft_parseinfo(op, optarg, hints);
			break;
		case '?':
		case 'h':
			ft_usage(argv[0], "A simple DRAM client-sever example.");
			return EXIT_FAILURE;
		}
	}

	if (optind < argc)
		opts.dst_addr = argv[optind];
	
	hints->ep_attr->type	= FI_EP_DGRAM;
	hints->caps		= FI_MSG;
	hints->mode		= FI_CONTEXT;

	/* Fabric initialization */
	ret = init_fabric();
	if(ret)
		return -ret;

	/* Exchange data */
	ret = send_recv();

	free_ep_res();
	fi_close(&dom->fid);
	fi_close(&fab->fid);
	fi_freeinfo(hints);

	return ret;
}
