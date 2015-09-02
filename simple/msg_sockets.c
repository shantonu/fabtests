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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <netdb.h>

#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_cm.h>

#include "shared.h"


union sockaddr_any {
	struct sockaddr		sa;
	struct sockaddr_in	sin;
	struct sockaddr_in6	sin6;
	struct sockaddr_storage	ss;
};

static union sockaddr_any bound_addr;
static size_t bound_addr_len = sizeof bound_addr;


/* Wrapper for memcmp for sockaddr.  Note that the sockaddr structure may
 * contain holes, so sockaddr's are expected to have been initialized to all
 * zeroes prior to being filled with an address. */
static int
sockaddrcmp(const union sockaddr_any *actual, socklen_t actual_len,
	    const union sockaddr_any *expected, socklen_t expected_len)
{
	if (actual->sa.sa_family != expected->sa.sa_family) {
		return actual->sa.sa_family - expected->sa.sa_family;
	} else if (actual_len != expected_len) {
		return actual_len - expected_len;
	}

	/* Handle binds to wildcard addresses, for address types we know
	 * about */
	switch (expected->sa.sa_family) {
	case AF_INET:
		if (expected->sin.sin_addr.s_addr == INADDR_ANY) {
			return 0;
		}
		break;
	case AF_INET6:
		if (!memcmp(&expected->sin6.sin6_addr,
			    &in6addr_any, sizeof(struct in6_addr))) {
			return 0;
		}
		break;
	}
	return memcmp(actual, expected, actual_len);
}

/* Returns a string for the given sockaddr using getnameinfo().  This returns a
 * static buffer so it is not reentrant or thread-safe.  Returns the string on
 * success and NULL on failure. */
static const char *
sockaddrstr(const union sockaddr_any *addr, socklen_t len, char *buf, size_t buflen)
{
	static char namebuf[BUFSIZ];
	static char servbuf[BUFSIZ];
	int errcode;

	if ((errcode = getnameinfo(&addr->sa, len, namebuf, BUFSIZ,
				servbuf, BUFSIZ,
				NI_NUMERICHOST | NI_NUMERICSERV))) {
		if (errcode != EAI_SYSTEM) {
			fprintf(stderr, "getnameinfo: %s\n", gai_strerror(errcode));
		} else {
			fprintf(stderr, "getnameinfo: %s\n", strerror(errno));
		}
		return NULL;
	}

	snprintf(buf, buflen, "[%s]:%s", namebuf, servbuf);
	return buf;
}

static int check_address(struct fid *fid, const char *message)
{
	char buf1[BUFSIZ], buf2[BUFSIZ];
	union sockaddr_any tmp;
	size_t tmplen;
	int ret;

	memset(&tmp, 0, sizeof tmp);
	tmplen = sizeof tmp;
	ret = fi_getname(fid, &tmp, &tmplen);
	if (ret) {
		FT_PRINTERR("fi_getname", ret);
	}

	if (sockaddrcmp(&tmp, tmplen, &bound_addr, bound_addr_len)) {
		FT_ERR("address changed after %s: got %s expected %s\n",
			message,
			sockaddrstr(&tmp, tmplen, buf1, BUFSIZ),
			sockaddrstr(&bound_addr, bound_addr_len, buf2, BUFSIZ));
		return -FI_EINVAL;
	}

	return 0;
}

static int server_listen(void)
{
	int ret;

	/* Bind EQ to passive endpoint */
	ret = fi_pep_bind(pep, &eq->fid, 0);
	if (ret) {
		FT_PRINTERR("fi_pep_bind", ret);
		return ret;
	}

	/* Listen for incoming connections */
	ret = fi_listen(pep);
	if (ret) {
		FT_PRINTERR("fi_listen", ret);
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

	/* Wait for connection request from client */
	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		FT_PRINTERR("fi_eq_sread", rd);
		return (int) rd;
	}

	info = entry.info;
	if (event != FI_CONNREQ) {
		FT_ERR("Unexpected CM event %d\n", event);
		ret = -FI_EOTHER;
		goto err;
	}

	ret = fi_domain(fabric, info, &domain, NULL);
	if (ret) {
		FT_PRINTERR("fi_domain", ret);
		goto err;
	}

	ret = ft_alloc_active_res(info);
	if (ret)
		 goto err;

	ret = ft_init_ep();
	if (ret)
		goto err;

	/* Accept the incoming connection. Also transitions endpoint to active state */
	ret = fi_accept(ep, NULL, 0);
	if (ret) {
		FT_PRINTERR("fi_accept", ret);
		goto err;
	}

	/* Wait for the connection to be established */
	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		FT_PRINTERR("fi_eq_sread", rd);
		goto err;
	}

	if (event != FI_CONNECTED || entry.fid != &ep->fid) {
		FT_ERR("Unexpected CM event %d fid %p (ep %p)\n", event, entry.fid, ep);
		ret = -FI_EOTHER;
		goto err;
	}

	ret = check_address(&ep->fid, "accept");
	if (ret) {
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

	/* Get fabric info */
	ret = fi_getinfo(FT_FIVERSION, opts.dst_addr, opts.dst_port, 0, hints, &fi);
	if (ret) {
		FT_PRINTERR("fi_getinfo", ret);
		return ret;
	}

	/* Open domain */
	ret = fi_domain(fabric, fi, &domain, NULL);
	if (ret) {
		FT_PRINTERR("fi_domain", ret);
		return ret;
	}

	ret = check_address(&pep->fid, "fi_endpoint (pep)");
	if (ret)
		return ret;

	assert(fi->handle == &pep->fid);
	ret = ft_alloc_active_res(fi);
	if (ret)
		return ret;

	/* Close the passive endpoint that we "stole" the source address
	 * from */
	FT_CLOSE_FID(pep);

	ret = check_address(&ep->fid, "fi_endpoint (ep)");
	if (ret)
		return ret;

	ret = ft_init_ep();
	if (ret)
		return ret;

	/* Connect to server */
	ret = fi_connect(ep, fi->dest_addr, NULL, 0);
	if (ret) {
		FT_PRINTERR("fi_connect", ret);
		return ret;
	}

	/* Wait for the connection to be established */
	rd = fi_eq_sread(eq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		FT_PRINTERR("fi_eq_sread", rd);
		return (int) rd;
	}

	if (event != FI_CONNECTED || entry.fid != &ep->fid) {
		FT_ERR("Unexpected CM event %d fid %p (ep %p)\n", event, entry.fid, ep);
		return -FI_EOTHER;
	}

	ret = check_address(&ep->fid, "connect");
	if (ret) {
		return ret;
	}

	return 0;
}

static int send_recv()
{
	struct fi_cq_entry comp;
	int ret;

	if (opts.dst_addr) {
		/* Client */
		fprintf(stdout, "Posting a send...\n");
		sprintf(buf, "Hello World!");
		ret = fi_send(ep, buf, sizeof("Hello World!"), fi_mr_desc(mr), 0, buf);
		if (ret) {
			FT_PRINTERR("fi_send", ret);
			return ret;
		}

		/* Read send queue */
		do {
			ret = fi_cq_read(txcq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Send completion received\n");
	} else {
		/* Server */
		fprintf(stdout, "Posting a recv...\n");
		ret = fi_recv(ep, buf, rx_size, fi_mr_desc(mr), 0, buf);
		if (ret) {
			FT_PRINTERR("fi_recv", ret);
			return ret;
		}

		/* Read recv queue */
		fprintf(stdout, "Waiting for client...\n");
		do {
			ret = fi_cq_read(rxcq, &comp, 1);
			if (ret < 0 && ret != -FI_EAGAIN) {
				FT_PRINTERR("fi_cq_read", ret);
				return ret;
			}
		} while (ret == -FI_EAGAIN);

		fprintf(stdout, "Received data from client: %s\n", (char *)buf);
	}

	return 0;
}

static int setup_handle(void)
{
	static char buf[BUFSIZ];
	struct addrinfo *ai, aihints;
	int ret;

	memset(&aihints, 0, sizeof aihints);
	aihints.ai_flags = AI_PASSIVE;
	ret = getaddrinfo(opts.src_addr, opts.src_port, &aihints, &ai);
	if (ret == EAI_SYSTEM) {
		FT_ERR("getaddrinfo for %s:%s: %s\n",
			opts.src_addr, opts.src_port, strerror(errno));
		return -ret;
	} else if (ret) {
		FT_ERR("getaddrinfo: %s\n", gai_strerror(ret));
		return -FI_ENODATA;
	}

	switch (ai->ai_family) {
	case AF_INET:
		hints->addr_format = FI_SOCKADDR_IN;
		break;
	case AF_INET6:
		hints->addr_format = FI_SOCKADDR_IN6;
		break;
	}

	/* Get fabric info */
	ret = fi_getinfo(FT_FIVERSION, opts.src_addr, NULL, FI_SOURCE, hints, &fi);
	if (ret) {
		FT_PRINTERR("fi_getinfo", ret);
		goto out;
	}
	free(fi->src_addr);
	fi->src_addr = NULL;
	fi->src_addrlen = 0;

	ret = ft_open_fabric_res();
	if (ret)
		return ret;

	/* Open a passive endpoint */
	ret = fi_passive_ep(fabric, fi, &pep, NULL);
	if (ret) {
		FT_PRINTERR("fi_passive_ep", ret);
		goto out;
	}

	ret = fi_setname(&pep->fid, ai->ai_addr, ai->ai_addrlen);
	if (ret) {
		FT_PRINTERR("fi_setname", ret);
		goto out;
	}

	ret = fi_getname(&pep->fid, &bound_addr, &bound_addr_len);
	if (ret) {
		FT_PRINTERR("fi_getname", ret);
		goto out;
	}

	/* Verify port number */
	switch (ai->ai_family) {
	case AF_INET:
		if (bound_addr.sin.sin_port == 0) {
			FT_ERR("port number is 0 after fi_setname()\n");
			ret = -FI_EINVAL;
			goto out;
		}
		break;
	case AF_INET6:
		if (bound_addr.sin6.sin6_port == 0) {
			FT_ERR("port number is 0 after fi_setname()\n");
			ret = -FI_EINVAL;
			goto out;
		}
		break;
	}

	printf("bound_addr: \"%s\"\n",
		sockaddrstr(&bound_addr, bound_addr_len, buf, BUFSIZ));

	hints->handle = &pep->fid;
out:
	freeaddrinfo(ai);
	return ret;
}


int main(int argc, char **argv)
{
	int op, ret;

	opts = INIT_OPTS;
	opts.options |= FT_OPT_SIZE;

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
			ft_usage(argv[0], "A simple MSG client-sever example.");
			return EXIT_FAILURE;
		}
	}

	if (optind < argc)
		opts.dst_addr = argv[optind];

	hints->ep_attr->type	= FI_EP_MSG;
	hints->caps		= FI_MSG;
	hints->mode		= FI_LOCAL_MR;
	hints->addr_format	= FI_SOCKADDR;

	/* Fabric and connection setup */
	if (!opts.src_addr || !opts.src_port) {
		fprintf(stderr, "Source address (-s) is required for this test\n");
		return EXIT_FAILURE;
	}

	if (opts.dst_addr && (opts.src_port == opts.dst_port))
		opts.src_port = "9229";

	ret = setup_handle();
	if (ret)
		return -ret;

	if (!opts.dst_addr) {
		ret = server_listen();
		if (ret)
			return -ret;
	}

	ret = opts.dst_addr ? client_connect() : server_connect();
	if (ret) {
		return -ret;
	}

	/* Exchange data */
	ret = send_recv();

	fi_shutdown(ep, 0);
	ft_free_res();
	return ret;
}
