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
#include <time.h>

#include <rdma/fabric.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>

#include <rdma/fabric.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_tagged.h>
#include <rdma/fi_rma.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_errno.h>
#include <rdma/fi_atomic.h>
#include <shared.h>

struct addr_key {
	uint64_t addr;
	uint64_t key;
};

static enum fi_op op_type = FI_MIN;
static int custom;
static int size_option;
static int iterations = 1000;
static int transfer_size = 1024;
static char test_name[10] = "custom";
static struct timespec start, end;
static void *buf;
static void *result;
static void *compare;
static size_t buffer_size;
struct addr_key local, remote;

static struct fi_info hints;
static struct fi_domain_attr domain_hints;
static struct fi_ep_attr ep_hints;
static char *dst_addr, *src_addr;
static char *port = "9228";

static struct fid_fabric *fab;
static struct fid_pep *pep;
static struct fid_domain *dom;
static struct fid_ep *ep;
static struct fid_eq *cmeq;
static struct fid_cq *rcq, *scq;
static struct fid_mr *mr;
static struct fid_mr *mr_result;
static struct fid_mr *mr_compare;
static struct fi_context fi_ctx_send;
static struct fi_context fi_ctx_recv;
static struct fi_context fi_ctx_atomic;
// performing atmics operation on UINT_64 as an example
static enum fi_datatype datatype = FI_UINT64;
static size_t *count;
static int run_all = 0;

void usage(char *name)
{
	fprintf(stderr, "usage: %s\n", name);
	fprintf(stderr, "\t[-d destination_address]\n");
	fprintf(stderr, "\t[-n domain_name]\n");
	fprintf(stderr, "\t[-p port_number]\n");
	fprintf(stderr, "\t[-s source_address]\n");
	fprintf(stderr, "\t[-I iterations]\n");
	fprintf(stderr, "\t[-o min|max|sum|prod|lor|land|read|write|cswap|cswap_ne|"
			"\n\tcswap_lt|cswap_gt] (default: min)\n");
	fprintf(stderr, "\t[-S transfer_size or 'all']\n");
	exit(1);
}

static const char* get_fi_op_name(enum fi_op op)
{
	switch(op)
	{
		case FI_MIN: return "min";
		case FI_MAX: return "max";
		case FI_SUM: return "sum";
		case FI_PROD: return "prod";
		case FI_LOR: return "lor";
		case FI_LAND: return "land";
		case FI_ATOMIC_READ: return "read";
		case FI_ATOMIC_WRITE: return "write";
		case FI_CSWAP: return "cswap";
		case FI_CSWAP_NE: return "cswap_ne";	
		case FI_CSWAP_LT: return "cswap_lt";
		case FI_CSWAP_GT: return "cswap_gt";
		
		default: return "";
	}	

}

static enum fi_op get_fi_op(char *op) {
	if (!strcmp(op, "min"))
		return FI_MIN;
	else if (!strcmp(op, "max"))
		return FI_MAX;
	else if (!strcmp(op, "sum"))
		return FI_SUM;
	else if (!strcmp(op, "prod"))
		return FI_PROD;
	else if (!strcmp(op, "lor"))
		return FI_LOR;	
	else if (!strcmp(op, "land"))
		return FI_LAND;
	else if (!strcmp(op, "read"))
		return FI_ATOMIC_READ;
	else if (!strcmp(op, "write"))
		return FI_ATOMIC_WRITE;
	else if (!strcmp(op, "cswap"))
		return FI_CSWAP;
	else if (!strcmp(op, "cswap_ne"))
		return FI_CSWAP_NE;
	else if (!strcmp(op, "cswap_lt"))
		return FI_CSWAP_LT;
	else if (!strcmp(op, "cswap_gt"))
		return FI_CSWAP_GT;
	else {
		printf("Invalid atomic operation\n");
		return FI_ATOMIC_OP_LAST;
	}
}

static int post_recv(int size)
{
	int ret;
	
	ret = fi_recv(ep, buf, buffer_size, fi_mr_desc(mr), 0, 
			&fi_ctx_recv);
	if (ret){
		fprintf(stderr, "fi_recv %d (%s)\n", ret, fi_strerror(-ret));
		return ret;
	}
	
	return wait_for_completion(rcq, 1);
}

static int send_msg(int size)
{
    int ret;
	
	ret = fi_send(ep, buf, (size_t) size, fi_mr_desc(mr), 0, 
			&fi_ctx_send);
    if (ret)
    	fprintf(stderr, "fi_send %d (%s)\n", ret, fi_strerror(-ret));

    return wait_for_completion(scq, 1);
}

static int sync_test(void)
{
    int ret;

	ret = dst_addr ? send_msg(16) : post_recv(16);
    if (ret)
    	return ret;
	
	return dst_addr ? post_recv(16) : send_msg(16);
}

static int is_valid_base_atomic_op(enum fi_op op)
{	
    int ret;

	ret = fi_atomicvalid(ep, datatype, op, count);
    if (ret) {
		fprintf(stderr, "Provider doesn't support %s"
		" base atomic operation\n", get_fi_op_name(op));
		return 0;
    }
	return 1;		
}

static int is_valid_fetch_atomic_op(enum fi_op op)
{		
    int ret;

	ret = fi_fetch_atomicvalid(ep, datatype, op, count);
    if (ret) {
		fprintf(stderr, "Provider doesn't support %s"
		" fetch atomic operation\n", get_fi_op_name(op));
		return ret;
    }
	return 1;		
}

static int is_valid_compare_atomic_op(enum fi_op op)
{		
	int ret;
	ret = fi_compare_atomicvalid(ep, datatype, op, count);
    if (ret) {
		fprintf(stderr, "Provider doesn't support %s"
		" compare atomic operation\n", get_fi_op_name(op));
		return ret;
    }
	return 1;		
}


static int execute_base_atomic_op(enum fi_op op)
{
	int ret;
	
	ret = fi_atomic(ep, buf, 1, fi_mr_desc(mr), 0,  remote.addr, 
		remote.key, datatype, op, &fi_ctx_atomic);
    if (ret) {		
		fprintf(stderr, "fi_atomic %d (%s)\n", ret, fi_strerror(-ret));
		return ret;
	} else {						
		ret = wait_for_completion(scq, 1);
		if (ret)
			return ret;
	}		
	
	return ret;
}

static int execute_fetch_atomic_op(enum fi_op op)
{
	int ret;
	
	ret = fi_fetch_atomic(ep, buf, 1, fi_mr_desc(mr), result, 
			fi_mr_desc(mr_result), 0, remote.addr, remote.key, 
			datatype, op, &fi_ctx_atomic);
    if (ret) {		
		fprintf(stderr, "fi_fetch_atomic %d (%s)\n",ret, fi_strerror(-ret));
		return ret;
	} else {						
		ret = wait_for_completion(scq, 1);
		if (ret)
			return ret;
	}
	
	return ret;
}

static int execute_compare_atomic_op(enum fi_op op)
{
	int ret;
	
	ret = fi_compare_atomic(ep, buf, 1, fi_mr_desc(mr), compare, 
			fi_mr_desc(mr_compare),result, fi_mr_desc(mr_result), 0, 
			remote.addr, remote.key, datatype, op, &fi_ctx_atomic);
    if (ret) {
		fprintf(stderr, "fi_compare_atomic %d (%s)\n", ret, fi_strerror(-ret));
		return ret;
	} else {			
		ret = wait_for_completion(scq, 1);
		if (ret)
			return ret;
	}
	
	return ret;
}

static int run_test(void)
{
	int ret, i;
	count = (size_t*) malloc(sizeof(size_t));

	// synchronize before performing atmic operations
	sync_test();

	clock_gettime(CLOCK_MONOTONIC, &start);
	
	if(run_all) {  
		switch(0) {	
			// atomic operations usable with base atomic and 
			// fetch atomic functions
			case FI_MIN:
        			ret = is_valid_base_atomic_op(FI_MIN);
        			if (ret > 0) {
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_MIN);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_MIN);
        			if (ret > 0) {
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_MIN);
							if(ret)
								break;
						}
        			}
				
			case FI_MAX:
        			ret = is_valid_base_atomic_op(FI_MAX);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_MAX);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_MAX);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_MAX);
							if(ret)
								break;
						}
        			}
				
			case FI_SUM:
        			ret = is_valid_base_atomic_op(FI_SUM);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_SUM);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_SUM);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_SUM);
							if(ret)
								break;
						}
        			}
				
			case FI_PROD:
        			ret = is_valid_base_atomic_op(FI_PROD);
        			if (ret > 0) {	
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_PROD);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_PROD);
        			if (ret > 0) {	
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_PROD);
							if(ret)
								break;
						}
        			}
				
			case FI_LOR:
        			ret = is_valid_base_atomic_op(FI_LOR);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_LOR);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_LOR);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_LOR);
							if(ret)
								break;
						}
        			}
				
			case FI_LAND:
        			ret = is_valid_base_atomic_op(FI_LAND);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_LAND);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_LAND);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_LAND);
							if(ret)
								break;
						}
        			}
				
			case FI_ATOMIC_READ:
        			ret = is_valid_base_atomic_op(FI_ATOMIC_READ);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_ATOMIC_READ);
							if(ret)
								break;
						}
        			}
        			
					ret = is_valid_fetch_atomic_op(FI_ATOMIC_READ);
        			if (ret > 0) {		
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_ATOMIC_READ);
							if(ret)
								break;
						}
        			}
				
			case FI_ATOMIC_WRITE:
        			ret = is_valid_base_atomic_op(FI_ATOMIC_WRITE);
        			if (ret > 0) {	
						for (i = 0; i < iterations; i++) {	
							ret = execute_base_atomic_op(FI_ATOMIC_WRITE);
							if(ret)
								break;
						}
        			}
				
					ret = is_valid_fetch_atomic_op(FI_ATOMIC_WRITE);
        			if (ret > 0) {	
						for (i = 0; i < iterations; i++) {	
							ret = execute_fetch_atomic_op(FI_ATOMIC_WRITE);
							if(ret)
								break;
						}	
        			}	                                           
			
			// compare atomic functions 
			case FI_CSWAP:
				ret = is_valid_compare_atomic_op(FI_CSWAP);
        		if (ret > 0) {
					for (i = 0; i < iterations; i++) {	
						ret = execute_compare_atomic_op(FI_CSWAP);
						if(ret)
							break;
					}
        		}
			case FI_CSWAP_NE:
				ret = is_valid_compare_atomic_op(FI_CSWAP_NE);
        		if (ret > 0) {
					for (i = 0; i < iterations; i++) {	
						ret = execute_compare_atomic_op(FI_CSWAP_NE);
						if(ret)
							break;
					}
				}
			case FI_CSWAP_LT:
				ret = is_valid_compare_atomic_op(FI_CSWAP_LT);
        		if (ret > 0) {
					for (i = 0; i < iterations; i++) {	
						ret = execute_compare_atomic_op(FI_CSWAP_LT);
						if(ret)
							break;
					}
        		}
			case FI_CSWAP_GT:
				ret = is_valid_compare_atomic_op(FI_CSWAP_GT);
        		if (ret > 0) {
					for (i = 0; i < iterations; i++) {	
						ret = execute_compare_atomic_op(FI_CSWAP_GT);
						if(ret)
							break;
					}
        		}
				break;
			default:
				goto out;
				break;				
		}

	} else {		
		switch(op_type) {	
			// atomic operations usable with base atomic and 
			// fetch atomic functions
			case FI_MIN:
			case FI_MAX:
			case FI_SUM:
			case FI_PROD:
			case FI_LOR:
			case FI_LAND:
			case FI_ATOMIC_READ:
			case FI_ATOMIC_WRITE:
        		ret = is_valid_base_atomic_op(op_type);
        		if (ret > 0) {
					for (i = 0; i < iterations; i++) {	
						ret = execute_base_atomic_op(op_type);
						if(ret)
							break;
					}
        		}
				
				ret = is_valid_fetch_atomic_op(op_type);
        		if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						ret = execute_fetch_atomic_op(op_type);
						if(ret)
							break;
					}
        		}	
      			break;

			// compare atomic functions 
			case FI_CSWAP:
			case FI_CSWAP_NE:
			case FI_CSWAP_LT:
			case FI_CSWAP_GT:
				ret = is_valid_compare_atomic_op(op_type);
        		if (ret > 0) {
					for (i = 0; i < iterations; i++) {	
						ret = execute_compare_atomic_op(op_type);
						if(ret)
							break;
					}
        		} else 
					goto out;
				
				break;
			default:
				goto out;
				break;				
		}
	}
	clock_gettime(CLOCK_MONOTONIC, &end);

	if(ret)
		goto out;
		
	show_perf(test_name, transfer_size, iterations, &start, &end, 2);
	ret = 0;

out:
	return ret;
}

static void free_lres(void)
{
	fi_close(&cmeq->fid);
}

static int alloc_cm_res(void)
{
	struct fi_eq_attr cm_attr;
	int ret;

	memset(&cm_attr, 0, sizeof cm_attr);
	cm_attr.wait_obj = FI_WAIT_FD;
	ret = fi_eq_open(fab, &cm_attr, &cmeq, NULL);
	if (ret)
		fprintf(stderr, "fi_eq_open cm %s\n", fi_strerror(-ret));

	return ret;
}

static void free_ep_res(void)
{
	fi_close(&mr->fid);
	fi_close(&mr_result->fid);
	fi_close(&mr_compare->fid);
	fi_close(&rcq->fid);
	fi_close(&scq->fid);
	free(buf);
	free(result);
	free(compare);
}

static int alloc_ep_res(struct fi_info *fi)
{
	struct fi_cq_attr cq_attr;
	int ret;

	buffer_size = !custom ? test_size[TEST_CNT - 1].size : transfer_size;
	buf = malloc(MAX(buffer_size, sizeof(uint64_t)));
	if (!buf) {
		perror("malloc");
		return -1;
	}

	result = malloc(MAX(buffer_size, sizeof(uint64_t)));
	if (!result) {
		perror("malloc");
		return -1;
	}
	
	compare = malloc(MAX(buffer_size, sizeof(uint64_t)));
	if (!compare) {
		perror("malloc");
		return -1;
	}
	
	memset(&cq_attr, 0, sizeof cq_attr);
	cq_attr.format = FI_CQ_FORMAT_CONTEXT;
	cq_attr.wait_obj = FI_WAIT_NONE;
	cq_attr.size = 128;
	ret = fi_cq_open(dom, &cq_attr, &scq, NULL);
	if (ret) {
		fprintf(stderr, "fi_cq_open send comp %s\n", fi_strerror(-ret));
		goto err1;
	}

	ret = fi_cq_open(dom, &cq_attr, &rcq, NULL);
	if (ret) {
		fprintf(stderr, "fi_cq_open recv comp %s\n", fi_strerror(-ret));
		goto err2;
	}
	
	// registers local data buffer buff that specifies 
	// the first operand of the atomic operation
	ret = fi_mr_reg(dom, buf, MAX(buffer_size, sizeof(uint64_t)), 
		FI_REMOTE_READ | FI_REMOTE_WRITE, 0, 0, 0, &mr, NULL);
	if (ret) {
		fprintf(stderr, "fi_mr_reg %s\n", fi_strerror(-ret));
		goto err3;
	}
	// registers local data buffer that stores initial value of 
	// the remote buffer
	ret = fi_mr_reg(dom, result, MAX(buffer_size, sizeof(uint64_t)), 
		FI_REMOTE_READ | FI_REMOTE_WRITE, 0, 0, 0, &mr_result, NULL);
	if (ret) {
		fprintf(stderr, "fi_mr_reg %s\n", fi_strerror(-ret));
		goto err4;
	}
	
	// registers local data buffer that contains comparison data
	ret = fi_mr_reg(dom, compare, MAX(buffer_size, sizeof(uint64_t)), 
		FI_REMOTE_READ | FI_REMOTE_WRITE, 0, 0, 0, &mr_compare, NULL);
	if (ret) {
		fprintf(stderr, "fi_mr_reg %s\n", fi_strerror(-ret));
		goto err5;
	}
	
	if (!cmeq) {
		ret = alloc_cm_res();
		if (ret)
			goto err6;
	}

	return 0;

err6:
	fi_close(&mr_compare->fid);
err5:
	fi_close(&mr_result->fid);
err4:
	fi_close(&mr->fid);
err3:
	fi_close(&rcq->fid);
err2:
	fi_close(&scq->fid);
err1:
	free(buf);
	free(result);
	free(compare);
	
	return ret;
}

static int bind_fid( fid_t ep, fid_t res, uint64_t flags)
{
	int ret;

	ret = fi_bind(ep, res, flags);
	if (ret)
		fprintf(stderr, "fi_bind %s\n", fi_strerror(-ret));
	return ret;
}

static int bind_ep_res(void)
{
	int ret;

	ret = bind_fid(&ep->fid, &cmeq->fid, 0);
	if (ret)
		return ret;

	ret = bind_fid(&ep->fid, &scq->fid, FI_SEND | FI_READ | FI_WRITE);
	if (ret)
		return ret;

	ret = bind_fid(&ep->fid, &rcq->fid, FI_RECV);
	if (ret)
		return ret;

	ret = fi_enable(ep);
	if (ret)
		return ret;	

	return ret;
}

static int server_listen(void)
{
	struct fi_info *fi;
	int ret;

	ret = fi_getinfo(FI_VERSION(1, 0), src_addr, port, FI_SOURCE, &hints, &fi);
	if (ret) {
		fprintf(stderr, "fi_getinfo %s\n", strerror(-ret));
		return ret;
	}

	ret = fi_fabric(fi->fabric_attr, &fab, NULL);
	if (ret) {
		fprintf(stderr, "fi_fabric %s\n", fi_strerror(-ret));
		goto err0;
	}

	ret = fi_passive_ep(fab, fi, &pep, NULL);
	if (ret) {
		fprintf(stderr, "fi_passive_ep %s\n", fi_strerror(-ret));
		goto err1;
	}

	ret = alloc_cm_res();
	if (ret)
		goto err2;

	ret = bind_fid(&pep->fid, &cmeq->fid, 0);
	if (ret)
		goto err3;

	ret = fi_listen(pep);
	if (ret) {
		fprintf(stderr, "fi_listen %s\n", fi_strerror(-ret));
		goto err3;
	}

	fi_freeinfo(fi);
	return 0;
err3:
	free_lres();
err2:
	fi_close(&pep->fid);
err1:
	fi_close(&fab->fid);
err0:
	fi_freeinfo(fi);
	return ret;
}

static int server_connect(void)
{
	struct fi_eq_cm_entry entry;
	uint32_t event;
	struct fi_info *info = NULL;
	ssize_t rd;
	int ret;

	rd = fi_eq_sread(cmeq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		fprintf(stderr, "fi_eq_sread %zd %s\n", 
			rd, fi_strerror((int) -rd));
		return (int) rd;
	}

	if (event != FI_CONNREQ) {
		fprintf(stderr, "Unexpected CM event %d\n", event);
		ret = -FI_EOTHER;
		goto err1;
	}

	info = entry.info;
	ret = fi_domain(fab, info, &dom, NULL);
	if (ret) {
		fprintf(stderr, "fi_domain %s\n", fi_strerror(-ret));
		goto err1;
	}


	ret = fi_endpoint(dom, info, &ep, NULL);
	if (ret) {
		fprintf(stderr, "fi_endpoint for req %s\n", fi_strerror(-ret));
		goto err1;
	}

	ret = alloc_ep_res(info);
	if (ret)
		 goto err2;

	ret = bind_ep_res();
	if (ret)
		goto err3;

	ret = fi_accept(ep, NULL, 0);
	if (ret) {
		fprintf(stderr, "fi_accept %s\n", fi_strerror(-ret));
		goto err3;
	}

	rd = fi_eq_sread(cmeq, &event, &entry, sizeof entry, -1, 0);
 	if (rd != sizeof entry) {
		fprintf(stderr, "fi_eq_condread %zd %s\n", 
			rd, fi_strerror((int) -rd));
		goto err3;
 	}

	if (event != FI_COMPLETE || entry.fid != &ep->fid) {
 		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
			event, entry.fid, ep);
 		ret = -FI_EOTHER;
 		goto err3;
 	}
 
 	fi_freeinfo(info);
 	return 0;

err3:
	free_ep_res();
err2:
	fi_close(&ep->fid);
err1:

 	fi_reject(pep, info->connreq, NULL, 0);
 	fi_freeinfo(info);
 	return ret;
}

static int client_connect(void)
{
	struct fi_eq_cm_entry entry;
	uint32_t event;
	struct fi_info *fi;
	ssize_t rd;
	int ret;

	if (src_addr) {
		ret = getaddr(src_addr, NULL, (struct sockaddr **) &hints.src_addr,
			      (socklen_t *) &hints.src_addrlen);
		if (ret)
			fprintf(stderr, "source address error %s\n", fi_strerror(ret));
	}

	ret = fi_getinfo(FI_VERSION(1, 0), dst_addr, port, 0, &hints, &fi);
	if (ret) {
		fprintf(stderr, "fi_getinfo %s\n", strerror(-ret));
		goto err0;
	}

	ret = fi_fabric(fi->fabric_attr, &fab, NULL);
	if (ret) {
		fprintf(stderr, "fi_fabric %s\n", fi_strerror(-ret));
		goto err1;
	}

 	ret = fi_domain(fab, fi, &dom, NULL);
	if (ret) {
		fprintf(stderr, "fi_domain %s %s\n", fi_strerror(-ret),
			fi->domain_attr->name);
		goto err2;
	}

	ret = fi_endpoint(dom, fi, &ep, NULL);
	if (ret) {
		fprintf(stderr, "fi_endpoint %s\n", fi_strerror(-ret));
		goto err3;
	}

	ret = alloc_ep_res(fi);
	if (ret)
		goto err4;

	ret = bind_ep_res();
	if (ret)
		goto err5;

	ret = fi_connect(ep, fi->dest_addr, NULL, 0);
	if (ret) {
		fprintf(stderr, "fi_connect %s\n", fi_strerror(-ret));
		goto err5;
	}

 	rd = fi_eq_sread(cmeq, &event, &entry, sizeof entry, -1, 0);
	if (rd != sizeof entry) {
		fprintf(stderr, "fi_eq_sread %zd %s\n", rd, fi_strerror((int) -rd));
		return (int) rd;
	}

 	if (event != FI_COMPLETE || entry.fid != &ep->fid) {
 		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
 			event, entry.fid, ep);
 		ret = -FI_EOTHER;
 		goto err1;
 	}

	if (hints.src_addr)
		free(hints.src_addr);
	fi_freeinfo(fi);
	return 0;

err5:
	free_ep_res();
err4:
	fi_close(&ep->fid);
err3:
	fi_close(&dom->fid);
err2:
	fi_close(&fab->fid);
err1:
	fi_freeinfo(fi);
err0:
	if (hints.src_addr)
		free(hints.src_addr);
	return ret;
}

static int exchange_params(void)
{
	int ret;
	int len = sizeof local;

	local.addr = (uint64_t)buf;
	local.key = fi_mr_key(mr);

	if (dst_addr) {
		*(struct addr_key *)buf = local;
		ret = send_msg(len);
		if(ret)
			return ret;
	} else {
		ret = post_recv(len);
		if(ret)
			return ret;
		remote = *(struct addr_key *)buf;	
	}

	if (dst_addr) {
		ret = post_recv(len);
		if(ret)
			return ret;	
		remote = *(struct addr_key *)buf;

	} else {
		*(struct addr_key *)buf = local;
		ret = send_msg(len);
		if(ret)
			return ret;
	}

	return 0;
}

static int run(void)
{
	int i, ret = 0;

	if (!dst_addr) {
		ret = server_listen();
		if (ret)
			return ret;
	}

	ret = dst_addr ? client_connect() : server_connect();
	if (ret)
		return ret;

	ret = exchange_params();
	if (ret)
		return ret;

	if (!custom) {
		for (i = 0; i < TEST_CNT; i++) {
			if (test_size[i].option > size_option)
				continue;
			init_test(test_size[i].size, 
				test_name, &transfer_size, &iterations);
			ret = run_test();
			if(ret)
				goto out;
		}
	} else {
		ret = run_test();
	}	
	sync_test();

out:
	fi_shutdown(ep, 0);
	fi_close(&ep->fid);
	free_ep_res();
	if (!dst_addr)
		free_lres();
	fi_close(&dom->fid);
	fi_close(&fab->fid);
	return ret;
}

int main(int argc, char **argv)
{
	int op, ret;

	while ((op = getopt(argc, argv, "d:n:p:s:C:I:o:S:")) != -1) {
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
		case 'o':
			op_type = get_fi_op(optarg);
			if (op_type == FI_ATOMIC_OP_LAST)
				usage(argv[0]);
			break;
		case 'S':
			if (!strncasecmp("all", optarg, 3)) {
				size_option = 1;
				run_all = 1;
			} else {
				custom = 1;
				transfer_size = atoi(optarg);
			}
			break;
		default:
			usage(argv[0]);
		}
	}

	hints.domain_attr = &domain_hints;
	hints.ep_attr = &ep_hints;
	hints.ep_type = FI_EP_MSG;
	hints.caps = FI_MSG | FI_ATOMICS;
	hints.addr_format = FI_SOCKADDR;

	// run the test
	ret = run();
	
	return ret;
}
