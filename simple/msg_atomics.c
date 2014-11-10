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
static int transfer_size = 1000;
static int max_credits = 128;
static char test_name[10] = "custom";
static struct timeval start, end;
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
static struct fi_context *fi_context;
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
	fprintf(stderr, "\t[-o min|max|sum|prod|lor|land|bor|band|lxor|bxor|"
		"\n\tread|write|cswap|cswap_ne|cswap_le|cswap_lt|cswap_ge|"
		"\n\tcswap_gt|mswap] (default: min)\n");
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
		case FI_BOR: return "bor";
		case FI_BAND: return "band";
		case FI_LXOR: return "xlor";
		case FI_BXOR: return "bxor";
		case FI_ATOMIC_READ: return "read";
		case FI_ATOMIC_WRITE: return "write";
		case FI_CSWAP: return "cswap";
		case FI_CSWAP_NE: return "cswap_ne";
		case FI_CSWAP_LE: return "cswap_le";
		case FI_CSWAP_LT: return "cswap_lt";
		case FI_CSWAP_GE: return "cswap_ge";
		case FI_CSWAP_GT: return "cswap_gt";
		case FI_MSWAP: return "mswap";
		
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
	else if (!strcmp(op, "bor"))
		return FI_BOR;
	else if (!strcmp(op, "band"))
		return FI_BAND;
	else if (!strcmp(op, "lxor"))
		return FI_LXOR;
	else if (!strcmp(op, "bxor"))
		return FI_BXOR;
	else if (!strcmp(op, "read"))
		return FI_ATOMIC_READ;
	else if (!strcmp(op, "write"))
		return FI_ATOMIC_WRITE;
	else if (!strcmp(op, "cswap"))
		return FI_CSWAP;
	else if (!strcmp(op, "cswap_ne"))
		return FI_CSWAP_NE;
	else if (!strcmp(op, "cswap_le"))
		return FI_CSWAP_LE;
	else if (!strcmp(op, "cswap_lt"))
		return FI_CSWAP_LT;
	else if (!strcmp(op, "cswap_ge"))
		return FI_CSWAP_GE;
	else if (!strcmp(op, "cswap_gt"))
		return FI_CSWAP_GT;
	else if (!strcmp(op, "mswap"))
		return FI_MSWAP;
	else {
		printf("Invalid atomic operation\n");
		return -1;
	}
}

static int wait_for_completion(struct fid_cq *cq, int num_completions)
{
	int ret;
	struct fi_cq_entry comp = {0};
	struct fi_cq_err_entry err;
	
	while(num_completions > 0)
	{
		ret = fi_cq_read(cq, &comp, sizeof comp);
		if (ret > 0) {
			printf("[wait_for_completion][cq_read] %p\n", comp.op_context);
			num_completions--;
		} else if (ret < 0) {
			fi_cq_readerr(cq, &err, sizeof err, 0);
			fprintf(stderr, "Completion queue read %d (%s)\n", 
			ret, fi_strerror(-ret));
			return ret;
		}
	}
	return 0;
}

static int send_msg(int size)
{
	int ret;
        printf("[send_msg][fi_send] %p\n", fi_context);
	
	ret = fi_send(ep, buf, (size_t) size, fi_mr_desc(mr), fi_context);
	if (ret){
		fprintf(stderr, "fi_send %d (%s)\n", ret, fi_strerror(-ret));
		return ret;
	}

	return wait_for_completion(scq, 1);
}

static int post_recv(int size)
{
	int ret;
        printf("[post_recv][fi_recv] %p\n", fi_context);
	
	ret = fi_recv(ep, buf, buffer_size, fi_mr_desc(mr), fi_context);
	if (ret){
		fprintf(stderr, "fi_recv %d (%s)\n", ret, fi_strerror(-ret));
		return ret;
	}

	return ret;
}

static int send_xfer(int size)
{
        int ret;

	if(dst_addr) {
		*(int *)buf = 1; 
	} else {
		*(int *)buf = 0;
	}
	printf("sending %d\n", *(int*)buf);
	ret = fi_send(ep, buf, (size_t) size, fi_mr_desc(mr), fi_context);
        if (ret)
                fprintf(stderr, "fi_send %d (%s)\n", ret, fi_strerror(-ret));

        return ret;
}

static int recv_xfer(int size)
{
        struct fi_cq_entry comp = {0};
        int ret;

        do {
                ret = fi_cq_read(rcq, &comp, sizeof comp);
                if (ret < 0) {
                        fprintf(stderr, "Completion queue read %d (%s)\n", ret, 
			fi_strerror(-ret));
                        return ret;
                }
        } while (!ret);
        printf("[recv_xfer][fi_recv] %p\n", fi_context);
	printf("received %d\n", *(int *)buf);

        ret = fi_recv(ep, buf, buffer_size, fi_mr_desc(mr), fi_context);
        if (ret)
                fprintf(stderr, "fi_recv %d (%s)\n", ret, fi_strerror(-ret));

        return ret;
}

static int sync_test(void)
{
        int ret;

	ret = dst_addr ? send_xfer(16) : recv_xfer(16);
        if (ret)
                return ret;

        return dst_addr ? recv_xfer(16) : send_xfer(16);
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
	printf("[atomic] %p\n", fi_context);
	
	ret = fi_atomic(ep, buf, 1, fi_mr_desc(mr), remote.addr, 
		remote.key, datatype, op, fi_context);
        if (ret) {		
		fprintf(stderr, "fi_atomic %d (%s)\n", ret, 
			fi_strerror(-ret));
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
	printf("[atomic] %p\n", fi_context);
	
	ret = fi_fetch_atomic(ep, buf, 1, fi_mr_desc(mr), result, 
		fi_mr_desc(mr_result), remote.addr, remote.key, 
		datatype, op, fi_context);
        if (ret) {		
		fprintf(stderr, "fi_fetch_atomic %d (%s)\n", 
			ret, fi_strerror(-ret));
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
	printf("[atomic] %p\n", fi_context);

	ret = fi_compare_atomic(ep, buf, 1, fi_mr_desc(mr), 
		compare, fi_mr_desc(mr_compare),result, 
		fi_mr_desc(mr_result), remote.addr, remote.key, 
		datatype, op, fi_context);
        if (ret) {
           	fprintf(stderr, "fi_compare_atomic %d (%s)\n", 
			ret, fi_strerror(-ret));
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
	
	sync_test();

	gettimeofday(&start, NULL);
	if(run_all) {  
	
		switch(0) {	
			// atomic operations usable with base atomic and 
			// fetch atomic functions
			case FI_MIN:
        			ret = is_valid_base_atomic_op(FI_MIN);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_MIN);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_MIN);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_MIN);
					}
        			}	                                           
			case FI_MAX:
        			ret = is_valid_base_atomic_op(FI_MAX);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_MAX);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_MAX);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_MAX);
					}
        			}	                                           
			case FI_SUM:
        			ret = is_valid_base_atomic_op(FI_SUM);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_SUM);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_SUM);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_SUM);
					}
        			}	                                           
			case FI_PROD:
        			ret = is_valid_base_atomic_op(FI_PROD);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_PROD);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_PROD);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_PROD);
					}
        			}	                                           
			case FI_LOR:
        			ret = is_valid_base_atomic_op(FI_LOR);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_LOR);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_LOR);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_LOR);
					}
        			}	                                           
			case FI_LAND:
        			ret = is_valid_base_atomic_op(FI_LAND);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_LAND);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_LAND);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_LAND);
					}
        			}	                                           
			case FI_BOR:
        			ret = is_valid_base_atomic_op(FI_BOR);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_BOR);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_BOR);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_BOR);
					}
        			}	                                           
			case FI_BAND:
        			ret = is_valid_base_atomic_op(FI_BAND);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_BAND);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_BAND);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_BAND);
					}
        			}	                                           
			case FI_LXOR:
        			ret = is_valid_base_atomic_op(FI_LXOR);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_LXOR);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_LXOR);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_LXOR);
					}
        			}	                                           
			case FI_BXOR:
        			ret = is_valid_base_atomic_op(FI_BXOR);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_BXOR);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_BXOR);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_BXOR);
					}
        			}	                                           
			case FI_ATOMIC_READ:
        			ret = is_valid_base_atomic_op(FI_ATOMIC_READ);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_ATOMIC_READ);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_ATOMIC_READ);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_ATOMIC_READ);
					}
        			}	                                           
			case FI_ATOMIC_WRITE:
        			ret = is_valid_base_atomic_op(FI_ATOMIC_WRITE);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(FI_ATOMIC_WRITE);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(FI_ATOMIC_WRITE);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(FI_ATOMIC_WRITE);
					}
        			}	                                           
			
			// compare atomic functions 
			case FI_CSWAP:
				ret = is_valid_compare_atomic_op(FI_CSWAP);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_CSWAP);
        			}
			case FI_CSWAP_NE:
				ret = is_valid_compare_atomic_op(FI_CSWAP_NE);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_CSWAP_NE);
        			}
			case FI_CSWAP_LE:
				ret = is_valid_compare_atomic_op(FI_CSWAP_LE);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_CSWAP_LE);
        			}
			case FI_CSWAP_LT:
				ret = is_valid_compare_atomic_op(FI_CSWAP_LT);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_CSWAP_LT);
        			}
			case FI_CSWAP_GE:
				ret = is_valid_compare_atomic_op(FI_CSWAP_GE);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_CSWAP_GE);
        			}
			case FI_CSWAP_GT:
				ret = is_valid_compare_atomic_op(FI_CSWAP_GT);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_CSWAP_GT);
        			}
			case FI_MSWAP:
				ret = is_valid_compare_atomic_op(FI_MSWAP);
        			if (ret > 0) {
					execute_compare_atomic_op(FI_MSWAP);
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
			case FI_BOR:
			case FI_BAND:
			case FI_LXOR:
			case FI_BXOR:
			case FI_ATOMIC_READ:
			case FI_ATOMIC_WRITE:
        			ret = is_valid_base_atomic_op(op_type);
        			if (ret > 0) {
					
					for (i = 0; i < iterations; i++) {	
						execute_base_atomic_op(op_type);
					}
        			}
				
				ret = is_valid_fetch_atomic_op(op_type);
        			if (ret > 0) {	
					for (i = 0; i < iterations; i++) {	
						execute_fetch_atomic_op(op_type);
					}
        			}	
      				break;
                                                                                   
			// compare atomic functions 
			case FI_CSWAP:
			case FI_CSWAP_NE:
			case FI_CSWAP_LE:
			case FI_CSWAP_LT:
			case FI_CSWAP_GE:
			case FI_CSWAP_GT:
			case FI_MSWAP:
				ret = is_valid_compare_atomic_op(op_type);
        			if (ret > 0) {
					execute_compare_atomic_op(op_type);
        			} else {
					goto out;
				}
				break;
			default:
				goto out;
				break;				
		}
	}
	gettimeofday(&end, NULL);
		
	fprintf(stderr, "%-10s%-8s%-8s%-8s%-8s%8s %10s%13s\n",
	       "name", "bytes", "xfers", "iters", "total", "time", 
		"Gb/sec", "usec/xfer");

	show_perf(start, end, transfer_size, iterations, test_name, 0);
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
	free(fi_context);
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
	cq_attr.size = max_credits << 1;
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

	ret = fi_recv(ep, buf, buffer_size, fi_mr_desc(mr), buf);
	if(ret)
		fprintf(stderr, "fi_recv %d (%s)\n", ret, fi_strerror(-ret));

	return ret;
}

static int server_listen(void)
{
	struct fi_info *fi;
	int ret;

	hints.ep_cap |= FI_PASSIVE;
	ret = fi_getinfo(FI_VERSION(1, 0), src_addr, port, 0, &hints, &fi);
	if (ret) {
		fprintf(stderr, "fi_getinfo %s\n", strerror(-ret));
		return ret;
	}

	ret = fi_fabric(fi->fabric_attr, &fab, NULL);
	if (ret) {
		fprintf(stderr, "fi_fabric %s\n", fi_strerror(-ret));
		goto err0;
	}

	ret = fi_pendpoint(fab, fi, &pep, NULL);
	if (ret) {
		fprintf(stderr, "fi_endpoint %s\n", fi_strerror(-ret));
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
	ssize_t rd;
	int ret;

	rd = fi_eq_condread(cmeq, &entry, sizeof entry, NULL, -1, 0);
	if (rd != sizeof entry) {
		fprintf(stderr, "fi_eq_cond_read %zd %s\n", 
			rd, fi_strerror((int) -rd));
		return (int) rd;
	}

	if (entry.event != FI_CONNREQ) {
		fprintf(stderr, "Unexpected CM event %d\n", entry.event);
		ret = -FI_EOTHER;
		goto err1;
	}

	ret = fi_fdomain(fab, entry.info->domain_attr, &dom, NULL);
	if (ret) {
		fprintf(stderr, "fi_fdomain %s\n", fi_strerror(-ret));
		goto err1;
	}


	ret = fi_endpoint(dom, entry.info, &ep, NULL);
	if (ret) {
		fprintf(stderr, "fi_endpoint for req %s\n", fi_strerror(-ret));
		goto err1;
	}

	ret = alloc_ep_res(entry.info);
	if (ret)
		 goto err2;

	ret = bind_ep_res();
	if (ret)
		goto err3;

	ret = fi_accept(ep, entry.connreq, NULL, 0);
	entry.connreq = NULL;
	if (ret) {
		fprintf(stderr, "fi_accept %s\n", fi_strerror(-ret));
		goto err3;
	}

	rd = fi_eq_condread(cmeq, &entry, sizeof entry, NULL, -1, 0);
 	if (rd != sizeof entry) {
		fprintf(stderr, "fi_eq_condread %zd %s\n", 
			rd, fi_strerror((int) -rd));
		goto err3;
 	}

	if (entry.event != FI_COMPLETE || entry.fid != &ep->fid) {
 		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
			entry.event, entry.fid, ep);
 		ret = -FI_EOTHER;
 		goto err3;
 	}
 
 	fi_freeinfo(entry.info);
 	return 0;

err3:
	free_ep_res();
err2:
	fi_close(&ep->fid);
err1:

 	fi_reject(pep, entry.connreq, NULL, 0);
 	fi_freeinfo(entry.info);
 	return ret;
}

static int client_connect(void)
{
	struct fi_eq_cm_entry entry;
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

 	ret = fi_fdomain(fab, fi->domain_attr, &dom, NULL);
	if (ret) {
		fprintf(stderr, "fi_fdomain %s %s\n", fi_strerror(-ret),
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

 	rd = fi_eq_condread(cmeq, &entry, sizeof entry, NULL, -1, 0);
	if (rd != sizeof entry) {
		fprintf(stderr, "fi_eq_condread %zd %s\n", rd, fi_strerror((int) -rd));
		return (int) rd;
	}

 	if (entry.event != FI_COMPLETE || entry.fid != &ep->fid) {
 		fprintf(stderr, "Unexpected CM event %d fid %p (ep %p)\n",
 			entry.event, entry.fid, ep);
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
	int len = sizeof local;

	local.addr = (uint64_t)buf;
	local.key = fi_mr_key(mr);

	if (dst_addr) {
		*(struct addr_key *)buf = local;
		send_msg(len);
	} else {
		post_recv(len);
		wait_for_completion(rcq, 1);
		remote = *(struct addr_key *)buf;	
	}

	if (dst_addr) {
		post_recv(len);
		wait_for_completion(rcq, 1);
		remote = *(struct addr_key *)buf;

	} else {
		*(struct addr_key *)buf = local;
		send_msg(len);
	}

	return 0;
}

static int run(void)
{
	int i, ret = 0;
	fi_context = (struct fi_context *) malloc(sizeof(struct fi_context));

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
			if (op_type < 0)
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
	hints.type = FI_EP_MSG;
	hints.ep_cap = FI_MSG | FI_ATOMICS;
	hints.addr_format = FI_SOCKADDR;

	ret = run();
	if(ret)
		printf("Return value 1\n");
	return ret;
}
