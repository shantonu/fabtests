/*
 * Copyright (c) 2013-2015 Intel Corporation.  All rights reserved.
 * Copyright (c) 2016, Cisco Systems, Inc. All rights reserved.
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

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdlib.h>

#include "fabtest.h"

void ft_record_error(int error)
{
	if (!ft_ctrl.error) {
		fprintf(stderr, "ERROR [%s], continuing with test",
			fi_strerror(error));
		ft_ctrl.error = error;
	}
}

static int ft_init_xcontrol(struct ft_xcontrol *ctrl)
{
	memset(ctrl, 0, sizeof *ctrl);
	ctrl->credits = FT_DEFAULT_CREDITS;
	ctrl->max_credits =  FT_DEFAULT_CREDITS;
	ctrl->comp_wait = test_info.cq_wait_obj;

	ctrl->iov = calloc(ft_ctrl.iov_array[ft_ctrl.iov_cnt - 1], sizeof *ctrl->iov);
	ctrl->iov_desc = calloc(ft_ctrl.iov_array[ft_ctrl.iov_cnt - 1],
				sizeof *ctrl->iov_desc);
	if (!ctrl->iov || !ctrl->iov_desc)
		return -FI_ENOMEM;

	return 0;
}

static int ft_init_rx_control(void)
{
	int ret;

	ret= ft_init_xcontrol(&ft_rx_ctrl);
	if (ret)
		return ret;

	ft_rx_ctrl.cq_format = FI_CQ_FORMAT_MSG;
	ft_rx_ctrl.addr = FI_ADDR_UNSPEC;

	rx_size = med_size_array[med_size_cnt - 1];
	if (fabric_info && fabric_info->ep_attr &&
	    fabric_info->ep_attr->max_msg_size &&
	    fabric_info->ep_attr->max_msg_size < rx_size)
		rx_size = fabric_info->ep_attr->max_msg_size;

	return 0;
}

static int ft_init_tx_control(void)
{
	int ret;

	ret = ft_init_xcontrol(&ft_tx_ctrl);
	if (ret)
		return ret;

	ft_tx_ctrl.cq_format = FI_CQ_FORMAT_CONTEXT;
	ft_tx_ctrl.remote_cq_data = ft_init_cq_data(fabric_info);
	return 0;
}

static int ft_init_control(void)
{
	int ret;

	memset(&ft_ctrl, 0, sizeof ft_ctrl);
	ft_ctrl.xfer_iter = FT_DEFAULT_CREDITS;
	ft_ctrl.inc_step = test_info.test_flags & FT_FLAG_QUICKTEST ? 4 : 1;

	ft_ctrl.iov_array = sm_size_array;
	ft_ctrl.iov_cnt = sm_size_cnt;

	if (test_info.caps & FI_RMA) {
		ft_ctrl.size_array = lg_size_array;
		ft_ctrl.size_cnt = lg_size_cnt;
	} else {
		ft_ctrl.size_array = med_size_array;
		ft_ctrl.size_cnt = med_size_cnt;
	}

	ret = ft_init_rx_control();
	if (!ret)
		ret = ft_init_tx_control();
	return ret;
}

static void ft_cleanup_xcontrol(struct ft_xcontrol *ctrl)
{
	free(ctrl->iov);
	free(ctrl->iov_desc);
	memset(ctrl, 0, sizeof *ctrl);
}

static void ft_format_iov_distributed(struct iovec *iov, size_t cnt, char *buf,
		size_t len)
{
	size_t offset;
	int i;

	for (i = 0, offset = 0; i < cnt - 1; i++) {
		iov[i].iov_base = buf + offset;
		iov[i].iov_len = len / cnt;
		offset += iov[i].iov_len;
	}
	iov[i].iov_base = buf + offset;
	iov[i].iov_len = len - offset;
}

/* One class of bugs is issues involving IOV length handling. The regular
 * ft_format_iov does not catch this class because it evenly partitions the
 * entries. Instead partition them proportional to their position in the iovec.
 */
static void _ft_format_iov_weighted(struct iovec *iov, size_t cnt, char *buf,
		size_t len, int reversed)
{
	double total_parts;
	double portion;
	size_t offset;
	size_t weight;
	size_t size;
	size_t i;

	/* Get the sum of the element positions in the list and calculate the
	 * base weight.
	 */
	total_parts = ((cnt + 1.0) * cnt) / 2.0;
	portion = len / total_parts;

	for (offset = 0, i = 0; i < cnt; i++) {
		if (reversed)
			weight = cnt - i;
		else
			weight = i + 1;

		/* Get the weight for this iovec entry and round it to the
		 * nearest integer.
		 */
		size = (portion * weight) + .5;

		iov[i].iov_base = buf + offset;
		iov[i].iov_len = size;

		offset += size;
	}
}

static void ft_format_iov_weighted(struct iovec *iov, size_t cnt, char *buf,
		size_t len)
{
	_ft_format_iov_weighted(iov, cnt, buf, len, 0);
}

static void ft_format_iov_reversed(struct iovec *iov, size_t cnt, char *buf,
		size_t len)
{
	_ft_format_iov_weighted(iov, cnt, buf, len, 1);
}

static void ft_format_iov_random(struct iovec *iov, size_t cnt, char *buf,
		size_t len)
{
	size_t offset;
	size_t weight;
	size_t i;

	offset = 0;
	for (i = 0; i < cnt; i++) {
		/* If last IOV then use remaining data. */
		if (i == (cnt - 1)) {
			weight = len;
		} else {
			/* Get a weight between 1 and the remaining length minus
			 * the remaining IOV count. This is so we can reserve at
			 * least a length of 1 for every IOV.
			 */
			weight = (rand() % (len - (cnt - i))) + 1;
		}

		len -= weight;

		iov[i].iov_base = buf + offset;
		iov[i].iov_len = weight;

		offset += weight;
	}
}

void ft_format_iov(struct iovec *iov, size_t cnt, char *buf, size_t len)
{
	typedef void (*iov_formatter)(struct iovec *iov, size_t cnt, char *buf,
			size_t len);
	size_t choice;

	static iov_formatter options[] = {
		ft_format_iov_distributed,
		ft_format_iov_weighted,
		ft_format_iov_reversed,
		ft_format_iov_random
	};

	choice = rand() % ARRAY_SIZE(options);

	options[choice](iov, cnt, buf, len);
}


void ft_next_iov_cnt(struct ft_xcontrol *ctrl, size_t max_iov_cnt)
{
	ctrl->iov_iter++;
	if (ctrl->iov_iter > ft_ctrl.iov_cnt ||
	    ft_ctrl.iov_array[ctrl->iov_iter] > max_iov_cnt)
		ctrl->iov_iter = 0;
}

static int ft_fw_sync(int value)
{
	int result = -FI_EOTHER;

	if (listen_sock < 0) {
		ft_fw_send(sock, &value,  sizeof value);
		ft_fw_recv(sock, &result, sizeof result);
	} else {
		ft_fw_recv(sock, &result, sizeof result);
		ft_fw_send(sock, &value,  sizeof value);
	}

	return result;
}

static int ft_sync_test(int value)
{
	int ret;

	ret = ft_reset_ep();
	if (ret)
		return ret;

	return ft_fw_sync(value);
}

static int ft_pingpong(void)
{
	int ret, i;

	// TODO: current flow will not handle manual progress mode
	// it can get stuck with both sides receiving
	if (listen_sock < 0) {
		for (i = 0; i < ft_ctrl.xfer_iter; i++) {
			ret = ft_send_msg();
			if (ret)
				return ret;

			ret = ft_recv_msg();
			if (ret)
				return ret;
		}
	} else {
		for (i = 0; i < ft_ctrl.xfer_iter; i++) {
			ret = ft_recv_msg();
			if (ret)
				return ret;

			ret = ft_send_msg();
			if (ret)
				return ret;
		}
	}

	return 0;
}

static int ft_pingpong_dgram(void)
{
	int ret, i;

	if (listen_sock < 0) {
		for (i = 0; i < ft_ctrl.xfer_iter; i++) {
			ret = ft_sendrecv_dgram();
			if (ret)
				return ret;
		}
	} else {
		for (i = 0; i < 1000; i++) {
			ret = ft_recv_dgram();
			if (!ret)
				break;
			else if (ret != -FI_ETIMEDOUT)
				return ret;
		}

		for (i = 0; i < ft_ctrl.xfer_iter - 1; i++) {
			ret = ft_sendrecv_dgram();
			if (ret)
				return ret;
		}

		ret = ft_send_dgram();
		if (ret)
			return ret;
	}

	return 0;
}

static int ft_run_latency(void)
{
	int ret, i;

	for (i = 0; i < ft_ctrl.size_cnt; i += ft_ctrl.inc_step) {
		tx_size = ft_ctrl.size_array[i];
		if (tx_size > fabric_info->ep_attr->max_msg_size)
			break;

		if (((test_info.class_function == FT_FUNC_INJECT) ||
			(test_info.class_function == FT_FUNC_INJECTDATA)) &&
			(tx_size > fabric_info->tx_attr->inject_size))
			break;

		ft_ctrl.xfer_iter = test_info.test_flags & FT_FLAG_QUICKTEST ?
				5 : size_to_count(tx_size);

		ret = ft_sync_test(0);
		if (ret)
			return ret;

		ret = ft_post_recv_bufs();
		if (ret)
			return ret;

		clock_gettime(CLOCK_MONOTONIC, &start);
		ret = (test_info.ep_type == FI_EP_DGRAM) ?
			ft_pingpong_dgram() : ft_pingpong();
		clock_gettime(CLOCK_MONOTONIC, &end);
		if (ret) {
			FT_PRINTERR("latency test failed!", ret);
			return ret;
		}

		show_perf("lat", tx_size, ft_ctrl.xfer_iter, &start, &end, 2);
	}

	return 0;
}

static int ft_bw(void)
{
	int ret, i;

	if (listen_sock < 0) {
		for (i = 0; i < ft_ctrl.xfer_iter; i++) {
			ret = ft_send_msg();
			if (ret)
				return ret;
		}

		ret = ft_recv_msg();
		if (ret)
			return ret;
	} else {
		for (i = 0; i < ft_ctrl.xfer_iter; i += ft_rx_ctrl.credits) {
			ret = ft_post_recv_bufs();
			if (ret)
				return ret;

			ret = ft_comp_rx(0);
			if (ret)
				return ret;
                }

		ret = ft_send_msg();
		if (ret)
			return ret;
	}

	return 0;
}

/*
 * The datagram streaming test sends datagrams with the initial byte
 * of the message cleared until we're ready to end the test.  The first
 * byte is then set to 0xFF.  On the receive side, we count the number
 * of completions until that message is seen.  Only the receiving side
 * reports any performance data.  The sender does not know how many
 * packets were dropped in flight.
 *
 * Because we re-use the same buffer for all messages, the receiving
 * side can notice that the first byte has changed and end the test
 * before the completion associated with the last message has been
 * written to the CQ.  As a result, the number of messages that were
 * counted as received may be slightly lower than the number of messages
 * that were actually received.
 *
 * For a significantly large number of transfers, this falls into the
 * noise, but it is visible if the number of iterations is small, such
 * as when running the quick test.  The fix for this would either to use
 * CQ data to exchange the end of test marker, or to allocate separate
 * buffers for each receive operation.
 *
 * The message with the end of test marker is retried until until the
 * receiver acknowledges it.  If the receiver ack message is lost, the
 * bandwidth test will hang.  However, this is the only message that the
 * receiver sends, so there's a reasonably good chance of it being transmitted
 * successfully.
 */
static int ft_bw_dgram(size_t *recv_cnt)
{
	int ret;

	if (listen_sock < 0) {
		*recv_cnt = 0;
		ret = ft_send_dgram_flood();
		if (ret)
			return ret;

		ft_tx_ctrl.seqno = ~0;
		ret = ft_sendrecv_dgram();
	} else {
		ret = ft_recv_dgram_flood(recv_cnt);
		if (ret)
			return ret;

		ret = ft_send_dgram();
	}

	return ret;
}

static int ft_run_bandwidth(void)
{
	size_t recv_cnt;
	int ret, i;

	for (i = 0; i < ft_ctrl.size_cnt; i += ft_ctrl.inc_step) {
		tx_size = ft_ctrl.size_array[i];
		if (tx_size > fabric_info->ep_attr->max_msg_size)
			break;

		if (((test_info.class_function == FT_FUNC_INJECT) ||
			(test_info.class_function == FT_FUNC_INJECTDATA)) &&
			(tx_size > fabric_info->tx_attr->inject_size))
			break;

		ft_ctrl.xfer_iter = test_info.test_flags & FT_FLAG_QUICKTEST ?
				5 : size_to_count(tx_size);
		recv_cnt = ft_ctrl.xfer_iter;

		ret = ft_sync_test(0);
		if (ret)
			return ret;

		ret = ft_post_recv_bufs();
		if (ret)
			return ret;

		clock_gettime(CLOCK_MONOTONIC, &start);
		ret = (test_info.ep_type == FI_EP_DGRAM) ?
			ft_bw_dgram(&recv_cnt) : ft_bw();
		clock_gettime(CLOCK_MONOTONIC, &end);
		if (ret) {
			FT_PRINTERR("bw test failed!", ret);
			return ret;
		}

		show_perf("bw", tx_size, recv_cnt, &start, &end, 1);
	}

	return 0;
}

static void ft_cleanup(void)
{
	ft_cleanup_xcontrol(&ft_rx_ctrl);
	ft_cleanup_xcontrol(&ft_tx_ctrl);
	memset(&ft_ctrl, 0, sizeof ft_ctrl);
	FT_CLOSE_FID(rx_mr);
	FT_CLOSE_FID(tx_mr);
	ft_free_res();
}

int ft_run_test()
{
	int ret;

	ret = ft_init_control();
	if (ret) {
		FT_PRINTERR("ft_init_control", ret);
		goto cleanup;
	}

	ret = ft_open_control();
	if (ret) {
		FT_PRINTERR("ft_open_control", ret);
		goto cleanup;
	}
	if (test_info.ep_type == FI_EP_MSG && listen_sock >= 0) {
		ret = ft_open_passive();
		if (ret) {
			FT_PRINTERR("ft_open_passive", ret);
			goto cleanup;
		}
	}
	else {
		ret = ft_open_active();
		if (ret) {
			FT_PRINTERR("ft_open_active", ret);
			goto cleanup;
		}
	}

	ft_fw_sync(0);

	ret = ft_enable_comm();
	if (ret) {
		FT_PRINTERR("ft_enable_comm", ret);
		goto cleanup;
	}

	switch (test_info.test_type) {
	case FT_TEST_LATENCY:
		ret = ft_run_latency();
		if (ret)
			FT_PRINTERR("ft_run_latency", ret);
		break;
	case FT_TEST_BANDWIDTH:
		ret = ft_run_bandwidth();
		if (ret)
			FT_PRINTERR("ft_run_bandwidth", ret);
		break;
	default:
		ret = -FI_ENOSYS;
		break;
	}

	ft_sync_test(0);
cleanup:
	ft_cleanup();

	return ret ? ret : -ft_ctrl.error;
}
