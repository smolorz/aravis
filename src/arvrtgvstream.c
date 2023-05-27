/* Aravis - Digital camera library
 *
 * Copyright © 2009-2021 Emmanuel Pacaud
 * Copyright © 2023 KION Group
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 *
 * Author: Emmanuel Pacaud <emmanuel@gnome.org>
 * Author: Sebastian Smolorz <sebastian.smolorz@kiongroup.com>
 */

/**
 * SECTION: arvrtgvstream
 * @short_description: Real-Time GigEVision stream
 */

#include <arvrtgvstreamprivate.h>
#include <arvgvdeviceprivate.h>
#include <arvstreamprivate.h>
#include <arvbufferprivate.h>
#include <arvfeatures.h>
#include <arvparamsprivate.h>
#include <arvgvspprivate.h>
#include <arvgvcpprivate.h>
#include <arvdebug.h>
#include <arvmisc.h>
#include <arvmiscprivate.h>
#include <arvnetworkprivate.h>
#include <arvstr.h>
#include <arvenumtypes.h>
#include <native/timer.h>
#include <rtdm/net.h>
#include <trank/rtdm/rtdm.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#if ARAVIS_HAS_PACKET_SOCKET
#include <ifaddrs.h>
#include <netinet/udp.h>
#include <net/if.h>
#include <netinet/in.h>
#include <linux/if_packet.h>
#include <linux/if_ether.h>
#include <linux/ip.h>
#include <linux/filter.h>
#include <sys/types.h>
#include <sys/mman.h>
#endif

#define ARV_GV_STREAM_DISCARD_LATE_FRAME_THRESHOLD	100

#define ARV_RT_GV_MAX_N_FRAMES	2

enum {
	ARV_GV_STREAM_PROPERTY_0,
	ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER,
	ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER_SIZE,
	ARV_GV_STREAM_PROPERTY_PACKET_RESEND,
	ARV_GV_STREAM_PROPERTY_PACKET_REQUEST_RATIO,
	ARV_GV_STREAM_PROPERTY_INITIAL_PACKET_TIMEOUT,
	ARV_GV_STREAM_PROPERTY_PACKET_TIMEOUT,
	ARV_GV_STREAM_PROPERTY_FRAME_RETENTION
} ArvRtGvStreamProperties;

typedef struct _ArvGvStreamThreadData ArvGvStreamThreadData;

typedef struct {
	GThread *thread;
	ArvGvStreamThreadData *thread_data;
} ArvRtGvStreamPrivate;

struct _ArvRtGvStream {
	ArvStream	stream;
};

struct _ArvRtGvStreamClass {
	ArvStreamClass parent_class;
};

G_DEFINE_TYPE_WITH_CODE (ArvRtGvStream, arv_rt_gv_stream, ARV_TYPE_STREAM, G_ADD_PRIVATE (ArvRtGvStream))

/* Acquisition thread */

typedef struct {
	gboolean received;
        gboolean resend_requested;
	guint64 abs_timeout_us;
} ArvGvStreamPacketData;

typedef struct {
	ArvBuffer *buffer;
	guint64 frame_id;

	gint32 last_valid_packet;
	guint64 first_packet_time_us;
	guint64 last_packet_time_us;

	gboolean disable_resend_request;

	guint n_packets;
	ArvGvStreamPacketData *packet_data;

	guint n_packet_resend_requests;
	gboolean resend_ratio_reached;

	gboolean extended_ids;
} ArvGvStreamFrameData;

struct _ArvGvStreamThreadData {
	GCancellable *cancellable;

	ArvStream *stream;

        gboolean thread_started;
        GMutex thread_started_mutex;
        GCond thread_started_cond;

	ArvStreamCallback callback;
	void *callback_data;

	int socket;
	GInetAddress *interface_address;
	GSocketAddress *interface_socket_address;
	GInetAddress *device_address;
	GSocketAddress *device_socket_address;
	struct sockaddr_in sockaddr_device;
	guint16 source_stream_port;
	guint16 stream_port;

	ArvGvStreamPacketResend packet_resend;
	double packet_request_ratio;
	guint initial_packet_timeout_us;
	guint packet_timeout_us;
	guint frame_retention_us;

	int64_t last_timeout_ns;

	guint64 timestamp_tick_frequency;
	guint scps_packet_size;

	guint16 packet_id;

	ArvGvStreamFrameData frames[ARV_RT_GV_MAX_N_FRAMES];
	size_t n_frames;
	ArvBuffer *input_buffers[ARV_RT_GV_MAX_N_FRAMES];
	size_t n_input_buffers;
	ArvBuffer *output_buffers[ARV_RT_GV_MAX_N_FRAMES];
	size_t n_output_buffers;
	ArvGvStreamPacketData *packet_data[ARV_RT_GV_MAX_N_FRAMES];
	size_t max_n_packets;

	gboolean first_packet;
	guint64 last_frame_id;

	gboolean use_packet_socket;

	/* Statistics */

	guint64 n_completed_buffers;
	guint64 n_failures;
	guint64 n_underruns;
	guint64 n_timeouts;
	guint64 n_aborteds;
	guint64 n_missing_frames;

	guint64 n_size_mismatch_errors;

	guint64 n_received_packets;
	guint64 n_missing_packets;
	guint64 n_error_packets;
	guint64 n_ignored_packets;
	guint64 n_resend_requests;
	guint64 n_resent_packets;
	guint64 n_resend_ratio_reached;
        guint64 n_resend_disabled;
	guint64 n_duplicated_packets;

        guint64 n_transferred_bytes;
        guint64 n_ignored_bytes;

	ArvHistogram *histogram;
	guint32 statistic_count;

	ArvGvStreamSocketBuffer socket_buffer_option;
	int socket_buffer_size;
	int current_socket_buffer_size;

	ArvGvspPacket *packet_buffer;
	guint packet_buffer_size;
};

static void
_send_packet_request (ArvGvStreamThreadData *thread_data,
		      guint64 frame_id,
		      guint32 first_block,
		      guint32 last_block,
		      gboolean extended_ids)
{
	ArvGvcpPacket *packet;
	size_t packet_size;
	int ret;

	thread_data->packet_id = arv_gvcp_next_packet_id (thread_data->packet_id);

	packet = arv_gvcp_packet_new_packet_resend_cmd (frame_id, first_block, last_block, extended_ids,
							thread_data->packet_id, &packet_size);

	arv_debug_stream_thread ("[RtGvStream::send_packet_request] frame_id = %" G_GUINT64_FORMAT
			       " (from packet %" G_GUINT32_FORMAT " to %" G_GUINT32_FORMAT ")",
			       frame_id, first_block, last_block);

	arv_gvcp_packet_debug (packet, ARV_DEBUG_LEVEL_DEBUG);

	ret = rt_dev_sendto(thread_data->socket, packet, packet_size, 0,
				(struct sockaddr*)&thread_data->sockaddr_device,
				sizeof(thread_data->sockaddr_device));
	if (ret < 0)
		arv_warning_stream_thread("[RtGvStream::send_packet_request] rt_dev_sendto error: %d", ret);

	arv_gvcp_packet_free (packet);
}

#if 0
static void
_update_socket (ArvGvStreamThreadData *thread_data, ArvBuffer *buffer)
{
	int buffer_size = thread_data->current_socket_buffer_size;
	int fd;

	if (thread_data->socket_buffer_option == ARV_GV_STREAM_SOCKET_BUFFER_FIXED &&
	    thread_data->socket_buffer_size <= 0)
		return;

	fd = g_socket_get_fd (thread_data->socket);

	switch (thread_data->socket_buffer_option) {
		case ARV_GV_STREAM_SOCKET_BUFFER_FIXED:
			buffer_size = thread_data->socket_buffer_size;
			break;
		case ARV_GV_STREAM_SOCKET_BUFFER_AUTO:
			if (thread_data->socket_buffer_size <= 0)
				buffer_size = buffer->priv->size;
			else
				buffer_size = MIN (buffer->priv->size, thread_data->socket_buffer_size);
			break;
	}

	if (buffer_size != thread_data->current_socket_buffer_size) {
		gboolean result;

		result = arv_socket_set_recv_buffer_size (fd, buffer_size);
		if (result) {
			thread_data->current_socket_buffer_size = buffer_size;
			arv_info_stream_thread ("[GvStream::update_socket] Socket buffer size set to %d", buffer_size);
		} else {
			arv_warning_stream_thread ("[GvStream::update_socket] Failed to set socket buffer size to %d (%d)",
						   buffer_size, errno);
		}
	}
}
#endif

static void
_process_data_leader (ArvGvStreamThreadData *thread_data,
		      ArvGvStreamFrameData *frame,
		      const ArvGvspPacket *packet,
		      guint32 packet_id)
{
	if (frame->buffer->priv->status != ARV_BUFFER_STATUS_FILLING)
		return;

	if (packet_id != 0) {
		frame->buffer->priv->status = ARV_BUFFER_STATUS_WRONG_PACKET_ID;
		return;
	}

	frame->buffer->priv->payload_type = arv_gvsp_packet_get_buffer_payload_type (packet);
	frame->buffer->priv->frame_id = frame->frame_id;
	frame->buffer->priv->chunk_endianness = G_BIG_ENDIAN;

	frame->buffer->priv->system_timestamp_ns = g_get_real_time() * 1000LL;
	if (frame->buffer->priv->payload_type != ARV_BUFFER_PAYLOAD_TYPE_H264) {
		if (G_LIKELY (thread_data->timestamp_tick_frequency != 0))
			frame->buffer->priv->timestamp_ns = arv_gvsp_packet_get_timestamp (packet,
											   thread_data->timestamp_tick_frequency);
		else {
			frame->buffer->priv->timestamp_ns = frame->buffer->priv->system_timestamp_ns;
		}
	} else
		frame->buffer->priv->timestamp_ns = frame->buffer->priv->system_timestamp_ns;

	if (arv_buffer_payload_type_has_aoi (frame->buffer->priv->payload_type)) {
		frame->buffer->priv->x_offset = arv_gvsp_packet_get_x_offset (packet);
		frame->buffer->priv->y_offset = arv_gvsp_packet_get_y_offset (packet);
		frame->buffer->priv->width = arv_gvsp_packet_get_width (packet);
		frame->buffer->priv->height = arv_gvsp_packet_get_height (packet);
		frame->buffer->priv->pixel_format = arv_gvsp_packet_get_pixel_format (packet);
	}

	if (frame->packet_data[packet_id].resend_requested) {
		thread_data->n_resent_packets++;
		arv_debug_stream_thread ("[GvStream::process_data_leader] Received resent packet %u for frame %" G_GUINT64_FORMAT,
				       packet_id, frame->frame_id);
	}
}

static void
_process_data_block (ArvGvStreamThreadData *thread_data,
		     ArvGvStreamFrameData *frame,
		     const ArvGvspPacket *packet,
		     guint32 packet_id,
		     size_t read_count)
{
	size_t block_size;
	ptrdiff_t block_offset;
	ptrdiff_t block_end;
	gboolean extended_ids;

	if (frame->buffer->priv->status != ARV_BUFFER_STATUS_FILLING)
		return;

	if (packet_id > frame->n_packets - 2 || packet_id < 1) {
		arv_gvsp_packet_debug (packet, read_count, ARV_DEBUG_LEVEL_INFO);
		frame->buffer->priv->status = ARV_BUFFER_STATUS_WRONG_PACKET_ID;
		return;
	}

	extended_ids = arv_gvsp_packet_has_extended_ids (packet);

	block_size = arv_gvsp_packet_get_data_size (packet, read_count);
	block_offset = (packet_id - 1) * (thread_data->scps_packet_size - (extended_ids ?
									   ARV_GVSP_PACKET_EXTENDED_PROTOCOL_OVERHEAD :
									   ARV_GVSP_PACKET_PROTOCOL_OVERHEAD));
	block_end = block_size + block_offset;

	if (block_end > frame->buffer->priv->size) {
		arv_info_stream_thread ("[GvStream::process_data_block] %" G_GINTPTR_FORMAT " unexpected bytes in packet %u "
					 " for frame %" G_GUINT64_FORMAT,
					 block_end - frame->buffer->priv->size,
					 packet_id, frame->frame_id);
		thread_data->n_size_mismatch_errors++;

		block_end = frame->buffer->priv->size;
		block_size = block_end - block_offset;
	}

	memcpy (((char *) frame->buffer->priv->data) + block_offset, arv_gvsp_packet_get_data (packet), block_size);

	if (frame->packet_data[packet_id].resend_requested) {
		thread_data->n_resent_packets++;
		arv_debug_stream_thread ("[GvStream::process_data_block] Received resent packet %u for frame %" G_GUINT64_FORMAT,
				       packet_id, frame->frame_id);
	}
}

static void
_process_data_trailer (ArvGvStreamThreadData *thread_data,
		       ArvGvStreamFrameData *frame,
		       guint32 packet_id)
{
	if (frame->buffer->priv->status != ARV_BUFFER_STATUS_FILLING)
		return;

	if (packet_id != frame->n_packets - 1) {
		frame->buffer->priv->status = ARV_BUFFER_STATUS_WRONG_PACKET_ID;
		return;
	}

	if (frame->packet_data[packet_id].resend_requested) {
		thread_data->n_resent_packets++;
		arv_debug_stream_thread ("[GvStream::process_data_trailer] Received resent packet %u for frame %" G_GUINT64_FORMAT,
				       packet_id, frame->frame_id);
	}
}

static ArvGvStreamFrameData *
_find_frame_data (ArvGvStreamThreadData *thread_data,
		  const ArvGvspPacket *packet,
		  size_t packet_size,
		  guint64 frame_id,
		  guint32 packet_id,
		  gboolean extended_ids,
		  size_t read_count,
		  guint64 time_us)
{
	ArvGvStreamFrameData *frame;
	ArvBuffer *buffer;
	guint n_packets = 0;
	gint64 frame_id_inc;
	guint32 block_size;
	size_t i, new_frame_index;

	for (i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
		frame = &thread_data->frames[i];
		if (frame->buffer) {
			if (frame->frame_id == frame_id) {
				arv_histogram_fill (thread_data->histogram, 1, time_us - frame->first_packet_time_us);
				arv_histogram_fill (thread_data->histogram, 2, time_us - frame->last_packet_time_us);

				frame->last_packet_time_us = time_us;
				return frame;
			}
		}
	}

#if 0
	if (thread_data->n_frames == ARV_RT_GV_MAX_N_FRAMES) {
		arv_warning_stream_thread("[RtGvStream::find_frame_data] Maximum number of open frames "
						"reached, cannot open new frame!");
		return NULL;
	}
#endif

	if (extended_ids) {
		frame_id_inc = (gint64) frame_id - (gint64) thread_data->last_frame_id;
		/* Frame id 0 is not a valid value */
		if ((gint64) frame_id > 0 && (gint64) thread_data->last_frame_id < 0)
			frame_id_inc--;
	} else {
		frame_id_inc = (gint16) frame_id - (gint16) thread_data->last_frame_id;
		/* Frame id 0 is not a valid value */
		if ((gint16) frame_id > 0 && (gint16) thread_data->last_frame_id < 0)
			frame_id_inc--;
	}

	if (frame_id_inc < 1  && frame_id_inc > -ARV_GV_STREAM_DISCARD_LATE_FRAME_THRESHOLD) {
		arv_info_stream_thread ("[RtGvStream::find_frame_data] Discard late frame %" G_GUINT64_FORMAT
					 " (last: %" G_GUINT64_FORMAT ")",
					 frame_id, thread_data->last_frame_id);
		arv_gvsp_packet_debug (packet, packet_size, ARV_DEBUG_LEVEL_INFO);
		return NULL;
	}

	buffer = NULL;
	if (thread_data->n_input_buffers > 0) {
		for (int i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
			if (thread_data->input_buffers[i] != NULL) {
				buffer = thread_data->input_buffers[i];
				thread_data->input_buffers[i] = NULL;
				thread_data->n_input_buffers--;
				break;
			}
		}
	}

	if (buffer == NULL) {
		thread_data->n_underruns++;

		return NULL;
	}

	block_size = thread_data->scps_packet_size -
		(extended_ids ? ARV_GVSP_PACKET_EXTENDED_PROTOCOL_OVERHEAD : ARV_GVSP_PACKET_PROTOCOL_OVERHEAD);

	for (new_frame_index = 0; new_frame_index < ARV_RT_GV_MAX_N_FRAMES; new_frame_index++) {
		frame = &thread_data->frames[new_frame_index];
		if (frame->buffer == NULL)
			break;
	}

	memset(frame, 0, sizeof(ArvGvStreamFrameData));
	thread_data->n_frames++;

	frame->disable_resend_request = FALSE;

	frame->frame_id = frame_id;
	frame->last_valid_packet = -1;

	frame->buffer = buffer;
//	_update_socket (thread_data, frame->buffer);
	frame->buffer->priv->status = ARV_BUFFER_STATUS_FILLING;
	n_packets = (frame->buffer->priv->size + block_size - 1) / block_size + 2;

	frame->first_packet_time_us = time_us;
	frame->last_packet_time_us = time_us;

	if (thread_data->max_n_packets >= n_packets) {
		frame->packet_data = thread_data->packet_data[new_frame_index];
		memset(frame->packet_data, 0, sizeof(ArvGvStreamPacketData) * thread_data->max_n_packets);
	} else {
		frame->packet_data = g_new0 (ArvGvStreamPacketData, n_packets);
	}
	frame->n_packets = n_packets;

	if (thread_data->callback != NULL &&
	    frame->buffer != NULL)
		thread_data->callback (thread_data->callback_data,
				       ARV_STREAM_CALLBACK_TYPE_START_BUFFER,
				       NULL);

	thread_data->last_frame_id = frame_id;

	if (frame_id_inc > 1) {
		thread_data->n_missing_frames++;
		arv_debug_stream_thread ("[RtGvStream::find_frame_data] Missed %" G_GINT64_FORMAT " frame(s) before %" G_GUINT64_FORMAT,
				       frame_id_inc - 1, frame_id);
	}

	arv_debug_stream_thread ("[RtGvStream::find_frame_data] Start frame %" G_GUINT64_FORMAT, frame_id);

	frame->extended_ids = extended_ids;

        arv_histogram_fill (thread_data->histogram, 1, 0);

	return frame;
}

static void
_missing_packet_check (ArvGvStreamThreadData *thread_data,
		       ArvGvStreamFrameData *frame,
		       guint32 packet_id,
		       guint64 time_us)
{
	int i;

	if (thread_data->packet_resend == ARV_GV_STREAM_PACKET_RESEND_NEVER ||
	    frame->disable_resend_request ||
	    frame->resend_ratio_reached)
		return;

	if ((int) (frame->n_packets * thread_data->packet_request_ratio) <= 0)
		return;

	if (packet_id < frame->n_packets) {
		int first_missing = -1;

		for (i = frame->last_valid_packet + 1; i <= packet_id + 1; i++) {
			gboolean need_resend;

			if (i <= packet_id && !frame->packet_data[i].received) {
                                if (frame->packet_data[i].abs_timeout_us == 0)
                                        frame->packet_data[i].abs_timeout_us = time_us +
                                                thread_data->initial_packet_timeout_us;
                                need_resend = time_us > frame->packet_data[i].abs_timeout_us;
                        } else
                                need_resend = FALSE;

			if (need_resend) {
				if (first_missing < 0)
					first_missing = i;
			}

			if (i > packet_id || !need_resend) {
				if (first_missing >= 0) {
					int last_missing;
					int n_missing_packets;
					int j;

					last_missing = i - 1;
					n_missing_packets = last_missing - first_missing + 1;

					if (frame->n_packet_resend_requests + n_missing_packets >
					    (frame->n_packets * thread_data->packet_request_ratio)) {
						frame->n_packet_resend_requests += n_missing_packets;

						arv_info_stream_thread ("[GvStream::missing_packet_check]"
									 " Maximum number of requests "
									 "reached at dt = %" G_GINT64_FORMAT
									 ", n_packet_requests = %u (%u packets/frame), frame_id = %"
									 G_GUINT64_FORMAT,
									 time_us - frame->first_packet_time_us,
									 frame->n_packet_resend_requests, frame->n_packets,
									 frame->frame_id);

						thread_data->n_resend_ratio_reached++;
						frame->resend_ratio_reached = TRUE;

						return;
					}

					arv_debug_stream_thread ("[GvStream::missing_packet_check]"
							       " Resend request at dt = %" G_GINT64_FORMAT
							       ", packet id = %u (%u packets/frame)",
							       time_us - frame->first_packet_time_us,
							       packet_id, frame->n_packets);

					_send_packet_request (thread_data,
							      frame->frame_id,
							      first_missing,
							      last_missing,
							      frame->extended_ids);

					for (j = first_missing; j <= last_missing; j++) {
						frame->packet_data[j].abs_timeout_us = time_us +
                                                        thread_data->packet_timeout_us;
                                                frame->packet_data[j].resend_requested = TRUE;
                                        }

					thread_data->n_resend_requests += n_missing_packets;

					first_missing = -1;
				}
			}
		}
	}
}

static void
_close_frame (ArvGvStreamThreadData *thread_data,
              guint64 time_us,
              ArvGvStreamFrameData *frame)
{
	if (frame->buffer->priv->status == ARV_BUFFER_STATUS_SUCCESS)
		thread_data->n_completed_buffers++;
	else
		if (frame->buffer->priv->status != ARV_BUFFER_STATUS_ABORTED)
			thread_data->n_failures++;

	if (frame->buffer->priv->status == ARV_BUFFER_STATUS_TIMEOUT)
		thread_data->n_timeouts++;

	if (frame->buffer->priv->status == ARV_BUFFER_STATUS_ABORTED)
		thread_data->n_aborteds++;

	if (frame->buffer->priv->status != ARV_BUFFER_STATUS_SUCCESS &&
	    frame->buffer->priv->status != ARV_BUFFER_STATUS_ABORTED)
		thread_data->n_missing_packets += (int) frame->n_packets - (frame->last_valid_packet + 1);

	for (int i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
		if (thread_data->output_buffers[i] == NULL) {
			thread_data->output_buffers[i] = frame->buffer;
			thread_data->n_output_buffers++;
			break;
		}
	}

	if (thread_data->callback != NULL)
		thread_data->callback (thread_data->callback_data,
				       ARV_STREAM_CALLBACK_TYPE_BUFFER_DONE,
				       frame->buffer);

        arv_histogram_fill (thread_data->histogram, 0,
                            time_us - frame->first_packet_time_us);

	arv_debug_stream_thread ("[GvStream::close_frame] Close frame %" G_GUINT64_FORMAT, frame->frame_id);

	frame->buffer = NULL;
	frame->frame_id = 0;

	if (thread_data->max_n_packets < frame->n_packets)
		g_free (frame->packet_data);

	thread_data->n_frames--;
}

static gboolean
_check_frame_completion (ArvGvStreamThreadData *thread_data,
			 guint64 time_us,
			 ArvGvStreamFrameData *current_frame)
{
	ArvGvStreamFrameData *frame;
	gboolean can_close_frame = TRUE;
	gboolean frame_closed = FALSE;
	size_t i;

	for (i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
		frame = &thread_data->frames[i];
		if (frame->buffer) {

			if (can_close_frame &&
			    thread_data->packet_resend == ARV_GV_STREAM_PACKET_RESEND_NEVER &&
			    thread_data->n_frames > 1) {
				frame->buffer->priv->status = ARV_BUFFER_STATUS_MISSING_PACKETS;
				arv_info_stream_thread ("[RtGvStream::check_frame_completion] Incomplete frame %" G_GUINT64_FORMAT,
							 frame->frame_id);
				_close_frame (thread_data, time_us, frame);
				frame_closed = TRUE;
				continue;
			}

			if (can_close_frame &&
			    frame->last_valid_packet == frame->n_packets - 1) {
				frame->buffer->priv->status = ARV_BUFFER_STATUS_SUCCESS;
				arv_debug_stream_thread ("[RtGvStream::check_frame_completion] Completed frame %" G_GUINT64_FORMAT,
						       frame->frame_id);
				_close_frame (thread_data, time_us, frame);
				frame_closed = TRUE;
				continue;
			}

			if (can_close_frame &&
		 	   time_us - frame->last_packet_time_us >= thread_data->frame_retention_us) {
				frame->buffer->priv->status = ARV_BUFFER_STATUS_TIMEOUT;
				arv_warning_stream_thread ("[RtGvStream::check_frame_completion] Timeout for frame %"
							   G_GUINT64_FORMAT " at dt = %" G_GUINT64_FORMAT,
							   frame->frame_id, time_us - frame->first_packet_time_us);
#if 0
				if (arv_debug_check (&arv_debug_category_stream_thread, ARV_DEBUG_LEVEL_LOG)) {
					int i;
					arv_debug_stream_thread ("frame_id          = %Lu", frame->frame_id);
					arv_debug_stream_thread ("last_valid_packet = %d", frame->last_valid_packet);
					for (i = 0; i < frame->n_packets; i++) {
						arv_debug_stream_thread ("%d - time = %Lu%s", i,
								       frame->packet_data[i].time_us,
								       frame->packet_data[i].received ? " - OK" : "");
					}
				}
#endif
				_close_frame (thread_data, time_us, frame);
				frame_closed = TRUE;
				continue;
			}

			can_close_frame = FALSE;

			if (frame != current_frame &&
			    time_us - frame->last_packet_time_us >= thread_data->packet_timeout_us) {
				_missing_packet_check (thread_data, frame, frame->n_packets - 1, time_us);
				continue;
			}
		}
	}

	return frame_closed;
}

static void
_flush_frames (ArvGvStreamThreadData *thread_data,
               guint64 time_us)
{
	ArvGvStreamFrameData *frame;
	size_t i;

	for (i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
		frame = &thread_data->frames[i];
		if (frame->buffer) {
			frame->buffer->priv->status = ARV_BUFFER_STATUS_ABORTED;
			_close_frame (thread_data, time_us, frame);
		}
	}
}

static ArvGvStreamFrameData *
_process_packet (ArvGvStreamThreadData *thread_data, const ArvGvspPacket *packet, size_t packet_size, guint64 time_us)

{
	ArvGvStreamFrameData *frame;
	guint32 packet_id;
	guint64 frame_id;
	gboolean extended_ids;
	int i;

	thread_data->n_received_packets++;

	extended_ids = arv_gvsp_packet_has_extended_ids (packet);
	frame_id = arv_gvsp_packet_get_frame_id (packet);
	packet_id = arv_gvsp_packet_get_packet_id (packet);

	if (thread_data->first_packet) {
		thread_data->last_frame_id = frame_id - 1;
		thread_data->first_packet = FALSE;
	}

	frame = _find_frame_data (thread_data, packet, packet_size, frame_id, packet_id, extended_ids, packet_size, time_us);

	if (frame != NULL) {
		ArvGvspPacketType packet_type = arv_gvsp_packet_get_packet_type (packet);

		if (arv_gvsp_packet_type_is_error (packet_type)) {
                        ArvGvcpError error = packet_type & 0xff;

			arv_info_stream_thread ("[RtGvStream::process_packet]"
						 " Error packet at dt = %" G_GINT64_FORMAT ", packet id = %u"
						 " frame id = %" G_GUINT64_FORMAT,
						 time_us - frame->first_packet_time_us,
						 packet_id, frame->frame_id);
			arv_gvsp_packet_debug (packet, packet_size, ARV_DEBUG_LEVEL_INFO);

                        if (error == ARV_GVCP_ERROR_PACKET_AND_PREVIOUS_REMOVED_FROM_MEMORY ||
                            error == ARV_GVCP_ERROR_PACKET_REMOVED_FROM_MEMORY ||
                            error == ARV_GVCP_ERROR_PACKET_UNAVAILABLE) {
                                frame->disable_resend_request = TRUE;
                                thread_data->n_resend_disabled++;
                        }

			thread_data->n_error_packets++;
                        thread_data->n_transferred_bytes += packet_size;
		} else if (packet_id < frame->n_packets &&
		           frame->packet_data[packet_id].received) {
			/* Ignore duplicate packet */
			thread_data->n_duplicated_packets++;
			arv_debug_stream_thread ("[RtGvStream::process_packet] Duplicated packet %d for frame %" G_GUINT64_FORMAT,
						 packet_id, frame->frame_id);
			arv_gvsp_packet_debug (packet, packet_size, ARV_DEBUG_LEVEL_DEBUG);

                        thread_data->n_transferred_bytes += packet_size;
		} else {
			ArvGvspContentType content_type;

			if (packet_id < frame->n_packets) {
				frame->packet_data[packet_id].received = TRUE;
			}

			/* Keep track of last packet of a continuous block starting from packet 0 */
			for (i = frame->last_valid_packet + 1; i < frame->n_packets; i++)
				if (!frame->packet_data[i].received)
					break;
			frame->last_valid_packet = i - 1;

			content_type = arv_gvsp_packet_get_content_type (packet);

			arv_gvsp_packet_debug (packet, packet_size,
					       content_type == ARV_GVSP_CONTENT_TYPE_DATA_BLOCK ?
					       ARV_DEBUG_LEVEL_TRACE :
					       ARV_DEBUG_LEVEL_DEBUG);

			switch (content_type) {
				case ARV_GVSP_CONTENT_TYPE_DATA_LEADER:
					_process_data_leader (thread_data, frame, packet, packet_id);
                                        thread_data->n_transferred_bytes += packet_size;
					break;
				case ARV_GVSP_CONTENT_TYPE_DATA_BLOCK:
					_process_data_block (thread_data, frame, packet, packet_id,
							     packet_size);
                                        thread_data->n_transferred_bytes += packet_size;
					break;
				case ARV_GVSP_CONTENT_TYPE_DATA_TRAILER:
					_process_data_trailer (thread_data, frame, packet_id);
                                        thread_data->n_transferred_bytes += packet_size;
					break;
				default:
					thread_data->n_ignored_packets++;
                                        thread_data->n_ignored_bytes += packet_size;
					break;
			}

			_missing_packet_check (thread_data, frame, packet_id, time_us);
		}
	} else {
                thread_data->n_ignored_packets++;
                thread_data->n_ignored_bytes += packet_size;
        }

	return frame;
}

gboolean
arv_rt_gv_stream_recv_frame(ArvStream *stream, int64_t recv_frame_timeout)
{
	ArvGvStreamFrameData *frame;
	guint64 time_us;
	uint64_t start_time;
	gboolean frame_completed = FALSE;
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (stream));
	ArvGvStreamThreadData *thread_data = priv->thread_data;

	if (thread_data->n_output_buffers > 0)
		return TRUE;

	start_time = rt_timer_read();

	do {
		int64_t timeout_ns;
		ssize_t count;
		char *packet_buffers;

		if (thread_data->n_frames > 0)
			timeout_ns = thread_data->packet_timeout_us * 1000;
		else
			timeout_ns = ARV_GV_STREAM_POLL_TIMEOUT_US * 1000;

		if (thread_data->last_timeout_ns != timeout_ns) {
			if (rt_dev_ioctl(thread_data->socket, RTNET_RTIOC_TIMEOUT, &timeout_ns) < 0) {
				arv_warning_stream_thread("Can't set timeout of data socket");
				thread_data->last_timeout_ns = 0;
			} else {
				thread_data->last_timeout_ns = timeout_ns;
			}
		}

		count = rt_dev_recv(thread_data->socket, thread_data->packet_buffer,
			thread_data->packet_buffer_size * ARV_GV_STREAM_NUM_BUFFERS, 0);
		if (count > 0) {
			time_us = g_get_monotonic_time ();
			for (packet_buffers = (char*)thread_data->packet_buffer; count > 0;
						count -= thread_data->packet_buffer_size,
						packet_buffers += thread_data->packet_buffer_size) {
				frame = _process_packet (thread_data, (ArvGvspPacket*)packet_buffers,
							count > thread_data->packet_buffer_size ?
								thread_data->packet_buffer_size : count,
							time_us);
				frame_completed = _check_frame_completion(thread_data, time_us, frame);
			}
		} else {
			if (count < 0)
				arv_warning_stream_thread("rt_dev_recv() returned error %ld", count);
			time_us = g_get_monotonic_time ();
			frame_completed = _check_frame_completion (thread_data, time_us, NULL);
                }
	} while (!frame_completed && rt_timer_read() - start_time < recv_frame_timeout);

	return frame_completed;
}


#if 0
#if ARAVIS_HAS_PACKET_SOCKET

static void
_set_socket_filter (int socket, guint32 source_ip, guint32 source_port, guint32 destination_ip, guint32 destination_port)
{
	struct sock_filter bpf[18] = {
		{ 0x28, 0, 0, 0x0000000c },
		{ 0x15, 15, 0, 0x000086dd },
		{ 0x15, 0, 14, 0x00000800 },
		{ 0x30, 0, 0, 0x00000017 },
		{ 0x15, 0, 12, 0x00000011 },
		{ 0x20, 0, 0, 0x0000001a },
		{ 0x15, 0, 10, source_ip },
		{ 0x28, 0, 0, 0x00000014 },
		{ 0x45, 8, 0, 0x00001fff },
		{ 0xb1, 0, 0, 0x0000000e },
		{ 0x48, 0, 0, 0x0000000e },
		{ 0x15, 0, 5, source_port },
		{ 0x20, 0, 0, 0x0000001e },
		{ 0x15, 0, 3, destination_ip },
		{ 0x48, 0, 0, 0x00000010 },
		{ 0x15, 0, 1, destination_port },
		{ 0x6, 0, 0, 0x00040000 },
		{ 0x6, 0, 0, 0x00000000 }
	};
	struct sock_fprog bpf_prog = {sizeof(bpf) / sizeof(struct sock_filter), bpf};

	arv_info_stream_thread ("[GvStream::set_socket_filter] source ip = 0x%08x - port = %d - dest ip = 0x%08x - port %d",
				 source_ip, source_port, destination_ip, destination_port);

	if (setsockopt(socket, SOL_SOCKET, SO_ATTACH_FILTER, &bpf_prog, sizeof(bpf_prog)) != 0)
		arv_warning_stream_thread ("[GvStream::set_socket_filter] Failed to attach Beckerley Packet Filter to stream socket");
}

static unsigned
_interface_index_from_address (guint32 ip)
{
    struct ifaddrs *ifaddr = NULL;
    struct ifaddrs *ifa;
    unsigned index = 0;

    if (getifaddrs(&ifaddr) == -1) {
        return index;
    }

    for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
	    if (ifa->ifa_addr != NULL &&
		ifa->ifa_addr->sa_family == AF_INET) {
		    struct sockaddr_in *sa;

		    sa = (struct sockaddr_in *) (ifa->ifa_addr);
		    if (ip == g_ntohl (sa->sin_addr.s_addr)) {
			    index = if_nametoindex (ifa->ifa_name);
			    break;
		    }
	    }
    }

    freeifaddrs (ifaddr);

    return index;
}

typedef struct {
	guint32 version;
	guint32 offset_to_priv;
	struct tpacket_hdr_v1 h1;
} ArvGvStreamBlockDescriptor;

static void
_ring_buffer_loop (ArvGvStreamThreadData *thread_data)
{
	GPollFD poll_fd[2];
	char *buffer;
	struct tpacket_req3 req;
	struct sockaddr_ll local_address = {0};
	enum tpacket_versions version;
	int fd;
	unsigned block_id;
	const guint8 *bytes;
	guint32 interface_address;
	guint32 device_address;
	gboolean use_poll;

	arv_info_stream ("[GvStream::loop] Packet socket method");

	fd = socket (PF_PACKET, SOCK_RAW, g_htons (ETH_P_ALL));
	if (fd < 0) {
		arv_warning_stream_thread ("[GvStream::loop] Failed to create AF_PACKET socket");
		goto af_packet_error;
	}

	version = TPACKET_V3;
	if (setsockopt (fd, SOL_PACKET, PACKET_VERSION, &version, sizeof(version)) < 0) {
		arv_warning_stream_thread ("[GvStream::loop] Failed to set packet version");
		goto socket_option_error;
	}

	req.tp_block_size = 1 << 21;
	req.tp_frame_size = 1024;
	req.tp_block_nr = 16;
	req.tp_frame_nr = (req.tp_block_size * req.tp_block_nr) / req.tp_frame_size;
	req.tp_sizeof_priv = 0;
	req.tp_retire_blk_tov = 5;
	req.tp_feature_req_word = TP_FT_REQ_FILL_RXHASH;
	if (setsockopt (fd, SOL_PACKET, PACKET_RX_RING, &req, sizeof(req)) < 0) {
		arv_warning_stream_thread ("[GvStream::loop] Failed to set packet rx ring");
		goto socket_option_error;
	}

	buffer = mmap (NULL, req.tp_block_size * req.tp_block_nr, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd, 0);
	if (buffer == MAP_FAILED) {
		arv_warning_stream_thread ("[GvStream::loop] Failed to map ring buffer");
		goto map_error;
	}

	bytes = g_inet_address_to_bytes (thread_data->interface_address);
	interface_address = g_ntohl (*((guint32 *) bytes));
	bytes = g_inet_address_to_bytes (thread_data->device_address);
	device_address = g_ntohl (*((guint32 *) bytes));

	local_address.sll_family   = AF_PACKET;
	local_address.sll_protocol = g_htons(ETH_P_IP);
	local_address.sll_ifindex  = _interface_index_from_address (interface_address);
	local_address.sll_hatype   = 0;
	local_address.sll_pkttype  = 0;
	local_address.sll_halen    = 0;
	if (bind (fd, (struct sockaddr *) &local_address, sizeof(local_address)) == -1) {
		arv_warning_stream_thread ("[GvStream::loop] Failed to bind packet socket");
		goto bind_error;
	}

	_set_socket_filter (fd, device_address, thread_data->source_stream_port, interface_address, thread_data->stream_port);

	poll_fd[0].fd = fd;
	poll_fd[0].events =  G_IO_IN;
	poll_fd[0].revents = 0;

	use_poll = g_cancellable_make_pollfd (thread_data->cancellable, &poll_fd[1]);

        g_mutex_lock (&thread_data->thread_started_mutex);
        thread_data->thread_started = TRUE;
        g_cond_signal (&thread_data->thread_started_cond);
        g_mutex_unlock (&thread_data->thread_started_mutex);

	block_id = 0;
	do {
		ArvGvStreamBlockDescriptor *descriptor;
		guint64 time_us;

		time_us = g_get_monotonic_time ();

		descriptor = (void *) (buffer + block_id * req.tp_block_size);
		if ((descriptor->h1.block_status & TP_STATUS_USER) == 0) {
                        int timeout_ms;
			int n_events;
			int errsv;

			_check_frame_completion (thread_data, time_us, NULL);

                        if (thread_data->frames != NULL)
                                timeout_ms = thread_data->packet_timeout_us / 1000;
                        else
                                timeout_ms = ARV_GV_STREAM_POLL_TIMEOUT_US / 1000;

			do {
				n_events = g_poll (poll_fd, use_poll ? 2 : 1,  timeout_ms);
				errsv = errno;
			} while (n_events < 0 && errsv == EINTR);
		} else {
			ArvGvStreamFrameData *frame;
			const struct tpacket3_hdr *header;
			unsigned i;

			header = (void *) (((char *) descriptor) + descriptor->h1.offset_to_first_pkt);

			for (i = 0; i < descriptor->h1.num_pkts; i++) {
				const struct iphdr *ip;
				const ArvGvspPacket *packet;
				size_t size;

				ip = (void *) (((char *) header) + header->tp_mac + ETH_HLEN);
				packet = (void *) (((char *) ip) + sizeof (struct iphdr) + sizeof (struct udphdr));
				size = g_ntohs (ip->tot_len) -  sizeof (struct iphdr) - sizeof (struct udphdr);

				frame = _process_packet (thread_data, packet, size, time_us);

				_check_frame_completion (thread_data, time_us, frame);

				header = (void *) (((char *) header) + header->tp_next_offset);
			}

			descriptor->h1.block_status = TP_STATUS_KERNEL;
			block_id = (block_id + 1) % req.tp_block_nr;
		}
	} while (!g_cancellable_is_cancelled (thread_data->cancellable));

	if (use_poll)
		g_cancellable_release_fd (thread_data->cancellable);

bind_error:
	munmap (buffer, req.tp_block_size * req.tp_block_nr);
socket_option_error:
map_error:
	close (fd);
af_packet_error:
        g_mutex_lock (&thread_data->thread_started_mutex);
        thread_data->thread_started = TRUE;
        g_cond_signal (&thread_data->thread_started_cond);
        g_mutex_unlock (&thread_data->thread_started_mutex);
}

#endif /* ARAVIS_HAS_PACKET_SOCKET */

static void *
arv_gv_stream_thread (void *data)
{
	ArvGvStreamThreadData *thread_data = data;
#if ARAVIS_HAS_PACKET_SOCKET
	int fd;
#endif

	thread_data->frames = NULL;
	thread_data->last_frame_id = 0;
	thread_data->first_packet = TRUE;

	if (thread_data->callback != NULL)
		thread_data->callback (thread_data->callback_data, ARV_STREAM_CALLBACK_TYPE_INIT, NULL);

#if ARAVIS_HAS_PACKET_SOCKET
	if (thread_data->use_packet_socket && (fd = socket (PF_PACKET, SOCK_RAW, g_htons (ETH_P_ALL))) >= 0) {
		close (fd);
		_ring_buffer_loop (thread_data);
	} else
#endif
		_loop (thread_data);

	_flush_frames (thread_data, g_get_monotonic_time ());

	if (thread_data->callback != NULL)
		thread_data->callback (thread_data->callback_data, ARV_STREAM_CALLBACK_TYPE_EXIT, NULL);

	return NULL;
}

/* ArvGvStream implementation */

guint16
arv_rt_gv_stream_get_port (ArvRtGvStream *gv_stream)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (gv_stream);

	g_return_val_if_fail (ARV_IS_RT_GV_STREAM (gv_stream), 0);

	return priv->thread_data->stream_port;
}

static void
arv_gv_stream_start_thread (ArvStream *stream)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (stream));
	ArvGvStreamThreadData *thread_data;

	g_return_if_fail (priv->thread == NULL);
	g_return_if_fail (priv->thread_data != NULL);

	thread_data = priv->thread_data;

        thread_data->thread_started = FALSE;
	thread_data->cancellable = g_cancellable_new ();
	priv->thread = g_thread_new ("arv_gv_stream", arv_gv_stream_thread, priv->thread_data);

        g_mutex_lock (&thread_data->thread_started_mutex);
        while (!thread_data->thread_started)
                g_cond_wait (&thread_data->thread_started_cond,
                             &thread_data->thread_started_mutex);
        g_mutex_unlock (&thread_data->thread_started_mutex);
}

static void
arv_gv_stream_stop_thread (ArvStream *stream)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (stream));
	ArvGvStreamThreadData *thread_data;

	g_return_if_fail (priv->thread != NULL);
	g_return_if_fail (priv->thread_data != NULL);

	thread_data = priv->thread_data;

	g_cancellable_cancel (thread_data->cancellable);
	g_thread_join (priv->thread);
	g_clear_object (&thread_data->cancellable);

	priv->thread = NULL;
}
#endif

/**
 * arv_rt_gv_stream_new: (skip)
 * @gv_device: a #ArvRtGvDevice
 * @callback: (scope call): processing callback
 * @callback_data: (closure): user data for @callback
 *
 * Return value: (transfer full): a new #ArvStream.
 */

ArvStream *
arv_rt_gv_stream_new (ArvRtGvDevice *gv_device, ArvStreamCallback callback, void *callback_data, GError **error)
{
	return g_initable_new (ARV_TYPE_RT_GV_STREAM, NULL, error,
			       "device", gv_device,
			       "callback", callback,
			       "callback-data", callback_data,
			       NULL);
}

/* ArvStream implementation */

/**
 * arv_rt_gv_stream_get_statistics:
 * @gv_stream: a #ArvRtGvStream
 * @n_resent_packets: (out)
 * @n_missing_packets: (out)
 */

void
arv_rt_gv_stream_get_statistics (ArvRtGvStream *gv_stream,
				 guint64 *n_resent_packets,
				 guint64 *n_missing_packets)

{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (gv_stream);
	ArvGvStreamThreadData *thread_data;

	g_return_if_fail (ARV_IS_GV_STREAM (gv_stream));

	thread_data = priv->thread_data;

	if (n_resent_packets != NULL)
		*n_resent_packets = thread_data->n_resent_packets;
	if (n_missing_packets != NULL)
		*n_missing_packets = thread_data->n_missing_packets;
}

static void
arv_rt_gv_stream_set_property (GObject * object, guint prop_id,
                               const GValue * value, GParamSpec * pspec)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (object));
	ArvGvStreamThreadData *thread_data;

	thread_data = priv->thread_data;

	switch (prop_id) {
		case ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER:
			thread_data->socket_buffer_option = g_value_get_enum (value);
			break;
		case ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER_SIZE:
			thread_data->socket_buffer_size = g_value_get_int (value);
			break;
		case ARV_GV_STREAM_PROPERTY_PACKET_RESEND:
			thread_data->packet_resend = g_value_get_enum (value);
			break;
		case ARV_GV_STREAM_PROPERTY_PACKET_REQUEST_RATIO:
			thread_data->packet_request_ratio = g_value_get_double (value);
			break;
		case ARV_GV_STREAM_PROPERTY_INITIAL_PACKET_TIMEOUT:
			thread_data->initial_packet_timeout_us = g_value_get_uint (value);
			break;
		case ARV_GV_STREAM_PROPERTY_PACKET_TIMEOUT:
			thread_data->packet_timeout_us = g_value_get_uint (value);
			break;
		case ARV_GV_STREAM_PROPERTY_FRAME_RETENTION:
			thread_data->frame_retention_us = g_value_get_uint (value);
			break;
		default:
			G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
			break;
	}
}

static void
arv_rt_gv_stream_get_property (GObject * object, guint prop_id,
			       GValue * value, GParamSpec * pspec)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (object));
	ArvGvStreamThreadData *thread_data;

	thread_data = priv->thread_data;

	switch (prop_id) {
		case ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER:
			g_value_set_enum (value, thread_data->socket_buffer_option);
			break;
		case ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER_SIZE:
			g_value_set_int (value, thread_data->socket_buffer_size);
			break;
		case ARV_GV_STREAM_PROPERTY_PACKET_RESEND:
			g_value_set_enum (value, thread_data->packet_resend);
			break;
		case ARV_GV_STREAM_PROPERTY_PACKET_REQUEST_RATIO:
			g_value_set_double (value, thread_data->packet_request_ratio);
			break;
		case ARV_GV_STREAM_PROPERTY_INITIAL_PACKET_TIMEOUT:
			g_value_set_uint (value, thread_data->initial_packet_timeout_us);
			break;
		case ARV_GV_STREAM_PROPERTY_PACKET_TIMEOUT:
			g_value_set_uint (value, thread_data->packet_timeout_us);
			break;
		case ARV_GV_STREAM_PROPERTY_FRAME_RETENTION:
			g_value_set_uint (value, thread_data->frame_retention_us);
			break;
		default:
			G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
			break;
	}
}

static void
arv_rt_gv_stream_init (ArvRtGvStream *gv_stream)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (gv_stream);

	priv->thread_data = g_new0 (ArvGvStreamThreadData, 1);
}

static void
arv_rt_gv_stream_constructed (GObject *object)
{
	ArvStream *stream = ARV_STREAM (object);
	ArvRtGvStream *gv_stream = ARV_RT_GV_STREAM (object);
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (stream));
	ArvGvStreamOption options;
	ArvRtGvDevice *gv_device = NULL;
	GInetAddress *interface_address;
	GInetAddress *device_address;
	guint64 timestamp_tick_frequency;
	const guint8 *address_bytes;
	guint packet_size;
	GError *local_error = NULL;
	int add_rtskbs = 1000;
	struct sockaddr_in sock_addr_in;
	socklen_t socklen = sizeof(sock_addr_in);

	G_OBJECT_CLASS (arv_rt_gv_stream_parent_class)->constructed (object);

	g_object_get (object, "device", &gv_device, NULL);

	timestamp_tick_frequency = arv_rt_gv_device_get_timestamp_tick_frequency (gv_device, NULL);
	options = arv_rt_gv_device_get_stream_options (gv_device);

	packet_size = arv_rt_gv_device_get_packet_size (gv_device, NULL);
	if (packet_size <= ARV_GVSP_PACKET_PROTOCOL_OVERHEAD) {
		arv_rt_gv_device_set_packet_size (gv_device, ARV_GV_DEVICE_GVSP_PACKET_SIZE_DEFAULT, NULL);
		arv_info_device ("[RtGvStream::stream_new] Packet size set to default value (%d)",
				  ARV_GV_DEVICE_GVSP_PACKET_SIZE_DEFAULT);
	}

	packet_size = arv_rt_gv_device_get_packet_size (gv_device, NULL);
	arv_info_device ("[RtGvStream::stream_new] Packet size = %d byte(s)", packet_size);

	if (packet_size <= ARV_GVSP_PACKET_PROTOCOL_OVERHEAD) {
		arv_stream_take_init_error (stream, g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_PROTOCOL_ERROR,
								 "Invalid packet size (%d byte(s))", packet_size));
		g_clear_object (&gv_device);
		return;
	}

	priv->thread_data->stream = stream;

	g_object_get (object,
		      "callback", &priv->thread_data->callback,
		      "callback-data", &priv->thread_data->callback_data,
		      NULL);

	priv->thread_data->timestamp_tick_frequency = timestamp_tick_frequency;
	priv->thread_data->scps_packet_size = packet_size;
	priv->thread_data->use_packet_socket = (options & ARV_GV_STREAM_OPTION_PACKET_SOCKET_DISABLED) == 0;

	priv->thread_data->packet_id = 65300;

	priv->thread_data->last_timeout_ns = 0;

	priv->thread_data->histogram = arv_histogram_new (3, 100, 2000, 0);

	arv_histogram_set_variable_name (priv->thread_data->histogram, 0, "frame_retention");
	arv_histogram_set_variable_name (priv->thread_data->histogram, 1, "packet_time");
	arv_histogram_set_variable_name (priv->thread_data->histogram, 2, "inter_packet");

	interface_address = g_inet_socket_address_get_address
                (G_INET_SOCKET_ADDRESS (arv_rt_gv_device_get_interface_address (gv_device)));
	device_address = g_inet_socket_address_get_address
                (G_INET_SOCKET_ADDRESS (arv_rt_gv_device_get_device_address (gv_device)));

	priv->thread_data->socket = rt_dev_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (priv->thread_data->socket < 0) {
		arv_stream_take_init_error(stream, g_error_new(ARV_DEVICE_ERROR,
								ARV_DEVICE_ERROR_UNKNOWN,
								"rt_dev_socket() failed"));
		g_clear_object (&gv_device);
		return;
	}

	if (rt_dev_ioctl(priv->thread_data->socket, RTNET_RTIOC_EXTPOOL, &add_rtskbs) != add_rtskbs)
		arv_warning_stream("ioctl(RTNET_RTIOC_EXTPOOL)");

	priv->thread_data->device_address = g_object_ref (device_address);
	priv->thread_data->interface_address = g_object_ref (interface_address);
	priv->thread_data->interface_socket_address = g_inet_socket_address_new (interface_address, 0);
	priv->thread_data->device_socket_address = g_inet_socket_address_new (device_address, ARV_GVCP_PORT);

	if (!g_socket_address_to_native(priv->thread_data->interface_socket_address,
					&sock_addr_in, sizeof(sock_addr_in),
					&local_error)) {
		if (local_error == NULL)
			local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
						   "Unknown error during g_socket_address_to_native(interface_address)");
		arv_stream_take_init_error(stream, local_error);
		g_clear_object (&gv_device);
		return;
	}
	if (rt_dev_bind(priv->thread_data->socket, (struct sockaddr*)&sock_addr_in, sizeof(sock_addr_in))) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
					   "rt_dev_bind() failed");
		arv_stream_take_init_error(stream, local_error);
		g_clear_object (&gv_device);
		return;
	}

	if (rt_dev_getsockname(priv->thread_data->socket, (struct sockaddr*)&sock_addr_in, &socklen)) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
					   "rt_dev_getsockname() failed");
		arv_stream_take_init_error(stream, local_error);
		g_clear_object (&gv_device);
		return;
	}
	priv->thread_data->stream_port = g_ntohs(sock_addr_in.sin_port);

	if (!g_socket_address_to_native(priv->thread_data->device_socket_address,
					&priv->thread_data->sockaddr_device,
					sizeof(priv->thread_data->sockaddr_device),
					&local_error)) {
		if (local_error == NULL)
			local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
						   "Unknown error during g_socket_address_to_native(device_address)");
		arv_stream_take_init_error(stream, local_error);
		g_clear_object (&gv_device);
		return;
	}

	address_bytes = g_inet_address_to_bytes (interface_address);
	arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevSCDA", g_htonl (*((guint32 *) address_bytes)), NULL);
	arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevSCPHostPort", priv->thread_data->stream_port, NULL);
	priv->thread_data->source_stream_port = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device), "GevSCSP", NULL);

	arv_info_stream ("[RtGvStream::stream_new] Destination stream port = %d", priv->thread_data->stream_port);
	arv_info_stream ("[RtGvStream::stream_new] Source stream port = %d", priv->thread_data->source_stream_port);

        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_completed_buffers",
                                 G_TYPE_UINT64, &priv->thread_data->n_completed_buffers);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_failures",
                                 G_TYPE_UINT64, &priv->thread_data->n_failures);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_underruns",
                                 G_TYPE_UINT64, &priv->thread_data->n_underruns);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_timeouts",
                                 G_TYPE_UINT64, &priv->thread_data->n_timeouts);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_aborteds",
                                 G_TYPE_UINT64, &priv->thread_data->n_aborteds);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_missing_frames",
                                 G_TYPE_UINT64, &priv->thread_data->n_missing_frames);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_size_mismatch_errors",
                                 G_TYPE_UINT64, &priv->thread_data->n_size_mismatch_errors);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_received_packets",
                                 G_TYPE_UINT64, &priv->thread_data->n_received_packets);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_missing_packets",
                                 G_TYPE_UINT64, &priv->thread_data->n_missing_packets);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_error_packets",
                                 G_TYPE_UINT64, &priv->thread_data->n_error_packets);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_ignored_packets",
                                 G_TYPE_UINT64, &priv->thread_data->n_ignored_packets);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_resend_requests",
                                 G_TYPE_UINT64, &priv->thread_data->n_resend_requests);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_resent_packets",
                                 G_TYPE_UINT64, &priv->thread_data->n_resent_packets);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_resend_ratio_reached",
                                 G_TYPE_UINT64, &priv->thread_data->n_resend_ratio_reached);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_resend_disabled",
                                 G_TYPE_UINT64, &priv->thread_data->n_resend_disabled);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_duplicated_packets",
                                 G_TYPE_UINT64, &priv->thread_data->n_duplicated_packets);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_transferred_bytes",
                                 G_TYPE_UINT64, &priv->thread_data->n_transferred_bytes);
        arv_stream_declare_info (ARV_STREAM (gv_stream), "n_ignored_bytes",
                                 G_TYPE_UINT64, &priv->thread_data->n_ignored_bytes);

//	arv_gv_stream_start_thread (ARV_STREAM (gv_stream));
	priv->thread_data->last_frame_id = 0;
	priv->thread_data->first_packet = TRUE;

	// we don't need to consider the IP and UDP header size
	priv->thread_data->packet_buffer_size = priv->thread_data->scps_packet_size - 20 - 8;
	priv->thread_data->packet_buffer = g_malloc0 (priv->thread_data->packet_buffer_size *
									ARV_GV_STREAM_NUM_BUFFERS);

	g_clear_object (&gv_device);
}

static void
arv_rt_gv_stream_finalize (GObject *object)
{
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private (ARV_RT_GV_STREAM (object));

//	arv_gv_stream_stop_thread (ARV_STREAM (object));

	if (priv->thread_data != NULL) {
		ArvGvStreamThreadData *thread_data;
		char *histogram_string;

		thread_data = priv->thread_data;

		_flush_frames (thread_data, g_get_monotonic_time ());

		histogram_string = arv_histogram_to_string (thread_data->histogram);
		arv_info_stream ("%s", histogram_string);
		g_free (histogram_string);
		arv_histogram_unref (thread_data->histogram);

		arv_info_stream ("[RtGvStream::finalize] n_completed_buffers    = %" G_GUINT64_FORMAT,
				  thread_data->n_completed_buffers);
		arv_info_stream ("[RtGvStream::finalize] n_failures             = %" G_GUINT64_FORMAT,
				  thread_data->n_failures);
		arv_info_stream ("[RtGvStream::finalize] n_underruns            = %" G_GUINT64_FORMAT,
				  thread_data->n_underruns);
		arv_info_stream ("[RtGvStream::finalize] n_timeouts             = %" G_GUINT64_FORMAT,
				  thread_data->n_timeouts);
		arv_info_stream ("[RtGvStream::finalize] n_aborteds             = %" G_GUINT64_FORMAT,
				  thread_data->n_aborteds);
		arv_info_stream ("[RtGvStream::finalize] n_missing_frames       = %" G_GUINT64_FORMAT,
				  thread_data->n_missing_frames);

		arv_info_stream ("[RtGvStream::finalize] n_size_mismatch_errors = %" G_GUINT64_FORMAT,
				  thread_data->n_size_mismatch_errors);

		arv_info_stream ("[RtGvStream::finalize] n_received_packets     = %" G_GUINT64_FORMAT,
				  thread_data->n_received_packets);
		arv_info_stream ("[RtGvStream::finalize] n_missing_packets      = %" G_GUINT64_FORMAT,
				  thread_data->n_missing_packets);
		arv_info_stream ("[RtGvStream::finalize] n_error_packets        = %" G_GUINT64_FORMAT,
				  thread_data->n_error_packets);
		arv_info_stream ("[RtGvStream::finalize] n_ignored_packets      = %" G_GUINT64_FORMAT,
				  thread_data->n_ignored_packets);

		arv_info_stream ("[RtGvStream::finalize] n_resend_requests      = %" G_GUINT64_FORMAT,
				  thread_data->n_resend_requests);
		arv_info_stream ("[RtGvStream::finalize] n_resent_packets       = %" G_GUINT64_FORMAT,
				  thread_data->n_resent_packets);
		arv_info_stream ("[RtGvStream::finalize] n_resend_ratio_reached = %" G_GUINT64_FORMAT,
				  thread_data->n_resend_ratio_reached);
		arv_info_stream ("[RtGvStream::finalize] n_resend_disabled      = %" G_GUINT64_FORMAT,
				  thread_data->n_resend_disabled);
		arv_info_stream ("[RtGvStream::finalize] n_duplicated_packets   = %" G_GUINT64_FORMAT,
				  thread_data->n_duplicated_packets);

		arv_info_stream ("[RtGvStream::finalize] n_transferred_bytes    = %" G_GUINT64_FORMAT,
				  thread_data->n_transferred_bytes);
		arv_info_stream ("[RtGvStream::finalize] n_ignored_bytes        = %" G_GUINT64_FORMAT,
				  thread_data->n_ignored_bytes);

		g_clear_object (&thread_data->device_address);
		g_clear_object (&thread_data->interface_address);
		g_clear_object (&thread_data->device_socket_address);
		g_clear_object (&thread_data->interface_socket_address);
		if (thread_data->socket >= 0)
			rt_dev_close(thread_data->socket);

		if (thread_data->max_n_packets) {
			for (int i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++)
				g_free(thread_data->packet_data[i]);
		}

		g_free (thread_data->packet_buffer);

		g_clear_pointer (&thread_data, g_free);
	}

	G_OBJECT_CLASS (arv_rt_gv_stream_parent_class)->finalize (object);
}

static void
arv_rt_gv_stream_class_init (ArvRtGvStreamClass *gv_stream_class)
{
	GObjectClass *object_class = G_OBJECT_CLASS (gv_stream_class);
//	ArvStreamClass *stream_class = ARV_STREAM_CLASS (gv_stream_class);

	object_class->constructed = arv_rt_gv_stream_constructed;
	object_class->finalize = arv_rt_gv_stream_finalize;
	object_class->set_property = arv_rt_gv_stream_set_property;
	object_class->get_property = arv_rt_gv_stream_get_property;

#if 0
	stream_class->start_thread = arv_gv_stream_start_thread;
	stream_class->stop_thread = arv_gv_stream_stop_thread;
#endif

        /**
         * ArvRtGvStream:socket-buffer:
         *
         * Incoming socket buffer policy.
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER,
		g_param_spec_enum ("socket-buffer", "Socket buffer",
				   "Socket buffer behaviour",
				   ARV_TYPE_GV_STREAM_SOCKET_BUFFER,
				   ARV_GV_STREAM_SOCKET_BUFFER_FIXED,
				  G_PARAM_CONSTRUCT |  G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
        /**
         * ArvRtGvStream:socket-buffer-size:
         *
         * Size in bytes of the incoming socket buffer. A greater value helps to lower the number of missings packets,
         * as the expense of an increased memory usage.
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_SOCKET_BUFFER_SIZE,
		g_param_spec_int ("socket-buffer-size", "Socket buffer size",
				  "Socket buffer size, in bytes",
				  -1, G_MAXINT, 0,
				  G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
        /**
         * ArvRtGvStream:packet-resend:
         *
         * Packet resend policy. This only applies if the device supports packet resend.
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_PACKET_RESEND,
		g_param_spec_enum ("packet-resend", "Packet resend",
				   "Packet resend behaviour",
				   ARV_TYPE_GV_STREAM_PACKET_RESEND,
				   ARV_GV_STREAM_PACKET_RESEND_ALWAYS,
				   G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
        /**
         * ArvRtGvStream:packet-request-ratio:
         *
         * Maximum number of packet resend requests for a given frame, as a percentage of the number of packets per
         * frame.
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_PACKET_REQUEST_RATIO,
		g_param_spec_double ("packet-request-ratio", "Packet request ratio",
				     "Packet resend request limit as a percentage of frame packet number",
				     0.0, 2.0, ARV_GV_STREAM_PACKET_REQUEST_RATIO_DEFAULT,
				     G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
        /**
         * ArvRtGvStream:initial-packet-timeout:
         *
         * Delay before asking for a packet resend after the packet was detected missing for the first time. The reason
         * for this delay is, depending on the network topology, stream packets are not always received in increasing id
         * order. As the missing packet detection happens at each received packet, by verifying if each previous packet
         * has been received, we could emit useless packet resend requests if they are not ordered.
         *
         * Since: 0.8.15
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_INITIAL_PACKET_TIMEOUT,
		g_param_spec_uint ("initial-packet-timeout", "Initial packet timeout",
				   "Initial packet timeout, in µs",
				   0,
				   G_MAXUINT,
				   ARV_GV_STREAM_INITIAL_PACKET_TIMEOUT_US_DEFAULT,
				   G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
        /**
         * ArvRtGvStream:packet-timeout:
         *
         * Timeout while waiting for a packet after a resend request, before asking again.
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_PACKET_TIMEOUT,
		g_param_spec_uint ("packet-timeout", "Packet timeout",
				   "Packet timeout, in µs",
				   0,
				   G_MAXUINT,
				   ARV_GV_STREAM_PACKET_TIMEOUT_US_DEFAULT,
				   G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
        /**
         * ArvRtGvStream:frame-retention:
         *
         * Amount of time Aravis is wating for frame completion after the last packet is received. A greater value will
         * also increase the maximum frame latency in case of missing packets.
         */
	g_object_class_install_property (
		object_class, ARV_GV_STREAM_PROPERTY_FRAME_RETENTION,
		g_param_spec_uint ("frame-retention", "Frame retention",
				   "Packet retention, in µs",
				   0,
				   G_MAXUINT,
				   ARV_GV_STREAM_FRAME_RETENTION_US_DEFAULT,
				   G_PARAM_CONSTRUCT | G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS)
		);
}

int
arv_rt_gv_stream_push_buffer(ArvStream *stream, ArvBuffer *buffer)
{
	ArvRtGvStream *gv_stream = ARV_RT_GV_STREAM(stream);
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private(gv_stream);
	ArvGvStreamThreadData *thread_data;

	g_return_val_if_fail (ARV_IS_RT_GV_STREAM (gv_stream), -EINVAL);
	g_return_val_if_fail (ARV_IS_BUFFER (buffer), -EINVAL);

	thread_data = priv->thread_data;

	if (thread_data->n_input_buffers + thread_data->n_output_buffers == ARV_RT_GV_MAX_N_FRAMES)
		return -ENOSPC;

	for (int i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
		if (thread_data->input_buffers[i] == NULL) {
			thread_data->input_buffers[i] = buffer;
			thread_data->n_input_buffers++;
			break;
		}
	}

	return 0;
}

ArvBuffer *
arv_rt_gv_stream_try_pop_buffer(ArvStream *stream)
{
	ArvRtGvStream *gv_stream = ARV_RT_GV_STREAM(stream);
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private(gv_stream);
	ArvBuffer *buffer = NULL;
	ArvGvStreamThreadData *thread_data;

	g_return_val_if_fail (ARV_IS_RT_GV_STREAM (gv_stream), NULL);

	thread_data = priv->thread_data;

	g_return_val_if_fail(thread_data->n_output_buffers > 0, NULL);

	for (int i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++) {
		if (thread_data->output_buffers[i] != NULL) {
			buffer = thread_data->output_buffers[i];
			thread_data->output_buffers[i] = NULL;
			thread_data->n_output_buffers--;
			break;
		}
	}

	return buffer;
}

void
arv_rt_gv_stream_get_n_buffers(ArvStream *stream, gint *n_input_buffers, gint *n_output_buffers)
{
	ArvRtGvStream *gv_stream = ARV_RT_GV_STREAM(stream);
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private(gv_stream);
	ArvGvStreamThreadData *thread_data;

	if (!ARV_IS_RT_GV_STREAM (gv_stream)) {
		if (n_input_buffers != NULL)
			*n_input_buffers = 0;
		if (n_output_buffers != NULL)
			*n_output_buffers = 0;
		return;
	}

	thread_data = priv->thread_data;

	if (n_input_buffers != NULL)
		*n_input_buffers = thread_data->n_input_buffers;
	if (n_output_buffers != NULL)
		*n_output_buffers = thread_data->n_output_buffers;

}

int
arv_rt_gv_stream_prealloc_packet_data(ArvStream *stream, ArvBuffer *buffer)
{
	ArvRtGvStream *gv_stream = ARV_RT_GV_STREAM(stream);
	ArvRtGvStreamPrivate *priv = arv_rt_gv_stream_get_instance_private(gv_stream);
	ArvGvStreamThreadData *thread_data;
	guint32 block_size;

	g_return_val_if_fail (ARV_IS_RT_GV_STREAM (gv_stream), -EINVAL);
	g_return_val_if_fail (ARV_IS_BUFFER (buffer), -EINVAL);

	thread_data = priv->thread_data;

	if (thread_data->packet_data[0])
		return -EEXIST;

	block_size = thread_data->scps_packet_size - ARV_GVSP_PACKET_EXTENDED_PROTOCOL_OVERHEAD;
	thread_data->max_n_packets = (buffer->priv->size + block_size - 1) / block_size + 2;

	for (int i = 0; i < ARV_RT_GV_MAX_N_FRAMES; i++)
		thread_data->packet_data[i] = g_new0(ArvGvStreamPacketData, thread_data->max_n_packets);

	return 0;
}
