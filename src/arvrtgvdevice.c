/* Aravis - Digital camera library
 *
 * Copyright © 2009-2019 Emmanuel Pacaud
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
 * SECTION: arvrtgvdevice
 * @short_description: Real-Time GigEVision device
 */

#include <arvgvdeviceprivate.h>
#include <arvdeviceprivate.h>
#include <arvgc.h>
#include <arvgccommand.h>
#include <arvgcboolean.h>
#include <arvgcregisterdescriptionnode.h>
#include <arvdebugprivate.h>
#include <arvrtgvstreamprivate.h>
#include <arvrtgvdevice.h>
#include <arvgvcpprivate.h>
#include <arvgvspprivate.h>
#include <arvnetworkprivate.h>
#include <arvzip.h>
#include <arvstr.h>
#include <arvmiscprivate.h>
#include <arvenumtypes.h>
#include <native/timer.h>
#include <rtdm/net.h>
#include <trank/rtdm/rtdm.h>
#include <string.h>
#include <stdlib.h>

/* Shared data (main thread - heartbeat) */

enum
{
	PROP_0,
	PROP_GV_DEVICE_INTERFACE_ADDRESS,
	PROP_GV_DEVICE_DEVICE_ADDRESS,
	PROP_GV_DEVICE_PACKET_SIZE_ADJUSTEMENT
};

typedef struct {
	GMutex mutex;

	guint16 packet_id;

	int                socket;
	GSocketAddress	   *interface_address;
	GSocketAddress     *device_address;
	struct sockaddr_in sockaddr_device;

	void *buffer;

	unsigned int gvcp_n_retries;
	int64_t      gvcp_timeout_ns;

	gboolean is_controller;
} ArvRtGvDeviceIOData;

typedef struct {
	GInetAddress *interface_address;
	GInetAddress *device_address;

	ArvRtGvDeviceIOData *io_data;
#if 0
	void *heartbeat_thread;
	void *heartbeat_data;
#endif
	ArvGc *genicam;

	char *genicam_xml;
	size_t genicam_xml_size;

	gboolean is_big_endian_device;

	gboolean is_packet_resend_supported;
	gboolean is_write_memory_supported;

	ArvGvStreamOption stream_options;
	ArvGvPacketSizeAdjustment packet_size_adjustment;

	gboolean first_stream_created;

	gboolean init_success;
} ArvRtGvDevicePrivate ;

struct _ArvRtGvDevice {
	ArvDevice device;
};

struct _ArvRtGvDeviceClass {
	ArvDeviceClass parent_class;
};

G_DEFINE_TYPE_WITH_CODE (ArvRtGvDevice, arv_rt_gv_device, ARV_TYPE_DEVICE, G_ADD_PRIVATE (ArvRtGvDevice))

static gboolean
_send_cmd_and_receive_ack (ArvRtGvDeviceIOData *io_data, ArvGvcpCommand command,
			   guint64 address, size_t size, void *buffer, GError **error)
{
	ArvGvcpCommand expected_ack_command;
	ArvGvcpPacket *ack_packet = io_data->buffer;
	ArvGvcpPacket *packet;
	const char *operation;
	size_t packet_size;
	size_t ack_size;
	unsigned int n_retries = 0;
	gboolean success = FALSE;
	ArvGvcpError command_error = ARV_GVCP_ERROR_NONE;
	int count;

	switch (command) {
		case ARV_GVCP_COMMAND_READ_MEMORY_CMD:
			operation = "read_memory";
			expected_ack_command = ARV_GVCP_COMMAND_READ_MEMORY_ACK;
			ack_size = arv_gvcp_packet_get_read_memory_ack_size (size);
			break;
		case ARV_GVCP_COMMAND_WRITE_MEMORY_CMD:
			operation = "write_memory";
			expected_ack_command = ARV_GVCP_COMMAND_WRITE_MEMORY_ACK;
			ack_size = arv_gvcp_packet_get_write_memory_ack_size ();
			break;
		case ARV_GVCP_COMMAND_READ_REGISTER_CMD:
			operation = "read_register";
			expected_ack_command = ARV_GVCP_COMMAND_READ_REGISTER_ACK;
			ack_size = arv_gvcp_packet_get_read_register_ack_size ();
			break;
		case ARV_GVCP_COMMAND_WRITE_REGISTER_CMD:
			operation = "write_register";
			expected_ack_command = ARV_GVCP_COMMAND_WRITE_REGISTER_ACK;
			ack_size = arv_gvcp_packet_get_write_register_ack_size ();
			break;
		default:
			g_assert_not_reached ();
	}

	g_return_val_if_fail (ack_size <= ARV_GV_DEVICE_BUFFER_SIZE, FALSE);

	g_mutex_lock (&io_data->mutex);

	io_data->packet_id = arv_gvcp_next_packet_id (io_data->packet_id);

	switch (command) {
		case ARV_GVCP_COMMAND_READ_MEMORY_CMD:
			packet = arv_gvcp_packet_new_read_memory_cmd (address, size,
								      io_data->packet_id, &packet_size);
			break;
		case ARV_GVCP_COMMAND_WRITE_MEMORY_CMD:
			packet = arv_gvcp_packet_new_write_memory_cmd (address, size, buffer,
								       io_data->packet_id, &packet_size);
			break;
		case ARV_GVCP_COMMAND_READ_REGISTER_CMD:
			packet = arv_gvcp_packet_new_read_register_cmd (address,
									io_data->packet_id, &packet_size);
			break;
		case ARV_GVCP_COMMAND_WRITE_REGISTER_CMD:
			packet = arv_gvcp_packet_new_write_register_cmd (address, *((guint32 *) buffer),
									 io_data->packet_id, &packet_size);
			break;
		default:
			g_assert_not_reached ();
	}

	do {
		GError *local_error = NULL;

		arv_gvcp_packet_debug (packet, ARV_DEBUG_LEVEL_TRACE);

		count = rt_dev_sendto(io_data->socket, packet, packet_size, 0,
					(struct sockaddr*)&io_data->sockaddr_device,
					sizeof(io_data->sockaddr_device));
		if (count < 0)
			arv_warning_device("[RtGvDevice::%s] rt_dev_sendto error: %d", operation, count);

		success = count >= 0;

		if (success) {
			int64_t timeout_ns;
			int64_t timeout_stop_ns;
			gboolean pending_ack;
			gboolean expected_answer;

			timeout_stop_ns = rt_timer_read() + io_data->gvcp_timeout_ns;

			do {
				pending_ack = FALSE;

				timeout_ns = timeout_stop_ns - rt_timer_read();
				if (timeout_ns == 0)
					timeout_ns = RTDM_TIMEOUT_NONE;

				if (rt_dev_ioctl(io_data->socket, RTNET_RTIOC_TIMEOUT, &timeout_ns) < 0)
        				arv_warning_device("Can't set timeout of IO data socket");

				count = rt_dev_recv(io_data->socket, io_data->buffer,
							ARV_GV_DEVICE_BUFFER_SIZE, 0);
				success = count >= sizeof (ArvGvcpHeader);

				if (success) {
					ArvGvcpPacketType packet_type;
					ArvGvcpCommand ack_command;
					guint16 packet_id;

					arv_gvcp_packet_debug (ack_packet, ARV_DEBUG_LEVEL_TRACE);

					packet_type = arv_gvcp_packet_get_packet_type (ack_packet);
					ack_command = arv_gvcp_packet_get_command (ack_packet);
					packet_id = arv_gvcp_packet_get_packet_id (ack_packet);

					if (ack_command == ARV_GVCP_COMMAND_PENDING_ACK &&
					    count >= arv_gvcp_packet_get_pending_ack_size ()) {
						gint64 pending_ack_timeout_ms = arv_gvcp_packet_get_pending_ack_timeout (ack_packet);
						pending_ack = TRUE;
						expected_answer = FALSE;

						timeout_stop_ns = rt_timer_read() + pending_ack_timeout_ms * 1000000;

						arv_debug_device ("[RtGvDevice::%s] Pending ack timeout = %" G_GINT64_FORMAT,
								operation, pending_ack_timeout_ms);
					} else if (packet_type == ARV_GVCP_PACKET_TYPE_ERROR ||
                                                   packet_type == ARV_GVCP_PACKET_TYPE_UNKNOWN_ERROR) {
						expected_answer = ack_command == expected_ack_command &&
							packet_id == io_data->packet_id;
						if (!expected_answer) {
							arv_info_device ("[RtGvDevice::%s] Unexpected answer (0x%02x)", operation,
									  packet_type);
						} else
							command_error = arv_gvcp_packet_get_packet_flags (ack_packet);
					} else  {
						expected_answer = packet_type == ARV_GVCP_PACKET_TYPE_ACK &&
							ack_command == expected_ack_command &&
							packet_id == io_data->packet_id &&
							count >= ack_size;
						if (!expected_answer) {
							arv_info_device ("[RtGvDevice::%s] Unexpected answer (0x%02x)", operation,
									  packet_type);
						}
					}
				} else {
					expected_answer = FALSE;
					if (local_error != NULL)
						arv_warning_device ("[RtGvDevice::%s] Ack reception error: %s", operation,
								    local_error->message);
					else
						arv_warning_device ("[RtGvDevice::%s] Ack reception timeout", operation);
					g_clear_error (&local_error);
				}
			} while (pending_ack || (!expected_answer && timeout_ns > 0));

			success = success && expected_answer;

			if (success && command_error == ARV_GVCP_ERROR_NONE) {
				switch (command) {
					case ARV_GVCP_COMMAND_READ_MEMORY_CMD:
						memcpy (buffer, arv_gvcp_packet_get_read_memory_ack_data (ack_packet), size);
						break;
					case ARV_GVCP_COMMAND_WRITE_MEMORY_CMD:
						break;
					case ARV_GVCP_COMMAND_READ_REGISTER_CMD:
						*((gint32 *) buffer) = arv_gvcp_packet_get_read_register_ack_value (ack_packet);
						break;
					case ARV_GVCP_COMMAND_WRITE_REGISTER_CMD:
						break;
					default:
						g_assert_not_reached ();
				}
			}
		} else {
			if (local_error != NULL)
				arv_warning_device ("[RtGvDevice::%s] Command sending error: %s", operation, local_error->message);
			g_clear_error (&local_error);
		}

		n_retries++;
	} while (!success && n_retries < io_data->gvcp_n_retries);

	arv_gvcp_packet_free (packet);

	g_mutex_unlock (&io_data->mutex);

	success = success && command_error == ARV_GVCP_ERROR_NONE;

	if (!success) {
		switch (command) {
			case ARV_GVCP_COMMAND_READ_MEMORY_CMD:
				memset (buffer, 0, size);
				break;
			case ARV_GVCP_COMMAND_WRITE_MEMORY_CMD:
				break;
			case ARV_GVCP_COMMAND_READ_REGISTER_CMD:
				*((guint32 *) buffer) = 0;
				break;
			case ARV_GVCP_COMMAND_WRITE_REGISTER_CMD:
				break;
			default:
				g_assert_not_reached ();
		}

		if (error != NULL && *error == NULL) {
			if (command_error != ARV_GVCP_ERROR_NONE)
				*error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_PROTOCOL_ERROR,
						      "GigEVision %s error (%s)", operation,
						      arv_gvcp_error_to_string (command_error));
			else
				*error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_TIMEOUT,
						      "GigEVision %s timeout", operation);
		}
	}

	return success;
}

static gboolean
_read_memory (ArvRtGvDeviceIOData *io_data, guint64 address, guint32 size, void *buffer, GError **error)
{
	return _send_cmd_and_receive_ack (io_data, ARV_GVCP_COMMAND_READ_MEMORY_CMD,
					  address, size, buffer, error);
}

static gboolean
_write_memory (ArvRtGvDeviceIOData *io_data, guint64 address, guint32 size, void *buffer, GError **error)
{
	return  _send_cmd_and_receive_ack (io_data, ARV_GVCP_COMMAND_WRITE_MEMORY_CMD,
					   address, size, buffer, error);
}

static gboolean
_read_register (ArvRtGvDeviceIOData *io_data, guint32 address, guint32 *value_placeholder, GError **error)
{
	return _send_cmd_and_receive_ack (io_data, ARV_GVCP_COMMAND_READ_REGISTER_CMD,
					  address, sizeof (guint32), value_placeholder, error);
}

static gboolean
_write_register (ArvRtGvDeviceIOData *io_data, guint32 address, guint32 value, GError **error)
{
	return _send_cmd_and_receive_ack (io_data, ARV_GVCP_COMMAND_WRITE_REGISTER_CMD,
					  address, sizeof (guint32), &value, error);
}

static gboolean
arv_rt_gv_device_read_memory (ArvDevice *device, guint64 address, guint32 size, void *buffer, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (device));
	int i;
	gint32 block_size;

	for (i = 0; i < (size + ARV_GVCP_DATA_SIZE_MAX - 1) / ARV_GVCP_DATA_SIZE_MAX; i++) {
		block_size = MIN (ARV_GVCP_DATA_SIZE_MAX, size - i * ARV_GVCP_DATA_SIZE_MAX);
		if (!_read_memory (priv->io_data,
				   address + i * ARV_GVCP_DATA_SIZE_MAX,
				   block_size, ((char *) buffer) + i * ARV_GVCP_DATA_SIZE_MAX, error))
			return FALSE;
	}

	return TRUE;
}

static gboolean
arv_rt_gv_device_write_memory (ArvDevice *device, guint64 address, guint32 size, void *buffer, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (device));
	int i;
	gint32 block_size;

	for (i = 0; i < (size + ARV_GVCP_DATA_SIZE_MAX - 1) / ARV_GVCP_DATA_SIZE_MAX; i++) {
		block_size = MIN (ARV_GVCP_DATA_SIZE_MAX, size - i * ARV_GVCP_DATA_SIZE_MAX);
		if (!_write_memory (priv->io_data,
				    address + i * ARV_GVCP_DATA_SIZE_MAX,
				    block_size, ((char *) buffer) + i * ARV_GVCP_DATA_SIZE_MAX, error))
			return FALSE;
	}

	return TRUE;
}

static gboolean
arv_rt_gv_device_read_register (ArvDevice *device, guint64 address, guint32 *value, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (device));

	return _read_register (priv->io_data, address, value, error);
}

static gboolean
arv_rt_gv_device_write_register (ArvDevice *device, guint64 address, guint32 value, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (device));

	return _write_register (priv->io_data, address, value, error);
}
#if 0
/* Heartbeat thread */

typedef struct {
	ArvRtGvDevice *gv_device;
	ArvRtGvDeviceIOData *io_data;
	int period_us;

	GCancellable *cancellable;
} ArvRtGvDeviceHeartbeatData;

static void *
arv_rt_gv_device_heartbeat_thread (void *data)
{
	ArvRtGvDeviceHeartbeatData *thread_data = data;
	ArvRtGvDeviceIOData *io_data = thread_data->io_data;
	GPollFD poll_fd;
	gboolean use_poll;
	GTimer *timer;
	guint32 value;

	timer = g_timer_new ();

	use_poll = g_cancellable_make_pollfd (thread_data->cancellable, &poll_fd);

	do {
		if (use_poll)
			g_poll (&poll_fd, 1, thread_data->period_us / 1000);
		else
			g_usleep (thread_data->period_us);

		if (io_data->is_controller) {
			guint counter = 1;

			/* TODO: Instead of reading the control register, Pylon does write the heartbeat
			 * timeout value, which is interresting, as doing this we could get an error
			 * ack packet which will indicate we lost the control access. */

			g_timer_start (timer);

			while (!_read_register (io_data, ARV_GVBS_CONTROL_CHANNEL_PRIVILEGE_OFFSET, &value, NULL) &&
			       g_timer_elapsed (timer, NULL) < ARV_GV_DEVICE_HEARTBEAT_RETRY_TIMEOUT_S &&
			       !g_cancellable_is_cancelled (thread_data->cancellable)) {
				g_usleep (ARV_GV_DEVICE_HEARTBEAT_RETRY_DELAY_US);
				counter++;
			}

			if (!g_cancellable_is_cancelled (thread_data->cancellable)) {
				arv_debug_device ("[RtGvDevice::Heartbeat] Ack value = %d", value);

				if (counter > 1)
					arv_debug_device ("[RtGvDevice::Heartbeat] Tried %u times", counter);

				if ((value & (ARV_GVBS_CONTROL_CHANNEL_PRIVILEGE_CONTROL |
					      ARV_GVBS_CONTROL_CHANNEL_PRIVILEGE_EXCLUSIVE)) == 0) {
					arv_warning_device ("[RtGvDevice::Heartbeat] Control access lost");

					arv_device_emit_control_lost_signal (ARV_DEVICE (thread_data->gv_device));

					io_data->is_controller = FALSE;
				}
			} else
				io_data->is_controller = FALSE;
		}
	} while (!g_cancellable_is_cancelled (thread_data->cancellable));

	if (use_poll)
		g_cancellable_release_fd (thread_data->cancellable);

	g_timer_destroy (timer);

	return NULL;
}
#endif
/* ArvRtGvDevice implementation */

/**
 * arv_rt_gv_device_take_control:
 * @gv_device: a #ArvRtGvDevice
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Returns: whether the control was successfully acquired
 *
 * Since: 0.8.3
 */

gboolean
arv_rt_gv_device_take_control (ArvRtGvDevice *gv_device, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	gboolean success = TRUE;

	success = arv_rt_gv_device_write_register (ARV_DEVICE (gv_device),
					     ARV_GVBS_CONTROL_CHANNEL_PRIVILEGE_OFFSET,
					     ARV_GVBS_CONTROL_CHANNEL_PRIVILEGE_CONTROL,
					     error);

	if (success)
		priv->io_data->is_controller = TRUE;
	else
		arv_warning_device ("[RtGvDevice::take_control] Can't get control access");

	return success;
}

/**
 * arv_rt_gv_device_leave_control:
 * @gv_device: a #ArvRtGvDevice
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Returns: whether the control was successfully relinquished
 *
 * Since: 0.8.3
 */

gboolean
arv_rt_gv_device_leave_control (ArvRtGvDevice *gv_device, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	gboolean success = TRUE;

	success = arv_rt_gv_device_write_register (ARV_DEVICE (gv_device),
					     ARV_GVBS_CONTROL_CHANNEL_PRIVILEGE_OFFSET,
					     0,
					     error);

	if (success)
		priv->io_data->is_controller = FALSE;
	else
		arv_warning_device ("[RtGvDevice::leave_control] Can't relinquish control access");

	return success;
}

guint64
arv_rt_gv_device_get_timestamp_tick_frequency (ArvRtGvDevice *gv_device, GError **error)
{
	GError *local_error = NULL;
	guint32 timestamp_tick_frequency_high;
	guint32 timestamp_tick_frequency_low;
	guint64 timestamp_tick_frequency;

	g_return_val_if_fail (ARV_IS_RT_GV_DEVICE (gv_device), 0);

	arv_rt_gv_device_read_register (ARV_DEVICE (gv_device),
				  ARV_GVBS_TIMESTAMP_TICK_FREQUENCY_HIGH_OFFSET,
				  &timestamp_tick_frequency_high, &local_error);
	if (local_error == NULL)
		arv_rt_gv_device_read_register (ARV_DEVICE (gv_device),
					  ARV_GVBS_TIMESTAMP_TICK_FREQUENCY_LOW_OFFSET,
					  &timestamp_tick_frequency_low, &local_error);

	if (local_error != NULL) {
		g_propagate_error (error, local_error);
		return 0;
	}

	timestamp_tick_frequency = ((guint64) timestamp_tick_frequency_high << 32) |
		timestamp_tick_frequency_low;

	return timestamp_tick_frequency;
}

guint
arv_rt_gv_device_get_packet_size (ArvRtGvDevice *gv_device, GError **error)
{
	return arv_device_get_integer_feature_value (ARV_DEVICE (gv_device), "GevSCPSPacketSize", error);
}

void
arv_rt_gv_device_set_packet_size (ArvRtGvDevice *gv_device, gint packet_size, GError **error)
{
	g_return_if_fail (packet_size > 0);

	arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevSCPSPacketSize", packet_size, error);
}

#if 0
static gboolean
test_packet_check (ArvDevice *device,
		   GPollFD *poll_fd,
		   GSocket *socket,
		   char *buffer,
                   guint max_size,
		   guint packet_size,
		   gboolean is_command)
{
	unsigned n_tries = 0;
	int n_events;
	size_t read_count;

	do {
		if (is_command) {
			arv_device_execute_command (device, "GevSCPSFireTestPacket", NULL);
		} else {
			arv_device_set_boolean_feature_value (device, "GevSCPSFireTestPacket", FALSE, NULL);
			arv_device_set_boolean_feature_value (device, "GevSCPSFireTestPacket", TRUE, NULL);
		}

		do {
			n_events = g_poll (poll_fd, 1, 10);
			if (n_events != 0) {
				arv_gpollfd_clear_one (poll_fd, socket);
				read_count = g_socket_receive (socket, buffer, max_size, NULL, NULL);
			}
			else
				read_count = 0;
			/* Discard late packets, read_count should be equal to packet size minus IP and UDP headers */
		} while (n_events != 0 && read_count != (packet_size - sizeof (struct iphdr) - sizeof (struct udphdr)));

		n_tries++;
	} while (n_events == 0 && n_tries < 3);

	return n_events != 0;
}

static guint
auto_packet_size (ArvRtGvDevice *gv_device, gboolean exit_early, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	ArvDevice *device = ARV_DEVICE (gv_device);
	ArvGcNode *node;
	GSocket *socket;
	GInetAddress *interface_address;
	GSocketAddress *interface_socket_address;
	GInetSocketAddress *local_address;
	GPollFD poll_fd;
	const guint8 *address_bytes;
	guint16 port;
	gboolean do_not_fragment;
	gboolean is_command;
	guint max_size, min_size;
	gint64 minimum, maximum, packet_size;
	guint inc;
	char *buffer;
	guint last_size = 0;
	gboolean success;

	g_return_val_if_fail (ARV_IS_RT_GV_DEVICE (gv_device), 1500);

	node = arv_device_get_feature (device, "GevSCPSFireTestPacket");
	if (!ARV_IS_GC_COMMAND (node) && !ARV_IS_GC_BOOLEAN (node)) {
		arv_info_device ("[RtGvDevice::auto_packet_size] No GevSCPSFireTestPacket feature found");
		return arv_device_get_integer_feature_value (device, "GevSCPSPacketSize", error);
	}

	inc = arv_device_get_integer_feature_increment (device, "GevSCPSPacketSize", NULL);
	if (inc < 1)
		inc = 1;
	packet_size = arv_device_get_integer_feature_value (device, "GevSCPSPacketSize", NULL);
	arv_device_get_integer_feature_bounds (device, "GevSCPSPacketSize", &minimum, &maximum, NULL);
	max_size = MIN (65536, maximum);
	min_size = MAX (ARV_GVSP_PACKET_PROTOCOL_OVERHEAD, minimum);

	if (max_size < min_size ||
	    inc > max_size - min_size ||
	    inc > 16) {
		arv_warning_device ("[RtGvDevice::auto_packet_size] Invalid GevSCPSPacketSize properties");
		return arv_device_get_integer_feature_value (device, "GevSCPSPacketSize", error);
	}

	is_command = ARV_IS_GC_COMMAND (node);

	interface_address = g_inet_socket_address_get_address (G_INET_SOCKET_ADDRESS (priv->io_data->interface_address));
	interface_socket_address = g_inet_socket_address_new (interface_address, 0);
	socket = g_socket_new (G_SOCKET_FAMILY_IPV4, G_SOCKET_TYPE_DATAGRAM, G_SOCKET_PROTOCOL_UDP, NULL);
	g_socket_bind (socket, interface_socket_address, FALSE, NULL);
	local_address = G_INET_SOCKET_ADDRESS (g_socket_get_local_address (socket, NULL));
	port = g_inet_socket_address_get_port (local_address);

	address_bytes = g_inet_address_to_bytes (interface_address);
	arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevSCDA", g_htonl (*((guint32 *) address_bytes)), NULL);
	arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevSCPHostPort", port, NULL);

	g_clear_object (&local_address);
	g_clear_object (&interface_socket_address);

	do_not_fragment = arv_device_get_boolean_feature_value (device, "GevSCPSDoNotFragment", NULL);
	arv_device_set_boolean_feature_value (device, "GevSCPSDoNotFragment", TRUE, NULL);

	poll_fd.fd = g_socket_get_fd (socket);
	poll_fd.events =  G_IO_IN;
	poll_fd.revents = 0;

	arv_gpollfd_prepare_all (&poll_fd, 1);

	buffer = g_malloc (max_size);

	success = test_packet_check (device, &poll_fd, socket, buffer, max_size, packet_size, is_command);

	/* When exit_early is set, the function only checks the current packet size is working.
	 * If not, the full automatic packet size adjustment is run. */
	if (success && exit_early) {
		arv_info_device ("[RtGvDevice::auto_packet_size] Current packet size check successfull "
				  "(%" G_GINT64_FORMAT " bytes)",
				  packet_size);
	} else {
		guint current_size = CLAMP (packet_size, min_size, max_size);

		do {
			current_size = ((current_size + inc - 1) / inc) * inc;

			arv_info_device ("[RtGvDevice::auto_packet_size] Try packet size = %d", current_size);
			arv_device_set_integer_feature_value (device, "GevSCPSPacketSize", current_size, NULL);

			current_size = arv_device_get_integer_feature_value (device, "GevSCPSPacketSize", NULL);

			if (current_size == last_size)
				break;

			last_size = current_size;

			success = test_packet_check (device, &poll_fd, socket, buffer, max_size, current_size,
                                                     is_command);

			if (success) {
				packet_size = current_size;
				min_size = current_size;
				current_size = (max_size - min_size) / 2 + min_size;
			} else {
				max_size = current_size;
				current_size = (max_size - min_size) / 2 + min_size;
			}
		} while ((max_size - min_size) > 16);

		arv_device_set_integer_feature_value (device, "GevSCPSPacketSize", packet_size, error);

		arv_info_device ("[RtGvDevice::auto_packet_size] Packet size set to %" G_GINT64_FORMAT " bytes",
				  packet_size);
	}

	g_clear_pointer (&buffer, g_free);
	g_clear_object (&socket);

	arv_gpollfd_finish_all (&poll_fd, 1);

	arv_device_set_boolean_feature_value (device, "GevSCPSDoNotFragment", do_not_fragment, NULL);

	return packet_size;
}

/**
 * arv_rt_gv_device_auto_packet_size:
 * @gv_device: a #ArvRtGvDevice
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Automatically determine the biggest packet size that can be used data streaming, and set GevSCPSPacketSize value
 * accordingly. This function relies on the GevSCPSFireTestPacket feature.
 *
 * Returns: The automatic packet size, in bytes, or the current one if GevSCPSFireTestPacket is not supported.
 *
 * Since: 0.6.0
 */

guint
arv_rt_gv_device_auto_packet_size (ArvRtGvDevice *gv_device, GError **error)
{
	return auto_packet_size (gv_device, FALSE, error);
}
#endif

/**
 * arv_rt_gv_device_set_packet_size_adjustment:
 * @gv_device: a #ArvRtGvDevice
 * @adjustment: a #ArvGvPacketSizeAdjustment option
 *
 * Sets the option for the packet size adjustment happening at stream object creation. See
 * arv_gv_device_auto_packet_size() for a description of the packet adjustment feature. The default behaviour is
 * @ARV_GV_PACKET_SIZE_ADJUSTEMENT_ON_FAILURE_ONCE, which means the packet size is adjusted if the current packet size
 * check fails, and only the first time arv_device_create_stream() is successfully called during @gv_device instance
 * life.
 *
 * Since: 0.8.3
 */

void
arv_rt_gv_device_set_packet_size_adjustment (ArvRtGvDevice *gv_device, ArvGvPacketSizeAdjustment adjustment)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));

	priv->packet_size_adjustment = adjustment;
}

/**
 * arv_rt_gv_device_get_current_ip:
 * @gv_device: a #ArvRtGvDevice
 * @ip: (out): a IP address placeholder
 * @mask: (out) (optional): a netmask placeholder
 * @gateway: (out) (optional): a gateway IP address placeholder
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Get the current IP address setting of device.
 *
 * Since: 0.8.22
 */

void
arv_rt_gv_device_get_current_ip (ArvRtGvDevice *gv_device,
                              GInetAddress **ip, GInetAddressMask **mask, GInetAddress **gateway,
                              GError **error)
{
	guint32 be_ip_int;
	guint32 be_mask_int;
	guint32 be_gateway_int;
	guint32 value;
	GInetAddress *netmask;

	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));
	g_return_if_fail (ip != NULL);

	value = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPAddress", NULL);
	be_ip_int = g_htonl(value);

	*ip = g_inet_address_new_from_bytes ((guint8 *) &be_ip_int, G_SOCKET_FAMILY_IPV4);

	if (mask != NULL) {
		value = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device),
                                                              "GevCurrentSubnetMask", NULL);
		be_mask_int = g_htonl(value);

		netmask = g_inet_address_new_from_bytes ((guint8 *) &be_mask_int, G_SOCKET_FAMILY_IPV4);
		*mask = g_inet_address_mask_new (netmask, 32, NULL);
		g_object_unref (netmask);
	}

	if (gateway != NULL) {
		value = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device),
                                                              "GevCurrentDefaultGateway", NULL);
		be_gateway_int = g_htonl(value);

		*gateway = g_inet_address_new_from_bytes ((guint8 *) &be_gateway_int, G_SOCKET_FAMILY_IPV4);
	}
}

/**
 * arv_rt_gv_device_get_persistent_ip:
 * @gv_device: a #ArvRtGvDevice
 * @ip: (out): a IP address placeholder
 * @mask: (out) (optional): a netmask placeholder
 * @gateway: (out) (optional): a gateway IP address placeholder
 * @error: a #GError placeholder
 *
 * Get the persistent IP address setting of device.
 *
 * Since: 0.8.22
 */

void
arv_rt_gv_device_get_persistent_ip (ArvRtGvDevice *gv_device,
                                    GInetAddress **ip, GInetAddressMask **mask, GInetAddress **gateway,
                                    GError **error)
{
	guint32 be_ip_int;
	guint32 be_mask_int;
	guint32 be_gateway_int;
	guint32 value;
	GInetAddress *netmask;

	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));
	g_return_if_fail (ip != NULL);

	value = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device),
                                                      "GevPersistentIPAddress", NULL);
	be_ip_int = g_htonl(value);

	*ip = g_inet_address_new_from_bytes ((guint8 *) &be_ip_int, G_SOCKET_FAMILY_IPV4);

	if (mask != NULL) {
		value = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device),
                                                              "GevPersistentSubnetMask", NULL);
		be_mask_int = g_htonl(value);

		netmask = g_inet_address_new_from_bytes ((guint8 *) &be_mask_int, G_SOCKET_FAMILY_IPV4);
		*mask = g_inet_address_mask_new (netmask, 32, NULL);
		g_object_unref (netmask);
	}

	if (gateway != NULL) {
		value = arv_device_get_integer_feature_value (ARV_DEVICE (gv_device),
                                                              "GevPersistentDefaultGateway", NULL);
		be_gateway_int = g_htonl(value);

		*gateway = g_inet_address_new_from_bytes ((guint8 *) &be_gateway_int, G_SOCKET_FAMILY_IPV4);
	}
}

/**
 * arv_rt_gv_device_set_persistent_ip:
 * @gv_device: a #ArvRtGvDevice
 * @ip: (nullable): IPv4 address
 * @mask: (nullable): Netmask
 * @gateway: (nullable): Gateway IPv4 address
 * @error: a #GError placeholder
 *
 * Sets the persistent IP address to device.
 * Also disable DHCP then enable persistent IP mode.
 *
 * Since: 0.8.22
 */

void
arv_rt_gv_device_set_persistent_ip (ArvRtGvDevice *gv_device,
                                    GInetAddress *ip, GInetAddressMask *mask, GInetAddress *gateway,
                                     GError **error)
{
	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));

	if (G_IS_INET_ADDRESS (ip)) {
		GError *local_error = NULL;
		const guint8 *ip_bytes;
		guint32 be_value;
		guint32 ip_int;

		/* GigEVision specification does not support IPv6 */
		if (g_inet_address_get_family (ip) != G_SOCKET_FAMILY_IPV4) {
			g_set_error (error, ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
						"IP address is not IPv4 address");
			return;
		}

		ip_bytes = g_inet_address_to_bytes (ip);
		be_value = ((guint32)ip_bytes[3] << 24) | (ip_bytes[2] << 16) | (ip_bytes[1] << 8) | ip_bytes[0];
		ip_int = GUINT32_FROM_BE(be_value);

		arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevPersistentIPAddress", ip_int, &local_error);
		if (local_error != NULL) {
			g_propagate_error(error, local_error);
			return;
		}
	}

	if (G_IS_INET_ADDRESS_MASK (mask)) {
		GError *local_error = NULL;
		const guint8 *mask_bytes;
		guint32 be_value;
		guint mask_length;
		guint32 mask_int;

		/* GigEVision specification does not support IPv6 */
		if (g_inet_address_mask_get_family (mask) != G_SOCKET_FAMILY_IPV4) {
			g_set_error (error, ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
						"Netmask is not IPv4 address");
			return;
		}

		mask_length = g_inet_address_mask_get_length (mask);
		mask_bytes = g_inet_address_to_bytes (g_inet_address_mask_get_address (mask));

		if (mask_length == 32) {
			/* Bitmask format (255.255.255.0) */
			be_value = ((guint32)mask_bytes[3] << 24) | (mask_bytes[2] << 16) | (mask_bytes[1] << 8) | mask_bytes[0];
		} else {
			/* CIDR(slash) format (192.168.1.0/24) */
			be_value = ~(~(guint32)0 >> mask_length);
		}
		mask_int = GUINT32_FROM_BE(be_value);
		arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevPersistentSubnetMask", mask_int, &local_error);
		if (local_error != NULL) {
			g_propagate_error(error, local_error);
			return;
		}
	}

	if (G_IS_INET_ADDRESS (gateway)) {
		GError *local_error = NULL;
		const guint8 *gateway_bytes;
		guint32 be_value;
		guint32 gateway_int;

		/* GigEVision specification does not support IPv6 */
		if (g_inet_address_get_family (gateway) != G_SOCKET_FAMILY_IPV4) {
			g_set_error (error, ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
						"Gateway address is not IPv4 address");
			return;
		}

		gateway_bytes = g_inet_address_to_bytes (gateway);
		be_value = ((guint32)gateway_bytes[3] << 24) | (gateway_bytes[2] << 16) | (gateway_bytes[1] << 8) | gateway_bytes[0];
		gateway_int = GUINT32_FROM_BE(be_value);
		arv_device_set_integer_feature_value (ARV_DEVICE (gv_device), "GevPersistentDefaultGateway", gateway_int, &local_error);
		if (local_error != NULL) {
			g_propagate_error(error, local_error);
			return;
		}
	}

	arv_rt_gv_device_set_ip_configuration_mode (gv_device, ARV_GV_IP_CONFIGURATION_MODE_PERSISTENT_IP, error);
}


/**
 * arv_rt_gv_device_set_persistent_ip_from_string:
 * @gv_device: a #ArvRtGvDevice
 * @ip: (nullable): IPv4 address in string format
 * @mask: (nullable): netmask in string format
 * @gateway: (nullable): Gateway IPv4 address in string format
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Sets the persistent IP address to device.
 *
 * Since: 0.8.22
 */

void
arv_rt_gv_device_set_persistent_ip_from_string (ArvRtGvDevice *gv_device,
                                                const char *ip, const char *mask, const char *gateway,
                                                GError **error)
{
	GError *local_error = NULL;
	GInetAddress *ip_gi = NULL;
	GInetAddressMask *mask_gi = NULL;
	GInetAddress *gateway_gi = NULL;

	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));

	if (ip != NULL)
		ip_gi = g_inet_address_new_from_string (ip);

	if (mask != NULL)
		mask_gi = g_inet_address_mask_new_from_string (mask, NULL);

	if (gateway != NULL)
		gateway_gi = g_inet_address_new_from_string (gateway);

	if (ip != NULL && ip_gi == NULL) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
                                           "IP address could not be parsed: \"%s\"", ip);
	} else if (mask != NULL && mask_gi == NULL) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
                                           "Netmask could not be parsed: \"%s\"", mask);
	} else if (gateway != NULL && gateway_gi == NULL) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
                                           "Gateway address could not be parsed: \"%s\"", gateway);
	}
	if (local_error != NULL){
		g_propagate_error (error, local_error);
		g_clear_object (&ip_gi);
		g_clear_object (&mask_gi);
		g_clear_object (&gateway_gi);
		return;
	}
	arv_rt_gv_device_set_persistent_ip (gv_device, ip_gi, mask_gi, gateway_gi, error);
	g_clear_object (&ip_gi);
	g_clear_object (&mask_gi);
	g_clear_object (&gateway_gi);
}

/**
 * arv_rt_gv_device_get_ip_configuration_mode:
 * @gv_device: a #ArvRtGvDevice
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Get the IP address configuration mode.
 *
 * Returns: IP address configuration mode
 *
 * Since: 0.8.22
 */

ArvGvIpConfigurationMode
arv_rt_gv_device_get_ip_configuration_mode (ArvRtGvDevice *gv_device, GError **error)
{
	gboolean dhcp_enabled;
	gboolean persistent_ip_enabled;

	g_return_val_if_fail (ARV_IS_RT_GV_DEVICE (gv_device), 0);

	if (arv_device_is_feature_available (ARV_DEVICE (gv_device), "GevIPConfigurationStatus", error))
		return arv_device_get_integer_feature_value (ARV_DEVICE (gv_device), "GevIPConfigurationStatus", error);

	dhcp_enabled = arv_device_get_boolean_feature_value( ARV_DEVICE (gv_device), "GevCurrentIPConfigurationDHCP", error);
	if (*error != NULL)
		return ARV_GV_IP_CONFIGURATION_MODE_NONE;
	persistent_ip_enabled = arv_device_get_boolean_feature_value( ARV_DEVICE (gv_device), "GevCurrentIPConfigurationPersistentIP", error);
	if (*error != NULL)
		return ARV_GV_IP_CONFIGURATION_MODE_NONE;

	if (dhcp_enabled && !persistent_ip_enabled)
		return ARV_GV_IP_CONFIGURATION_MODE_DHCP;
	if (!dhcp_enabled && persistent_ip_enabled)
		return ARV_GV_IP_CONFIGURATION_MODE_PERSISTENT_IP;
	return ARV_GV_IP_CONFIGURATION_MODE_LLA;
}


/**
 * arv_rt_gv_device_set_ip_configuration_mode:
 * @gv_device: a #ArvRtGvDevice
 * @mode: IP address configuration mode
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Sets the IP address configuration mode.
 * Available modes are ARV_GV_IP_CONFIGURATION_MODE_DHCP, ARV_GV_IP_CONFIGURATION_MODE_PERSISTENT_IP,
 * ARV_GV_IP_CONFIGURATION_MODE_LLA
 *
 * Since: 0.8.22
 */

void
arv_rt_gv_device_set_ip_configuration_mode (ArvRtGvDevice *gv_device, ArvGvIpConfigurationMode mode, GError **error)
{
	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));
        g_return_if_fail ((mode == ARV_GV_IP_CONFIGURATION_MODE_DHCP) ||
                          (mode == ARV_GV_IP_CONFIGURATION_MODE_PERSISTENT_IP) ||
                          (mode == ARV_GV_IP_CONFIGURATION_MODE_LLA));

	if (mode == ARV_GV_IP_CONFIGURATION_MODE_PERSISTENT_IP) {
		/* Persistent IP: disable DHCP, enable persistent IP */
		arv_device_set_boolean_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPConfigurationDHCP", FALSE, NULL);
		arv_device_set_boolean_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPConfigurationPersistentIP", TRUE, NULL);
	} else if (mode == ARV_GV_IP_CONFIGURATION_MODE_DHCP) {
		/* DHCP: enable DHCP, disable persistent IP */
		arv_device_set_boolean_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPConfigurationDHCP", TRUE, NULL);
		arv_device_set_boolean_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPConfigurationPersistentIP", FALSE, NULL);
	} else {
		/* LLA: disable both */
		arv_device_set_boolean_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPConfigurationDHCP", FALSE, NULL);
		arv_device_set_boolean_feature_value (ARV_DEVICE (gv_device),
                                                      "GevCurrentIPConfigurationPersistentIP", FALSE, NULL);
	}
}

/**
 * arv_rt_gv_device_is_controller:
 * @gv_device: a #ArvRtGvDevice
 *
 * Returns: value indicating whether the ArvRtGvDevice has control access to the camera
 *
 * Since: 0.8.0
 */

gboolean
arv_rt_gv_device_is_controller (ArvRtGvDevice *gv_device)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	g_return_val_if_fail (ARV_IS_RT_GV_DEVICE (gv_device), 0);

	return priv->io_data->is_controller;
}

static char *
_load_genicam (ArvRtGvDevice *gv_device, guint32 address, size_t  *size, GError **error)
{
	char filename[ARV_GVBS_XML_URL_SIZE];
	char *genicam = NULL;
	char *scheme = NULL;
	char *path = NULL;
	guint64 file_address;
	guint64 file_size;

	g_return_val_if_fail (size != NULL, NULL);

	*size = 0;

	if (!arv_rt_gv_device_read_memory (ARV_DEVICE (gv_device), address, ARV_GVBS_XML_URL_SIZE, filename, error))
		return NULL;

	filename[ARV_GVBS_XML_URL_SIZE - 1] = '\0';

	arv_info_device ("[RtGvDevice::load_genicam] xml url = '%s' at 0x%x", filename, address);

	arv_parse_genicam_url (filename, -1, &scheme, NULL, &path, NULL, NULL,
			       &file_address, &file_size);

	if (g_ascii_strcasecmp (scheme, "file") == 0) {
		gsize len;

		g_file_get_contents (path, &genicam, &len, NULL);
		if (genicam)
			*size = len;
	} else if (g_ascii_strcasecmp (scheme, "local") == 0) {
		arv_info_device ("[RtGvDevice::load_genicam] Xml address = 0x%" G_GINT64_MODIFIER "x - "
				  "size = 0x%" G_GINT64_MODIFIER "x - %s", file_address, file_size, path);

		if (file_size > 0) {
			genicam = g_malloc (file_size);
			if (arv_rt_gv_device_read_memory (ARV_DEVICE (gv_device), file_address, file_size,
							  genicam, NULL)) {

				if (arv_debug_check (ARV_DEBUG_CATEGORY_MISC, ARV_DEBUG_LEVEL_DEBUG)) {
					GString *string = g_string_new ("");

					g_string_append_printf (string,
								"[RtGvDevice::load_genicam] Raw data size = 0x%"
								G_GINT64_MODIFIER "x\n", file_size);
					arv_g_string_append_hex_dump (string, genicam, file_size);

					arv_debug_misc ("%s", string->str);

					g_string_free (string, TRUE);
				}

				if (g_str_has_suffix (path, ".zip")) {
					ArvZip *zip;
					const GSList *zip_files;

					arv_info_device ("[RtGvDevice::load_genicam] Zipped xml data");

					zip = arv_zip_new (genicam, file_size);
					zip_files = arv_zip_get_file_list (zip);

					if (zip_files != NULL) {
						const char *zip_filename;
						void *tmp_buffer;
						size_t tmp_buffer_size;

						zip_filename = arv_zip_file_get_name (zip_files->data);
						tmp_buffer = arv_zip_get_file (zip, zip_filename,
									       &tmp_buffer_size);

						g_free (genicam);
						file_size = tmp_buffer_size;
						genicam = tmp_buffer;
					} else
						arv_warning_device ("[RtGvDevice::load_genicam] Invalid format");
					arv_zip_free (zip);
				}
				*size = file_size;
			} else {
				g_free (genicam);
				genicam = NULL;
				*size = 0;
			}
		}
	} else if (g_ascii_strcasecmp (scheme, "http")) {
		GFile *file;
		GFileInputStream *stream;

		file = g_file_new_for_uri (filename);
		stream = g_file_read (file, NULL, NULL);
		if(stream) {
			GDataInputStream *data_stream;
			gsize len;

			data_stream = g_data_input_stream_new (G_INPUT_STREAM (stream));
			genicam = g_data_input_stream_read_upto (data_stream, "", 0, &len, NULL, NULL);

			if (genicam)
				*size = len;

			g_object_unref (data_stream);
			g_object_unref (stream);
		}
		g_object_unref (file);
	} else {
		g_critical ("Unkown GENICAM url scheme: '%s'", filename);
	}

	g_free (scheme);
	g_free (path);

	return genicam;
}

static const char *
_get_genicam_xml (ArvDevice *device, size_t *size, GError **error)
{
	ArvRtGvDevice *gv_device = ARV_RT_GV_DEVICE (device);
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	GError *local_error = NULL;
	char *xml;

	if (priv->genicam_xml != NULL) {
		*size = priv->genicam_xml_size;
		return priv->genicam_xml;
	}

	*size = 0;

	xml = _load_genicam (gv_device, ARV_GVBS_XML_URL_0_OFFSET, size, &local_error);
	if (xml == NULL && local_error == NULL)
		xml = _load_genicam (gv_device, ARV_GVBS_XML_URL_1_OFFSET, size, &local_error);

	if (local_error != NULL) {
		g_propagate_error (error, local_error);
		return NULL;
	}

	priv->genicam_xml = xml;
	priv->genicam_xml_size = *size;

	return xml;
}

static const char *
arv_rt_gv_device_get_genicam_xml (ArvDevice *device, size_t *size)
{
	return _get_genicam_xml (device, size, NULL);
}

static void
arv_rt_gv_device_load_genicam (ArvRtGvDevice *gv_device, GError **error)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	const char *genicam;
	size_t size;

	genicam = _get_genicam_xml (ARV_DEVICE (gv_device), &size, error);
	if (genicam != NULL) {
		priv->genicam = arv_gc_new (ARV_DEVICE (gv_device), genicam, size);

		arv_gc_set_default_node_data (priv->genicam, "GevCurrentIPConfigurationLLA",
					      "<Boolean Name=\"GevCurrentIPConfigurationLLA\">"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevCurrentIPConfigurationLLA</pValue>"
					      "</Boolean>",
					      "<MaskedIntReg Name=\"ArvGevCurrentIPConfigurationLLA\">"
					      "  <Address>0x14</Address>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <Bit>29</Bit>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevCurrentIPConfigurationDHCP",
					      "<Boolean Name=\"GevCurrentIPConfigurationDHCP\">"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevCurrentIPConfigurationDHCP</pValue>"
					      "</Boolean>",
					      "<MaskedIntReg Name=\"ArvGevCurrentIPConfigurationDHCP\">"
					      "  <Address>0x14</Address>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <Bit>30</Bit>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevCurrentIPConfigurationPersistentIP",
					      "<Boolean Name=\"GevCurrentIPConfigurationPersistentIP\">"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevCurrentIPConfigurationPersistentIP</pValue>"
					      "</Boolean>",
					      "<MaskedIntReg Name=\"ArvGevCurrentIPConfigurationPersistentIP\">"
					      "  <Address>0x14</Address>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <Bit>31</Bit>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "DeviceVendorName",
					      "<StringReg Name=\"DeviceVendorName\">"
					      "<DisplayName>Vendor Name</DisplayName>"
					      "<Address>0x48</Address>"
					      "<Length>32</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "</StringReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "DeviceModelName",
					      "<StringReg Name=\"DeviceModelName\">"
					      "<DisplayName>Model Name</DisplayName>"
					      "<Address>0x68</Address>"
					      "<Length>32</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "</StringReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "DeviceVersion",
					      "<StringReg Name=\"DeviceVersion\">"
					      "<DisplayName>Device Version</DisplayName>"
					      "<Address>0x88</Address>"
					      "<Length>32</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "</StringReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "DeviceManufacturerInfo",
					      "<StringReg Name=\"DeviceManufacturerInfo\">"
					      "<DisplayName>Manufacturer Info</DisplayName>"
					      "<Address>0xa8</Address>"
					      "<Length>48</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "</StringReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "DeviceID",
					      "<StringReg Name=\"DeviceID\">"
					      "<DisplayName>Device ID</DisplayName>"
					      "<Address>0xd8</Address>"
					      "<Length>16</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "</StringReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevPersistentIPAddress",
					      "<IntReg Name=\"GevPersistentIPAddress\">"
					      "<Address>0x64c</Address>"
					      "<Length>4</Length>"
					      "<AccessMode>RW</AccessMode>"
					      "<Endianess>BigEndian</Endianess>"
					      "<pPort>Device</pPort>"
					      "</IntReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevPersistentSubnetMask",
					      "<IntReg Name=\"GevPersistentSubnetMask\">"
					      "<Address>0x65c</Address>"
					      "<Length>4</Length>"
					      "<AccessMode>RW</AccessMode>"
					      "<Endianess>BigEndian</Endianess>"
					      "<pPort>Device</pPort>"
					      "</IntReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevPersistentDefaultGateway",
					      "<IntReg Name=\"GevPersistentDefaultGateway\">"
					      "<Address>0x66c</Address>"
					      "<Length>4</Length>"
					      "<AccessMode>RW</AccessMode>"
					      "<Endianess>BigEndian</Endianess>"
					      "<pPort>Device</pPort>"
					      "</IntReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevStreamChannelCount",
					      "<IntReg Name=\"GevStreamChannelCount\">"
					      "<Address>0x904</Address>"
					      "<Length>4</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<Endianess>BigEndian</Endianess>"
					      "<pPort>Device</pPort>"
					      "</IntReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevTimestampTickFrequency",
					      "<Integer Name=\"GevTimestampTickFrequency\">"
					      "<pValue>ArvGevTimestampTickFrequencyCalc</pValue>"
					      "</Integer>",
					      "<IntSwissKnife Name=\"ArvGevTimestampTickFrequencyCalc\">"
					      "<pVariable Name=\"HIGH\">ArvGevTimestampTickFrequencyHigh</pVariable>"
					      "<pVariable Name=\"LOW\">ArvGevTimestampTickFrequencyLow</pVariable>"
					      "<Formula>(HIGH&lt;&lt; 32) | LOW</Formula>"
					      "</IntSwissKnife>",
					      "<MaskedIntReg Name=\"ArvGevTimestampTickFrequencyHigh\">"
					      "<Visibility>Invisible</Visibility>"
					      "<Address>0x93C</Address>"
					      "<Length>4</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "<LSB>31</LSB>"
					      "<MSB>0</MSB>"
					      "<Sign>Unsigned</Sign>"
					      "<Endianess>BigEndian</Endianess>"
					      "</MaskedIntReg>",
					      "<MaskedIntReg Name=\"ArvGevTimestampTickFrequencyLow\">"
					      "<Visibility>Invisible</Visibility>"
					      "<Address>0x940</Address>"
					      "<Length>4</Length>"
					      "<AccessMode>RO</AccessMode>"
					      "<pPort>Device</pPort>"
					      "<LSB>31</LSB>"
					      "<MSB>0</MSB>"
					      "<Sign>Unsigned</Sign>"
					      "<Endianess>BigEndian</Endianess>"
					      "</MaskedIntReg>", NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevSCPHostPort",
					      "<Integer Name=\"GevSCPHostPort\">"
					      "  <Visibility>Expert</Visibility>"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevSCPHostPortReg</pValue>"
					      "</Integer>",
					      "<MaskedIntReg Name=\"ArvGevSCPHostPortReg\">"
					      "  <Address>0xd00</Address>"
					      "  <pAddress>GevSCPAddrCalc</pAddress>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <LSB>31</LSB>"
					      "  <MSB>16</MSB>"
					      "  <Sign>Unsigned</Sign>"
					      "  <Endianess>BigEndian</Endianess>"
					      "</MaskedIntReg>",
					      NULL);
#if 0
		arv_gc_set_default_node_data (priv->genicam, "GevSCPSFireTestPacket",
					      "<Command Name=\"GevSCPSFireTestPacket\">"
					      "<pIsLocked>TLParamsLocked</pIsLocked>"
					      "<pValue>GevSCPSFireTestPacketReg</pValue>"
					      "<CommandValue>1</CommandValue>"
					      "</Boolean>");
		arv_gc_set_default_node_data (priv->genicam, "GevSCPSFireTestPacketReg",
					      "<MaskedIntReg Name=\"GevSCPSFireTestPacketReg\">"
					      "<Address>0x0d04</Address>"
					      "<pAddress>GevSCPAddrCalc</pAddress>"
					      "<Length>4</Length>"
					      "<AccessMode>RW</AccessMode>"
					      "<Bit>0</Bit>"
					      "</MaskedIntReg>");
#endif
		arv_gc_set_default_node_data (priv->genicam, "GevSCPSDoNotFragment",
					      "<Boolean Name=\"GevSCPSDoNotFragment\">"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevSCPSDoNotFragmentReg</pValue>"
					      "</Boolean>",
					      "<MaskedIntReg Name=\"ArvGevSCPSDoNotFragmentReg\">"
					      "  <Address>0x0d04</Address>"
					      "  <pAddress>GevSCPAddrCalc</pAddress>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <Bit>1</Bit>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevSCPSBigEndian",
					      "<Boolean Name=\"GevSCPSBigEndian\">"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevSCPSBigEndianReg</pValue>"
					      "</Boolean>",
					      "  <MaskedIntReg Name=\"ArvGevSCPSBigEndianReg\">"
					      "  <Address>0x0d04</Address>"
					      "  <pAddress>GevSCPAddrCalc</pAddress>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <Bit>2</Bit>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevSCPSPacketSize",
					      "<Integer Name=\"GevSCPSPacketSize\">"
					      "  <Visibility>Expert</Visibility>"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevSCPSPacketSizeReg</pValue>"
					      "</Integer>",
					      "<MaskedIntReg Name=\"ArvGevSCPSPacketSizeReg\">"
					      "  <Address>0xd04</Address>"
					      "  <pAddress>GevSCPAddrCalc</pAddress>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <LSB>31</LSB>"
					      "  <MSB>16</MSB>"
					      "  <Sign>Unsigned</Sign>"
					      "  <Endianess>BigEndian</Endianess>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevSCDA",
					      "<Integer Name=\"GevSCDA\">"
					      "  <Visibility>Expert</Visibility>"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevSCDAReg</pValue>"
					      "</Integer>",
					      "<IntReg Name=\"ArvGevSCDAReg\">"
					      "  <Address>0xd18</Address>"
					      "  <pAddress>GevSCPAddrCalc</pAddress>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RW</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <Sign>Unsigned</Sign>"
					      "  <Endianess>BigEndian</Endianess>"
					      "</IntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevSCSP",
					      "<Integer Name=\"GevSCSP\">"
					      "  <Visibility>Expert</Visibility>"
					      "  <pIsLocked>TLParamsLocked</pIsLocked>"
					      "  <pValue>ArvGevSCSPReg</pValue>"
					      "</Integer>",
					      "<MaskedIntReg Name=\"ArvGevSCSPReg\">"
					      "  <Address>0xd1c</Address>"
					      "  <pAddress>GevSCPAddrCalc</pAddress>"
					      "  <Length>4</Length>"
					      "  <AccessMode>RO</AccessMode>"
					      "  <pPort>Device</pPort>"
					      "  <Cachable>NoCache</Cachable>"
					      "  <LSB>31</LSB>"
					      "  <MSB>16</MSB>"
					      "  <Sign>Unsigned</Sign>"
					      "  <Endianess>BigEndian</Endianess>"
					      "</MaskedIntReg>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "GevSCPAddrCalc",
					      "<IntSwissKnife Name= \"GevSCPAddrCalc\">"
					      "  <pVariable Name=\"SEL\">ArvGevStreamChannelSelector</pVariable>"
					      "  <Formula>SEL * 0x40</Formula>"
					      "</IntSwissKnife>",
					      "<Integer Name=\"ArvGevStreamChannelSelector\">"
					      "  <Value>0</Value>"
					      "  <Min>0</Min>"
					      "  <pMax>ArvGevStreamChannelSelectorMax</pMax>"
					      "  <Inc>1</Inc>"
					      "</Integer>",
					      "<IntSwissKnife Name=\"ArvGevStreamChannelSelectorMax\">"
					      "  <pVariable Name=\"N_STREAM_CHANNELS\">NumberOfStreamChannels</pVariable>"
					      "  <Formula>N_STREAM_CHANNELS - 1</Formula>"
					      "</IntSwissKnife>",
					      NULL);
		arv_gc_set_default_node_data (priv->genicam, "TLParamsLocked",
					      "<Integer Name=\"TLParamsLocked\">"
					      "<Visibility>Invisible</Visibility>"
					      "<Value>0</Value>"
					      "<Min>0</Min>"
					      "<Max>1</Max>"
					      "</Integer>",
					      NULL);
	}
}

/* ArvDevice implemenation */

static ArvStream *
arv_rt_gv_device_create_stream (ArvDevice *device, ArvStreamCallback callback, void *user_data, GError **error)
{
	ArvRtGvDevice *gv_device = ARV_RT_GV_DEVICE (device);
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	ArvStream *stream;
	guint32 n_stream_channels;

	n_stream_channels = arv_device_get_integer_feature_value (device, "GevStreamChannelCount", NULL);
	arv_info_device ("[RtGvDevice::create_stream] Number of stream channels = %d", n_stream_channels);

	if (n_stream_channels < 1) {
		g_set_error (error, ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_NO_STREAM_CHANNEL,
			     "No stream channel found");
		return NULL;
	}

	if (!priv->io_data->is_controller) {
		arv_warning_device ("[RtGvDevice::create_stream] Can't create stream without control access");
		g_set_error (error, ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_NOT_CONTROLLER,
			     "Controller privilege required for streaming control");
		return NULL;
	}

#if 0
	if (priv->packet_size_adjustment != ARV_GV_PACKET_SIZE_ADJUSTMENT_NEVER &&
	    ((priv->packet_size_adjustment != ARV_GV_PACKET_SIZE_ADJUSTMENT_ONCE &&
	      priv->packet_size_adjustment != ARV_GV_PACKET_SIZE_ADJUSTMENT_ON_FAILURE_ONCE) ||
	     !priv->first_stream_created)) {
		auto_packet_size (gv_device,
				  priv->packet_size_adjustment == ARV_GV_PACKET_SIZE_ADJUSTMENT_ON_FAILURE ||
				      priv->packet_size_adjustment == ARV_GV_PACKET_SIZE_ADJUSTMENT_ON_FAILURE_ONCE,
				  &local_error);
		if (local_error != NULL) {
			g_propagate_error (error, local_error);
			return NULL;
		}
	}
#endif

	stream = arv_rt_gv_stream_new (gv_device, callback, user_data, error);
	if (!ARV_IS_STREAM (stream))
		return NULL;

	if (!priv->is_packet_resend_supported)
		g_object_set (stream, "packet-resend", ARV_GV_STREAM_PACKET_RESEND_NEVER, NULL);

	priv->first_stream_created = TRUE;

	return stream;
}

static ArvGc *
arv_rt_gv_device_get_genicam (ArvDevice *device)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (device));

	return priv->genicam;
}

/**
 * arv_rt_gv_device_get_stream_options:
 * @gv_device: a #ArvRtGvDevice
 *
 * Returns: options for stream creation
 *
 * Since: 0.6.0
 */

ArvGvStreamOption
arv_rt_gv_device_get_stream_options (ArvRtGvDevice *gv_device)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	g_return_val_if_fail (ARV_IS_RT_GV_DEVICE (gv_device), ARV_GV_STREAM_OPTION_NONE);

	return priv->stream_options;
}

/**
 * arv_rt_gv_device_set_stream_options:
 * @gv_device: a #ArvRtGvDevice
 * @options: options for stream creation
 *
 * Sets the option used during stream creation. It must be called before arv_device_create_stream().
 *
 * Since: 0.6.0
 */

void
arv_rt_gv_device_set_stream_options (ArvRtGvDevice *gv_device, ArvGvStreamOption options)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	g_return_if_fail (ARV_IS_RT_GV_DEVICE (gv_device));

	priv->stream_options = options;
}

/**
 * arv_rt_gv_device_new:
 * @interface_address: address of the interface connected to the device
 * @device_address: device address
 * @error: a #GError placeholder, %NULL to ignore
 *
 * Returns: a newly created #ArvDevice using GigE protocol over RTnet
 *
 * Since: 0.8.0
 */

ArvDevice *
arv_rt_gv_device_new (GInetAddress *interface_address, GInetAddress *device_address, GError **error)
{
	return g_initable_new (ARV_TYPE_RT_GV_DEVICE, NULL, error,
			       "interface-address", interface_address,
			       "device-address", device_address,
			       NULL);
}

static void
arv_rt_gv_device_constructed (GObject *object)
{
	ArvRtGvDevice *gv_device = ARV_RT_GV_DEVICE (object);
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	ArvRtGvDeviceIOData *io_data;
#if 0
	ArvRtGvDeviceHeartbeatData *heartbeat_data;
#endif
	ArvGcRegisterDescriptionNode *register_description;
	ArvDomDocument *document;
	GError *local_error = NULL;
	char *address_string;
	guint32 capabilities;
	guint32 device_mode;
	int add_rtskbs = 400;
	struct sockaddr_in sock_addr_in;

        G_OBJECT_CLASS (arv_rt_gv_device_parent_class)->constructed (object);

	if (!G_IS_INET_ADDRESS (priv->interface_address) ||
	    !G_IS_INET_ADDRESS (priv->device_address)) {
		arv_device_take_init_error (ARV_DEVICE (object), g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_INVALID_PARAMETER,
									     "Invalid interface or device address"));
		return;
	}

	address_string = g_inet_address_to_string (priv->interface_address);
	arv_info_device ("[RtGvDevice::new] Interface address = %s", address_string);
	g_free (address_string);
	address_string = g_inet_address_to_string (priv->device_address);
	arv_info_device ("[RtGvDevice::new] Device address = %s", address_string);
	g_free (address_string);

	io_data = g_new0 (ArvRtGvDeviceIOData, 1);

	g_mutex_init (&io_data->mutex);

	io_data->packet_id = 65300; /* Start near the end of the circular counter */

	io_data->interface_address = g_inet_socket_address_new (priv->interface_address, 0);
	io_data->device_address = g_inet_socket_address_new (priv->device_address, ARV_GVCP_PORT);

	io_data->socket = rt_dev_socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
	if (io_data->socket < 0) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
					   "rt_dev_socket() failed");
		arv_device_take_init_error (ARV_DEVICE (gv_device), local_error);
		return;
	}

	if (rt_dev_ioctl(io_data->socket, RTNET_RTIOC_EXTPOOL, &add_rtskbs) != add_rtskbs)
		arv_warning_device("ioctl(RTNET_RTIOC_EXTPOOL)");

	if (!g_socket_address_to_native(io_data->interface_address,
					&sock_addr_in, sizeof(sock_addr_in),
					&local_error)) {
		if (local_error == NULL)
			local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
						   "Unknown error during g_socket_address_to_native(interface_address)");
		arv_device_take_init_error (ARV_DEVICE (gv_device), local_error);
		return;
	}
	if (rt_dev_bind(io_data->socket, (struct sockaddr*)&sock_addr_in, sizeof(sock_addr_in))) {
		local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
					   "rt_dev_bind() failed");
		arv_device_take_init_error (ARV_DEVICE (gv_device), local_error);
		return;
	}

	if (!g_socket_address_to_native(io_data->device_address,
					&io_data->sockaddr_device,
					sizeof(io_data->sockaddr_device),
					&local_error)) {
		if (local_error == NULL)
			local_error = g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_UNKNOWN,
						   "Unknown error during g_socket_address_to_native(device_address)");
		arv_device_take_init_error (ARV_DEVICE (gv_device), local_error);
		return;
	}

	io_data->buffer = g_malloc (ARV_GV_DEVICE_BUFFER_SIZE);
	io_data->gvcp_n_retries = ARV_GV_DEVICE_GVCP_N_RETRIES_DEFAULT;
	io_data->gvcp_timeout_ns = ARV_GV_DEVICE_GVCP_TIMEOUT_MS_DEFAULT * 1000000;

	priv->io_data = io_data;

	arv_rt_gv_device_load_genicam (gv_device, &local_error);
	if (local_error != NULL) {
		arv_device_take_init_error (ARV_DEVICE (gv_device), local_error);

		return;
	}

	if (!ARV_IS_GC (priv->genicam)) {
		arv_device_take_init_error (ARV_DEVICE (object),
					    g_error_new (ARV_DEVICE_ERROR, ARV_DEVICE_ERROR_GENICAM_NOT_FOUND,
							 "Invalid Genicam data"));
		return;
	}

	arv_rt_gv_device_take_control (gv_device, NULL);
#if 0
	heartbeat_data = g_new (ArvRtGvDeviceHeartbeatData, 1);
	heartbeat_data->gv_device = gv_device;
	heartbeat_data->io_data = io_data;
	heartbeat_data->period_us = ARV_GV_DEVICE_HEARTBEAT_PERIOD_US;
	heartbeat_data->cancellable = g_cancellable_new ();

	priv->heartbeat_data = heartbeat_data;

	priv->heartbeat_thread = g_thread_new ("arv_rt_gv_heartbeat", arv_rt_gv_device_heartbeat_thread, priv->heartbeat_data);
#endif
	arv_rt_gv_device_read_register (ARV_DEVICE (gv_device), ARV_GVBS_DEVICE_MODE_OFFSET, &device_mode, NULL);
	priv->is_big_endian_device = (device_mode & ARV_GVBS_DEVICE_MODE_BIG_ENDIAN) != 0;

	arv_rt_gv_device_read_register (ARV_DEVICE (gv_device), ARV_GVBS_GVCP_CAPABILITY_OFFSET, &capabilities, NULL);
	priv->is_packet_resend_supported = (capabilities & ARV_GVBS_GVCP_CAPABILITY_PACKET_RESEND) != 0;
	priv->is_write_memory_supported = (capabilities & ARV_GVBS_GVCP_CAPABILITY_WRITE_MEMORY) != 0;

	arv_info_device ("[RtGvDevice::new] Device endianness = %s", priv->is_big_endian_device ? "big" : "little");
	arv_info_device ("[RtGvDevice::new] Packet resend     = %s", priv->is_packet_resend_supported ? "yes" : "no");
	arv_info_device ("[RtGvDevice::new] Write memory      = %s", priv->is_write_memory_supported ? "yes" : "no");

	document = ARV_DOM_DOCUMENT (priv->genicam);
	register_description = ARV_GC_REGISTER_DESCRIPTION_NODE (arv_dom_document_get_document_element (document));
	arv_info_device ("[RtGvDevice::new] Legacy endianness handling = %s",
			  arv_gc_register_description_node_compare_schema_version (register_description, 1, 1, 0) < 0 ?
			  "yes" : "no");

	priv->init_success = TRUE;
}

static void
arv_rt_gv_device_init (ArvRtGvDevice *gv_device)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	priv->genicam = NULL;
	priv->genicam_xml = NULL;
	priv->genicam_xml_size = 0;
	priv->stream_options = ARV_GV_STREAM_OPTION_NONE;
}

static void
arv_rt_gv_device_finalize (GObject *object)
{
	ArvRtGvDevice *gv_device = ARV_RT_GV_DEVICE (object);
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);
	ArvRtGvDeviceIOData *io_data;
/*
	if (priv->heartbeat_thread != NULL) {
		ArvRtGvDeviceHeartbeatData *heartbeat_data;

		heartbeat_data = priv->heartbeat_data;

		g_cancellable_cancel (heartbeat_data->cancellable);
		g_thread_join (priv->heartbeat_thread);
		g_clear_object (&heartbeat_data->cancellable);

		g_clear_pointer (&heartbeat_data, g_free);

		priv->heartbeat_data = NULL;
		priv->heartbeat_thread = NULL;
	}
*/
	if (priv->init_success)
		arv_rt_gv_device_leave_control (gv_device, NULL);

	io_data = priv->io_data;
	if (io_data != NULL) {
		g_clear_object (&io_data->device_address);
		g_clear_object (&io_data->interface_address);
		if (io_data->socket >= 0)
			rt_dev_close(io_data->socket);
		g_clear_pointer (&io_data->buffer, g_free);
		g_mutex_clear (&io_data->mutex);

		g_clear_pointer (&priv->io_data, g_free);
	}

	g_clear_object (&priv->genicam);
	g_clear_pointer (&priv->genicam_xml, g_free);

	g_clear_object (&priv->interface_address);
	g_clear_object (&priv->device_address);

	G_OBJECT_CLASS (arv_rt_gv_device_parent_class)->finalize (object);
}

/**
 * arv_rt_gv_device_get_interface_address:
 * @device: a #ArvRtGvDevice
 *
 * Returns: (transfer none): the device host interface IP address.
 *
 * Since: 0.2.0
 */

GSocketAddress *arv_rt_gv_device_get_interface_address(ArvRtGvDevice *gv_device)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	return priv->io_data->interface_address;
}

/**
 * arv_rt_gv_device_get_device_address:
 * @device: a #ArvRtGvDevice
 *
 * Returns: (transfer none): the device IP address.
 *
 * since: 0.2.0
 */

GSocketAddress *arv_rt_gv_device_get_device_address(ArvRtGvDevice *gv_device)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (gv_device);

	return priv->io_data->device_address;
}

static void
arv_rt_gv_device_set_property (GObject *self, guint prop_id, const GValue *value, GParamSpec *pspec)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (self));

	switch (prop_id)
	{
		case PROP_GV_DEVICE_INTERFACE_ADDRESS:
			g_clear_object (&priv->interface_address);
			priv->interface_address = g_value_dup_object (value);
			break;
		case PROP_GV_DEVICE_DEVICE_ADDRESS:
			g_clear_object (&priv->device_address);
			priv->device_address = g_value_dup_object (value);
			break;
		case PROP_GV_DEVICE_PACKET_SIZE_ADJUSTEMENT:
			priv->packet_size_adjustment = g_value_get_enum (value);
			break;
		default:
			G_OBJECT_WARN_INVALID_PROPERTY_ID (self, prop_id, pspec);
			break;
	}
}

static void
arv_rt_gv_device_get_property (GObject *object, guint prop_id, GValue *value, GParamSpec *pspec)
{
	ArvRtGvDevicePrivate *priv = arv_rt_gv_device_get_instance_private (ARV_RT_GV_DEVICE (object));

	switch (prop_id) {
		case PROP_GV_DEVICE_PACKET_SIZE_ADJUSTEMENT:
			g_value_set_enum (value, priv->packet_size_adjustment);
			break;
		default:
			G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
			break;
	}
}

static void
arv_rt_gv_device_class_init (ArvRtGvDeviceClass *gv_device_class)
{
	GObjectClass *object_class = G_OBJECT_CLASS (gv_device_class);
	ArvDeviceClass *device_class = ARV_DEVICE_CLASS (gv_device_class);

	object_class->finalize = arv_rt_gv_device_finalize;
	object_class->constructed = arv_rt_gv_device_constructed;
	object_class->set_property = arv_rt_gv_device_set_property;
	object_class->get_property = arv_rt_gv_device_get_property;

	device_class->create_stream = arv_rt_gv_device_create_stream;
	device_class->get_genicam_xml = arv_rt_gv_device_get_genicam_xml;
	device_class->get_genicam = arv_rt_gv_device_get_genicam;
	device_class->read_memory = arv_rt_gv_device_read_memory;
	device_class->write_memory = arv_rt_gv_device_write_memory;
	device_class->read_register = arv_rt_gv_device_read_register;
	device_class->write_register = arv_rt_gv_device_write_register;

	g_object_class_install_property
		(object_class,
		 PROP_GV_DEVICE_INTERFACE_ADDRESS,
		 g_param_spec_object ("interface-address",
				      "Interface address",
				      "The address of the interface connected to the device",
				      G_TYPE_INET_ADDRESS,
				      G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
	g_object_class_install_property
		(object_class,
		 PROP_GV_DEVICE_DEVICE_ADDRESS,
		 g_param_spec_object ("device-address",
				      "Device address",
				      "The device address",
				      G_TYPE_INET_ADDRESS,
				      G_PARAM_WRITABLE | G_PARAM_CONSTRUCT_ONLY));
	g_object_class_install_property (object_class, PROP_GV_DEVICE_PACKET_SIZE_ADJUSTEMENT,
					 g_param_spec_enum ("packet-size-adjustment", "Packet size adjustment",
							    "Packet size adjustment option",
							    ARV_TYPE_GV_PACKET_SIZE_ADJUSTMENT,
							    ARV_GV_PACKET_SIZE_ADJUSTMENT_DEFAULT,
							    G_PARAM_READWRITE | G_PARAM_STATIC_STRINGS |
								G_PARAM_CONSTRUCT));
}
