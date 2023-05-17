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

#if !defined (ARV_H_INSIDE) && !defined (ARAVIS_COMPILATION)
#error "Only <arv.h> can be included directly."
#endif

#ifndef ARV_RT_GV_DEVICE_H
#define ARV_RT_GV_DEVICE_H

#include <arvapi.h>
#include <arvtypes.h>
#include <arvdevice.h>
#include <arvrtgvstream.h>
#include <gio/gio.h>
#include <arvgvdevice.h>

G_BEGIN_DECLS

#define ARV_TYPE_RT_GV_DEVICE             (arv_rt_gv_device_get_type ())
ARV_API G_DECLARE_FINAL_TYPE (ArvRtGvDevice, arv_rt_gv_device, ARV, RT_GV_DEVICE, ArvDevice)

ARV_API ArvDevice *		arv_rt_gv_device_new				(GInetAddress *interface_address, GInetAddress *device_address,
										 GError **error);

ARV_API gboolean		arv_rt_gv_device_take_control			(ArvRtGvDevice *gv_device, GError **error);
ARV_API gboolean		arv_rt_gv_device_leave_control			(ArvRtGvDevice *gv_device, GError **error);

ARV_API guint64			arv_rt_gv_device_get_timestamp_tick_frequency	(ArvRtGvDevice *gv_device, GError **error);

ARV_API GSocketAddress *	arv_rt_gv_device_get_interface_address		(ArvRtGvDevice *device);
ARV_API GSocketAddress *	arv_rt_gv_device_get_device_address		(ArvRtGvDevice *device);

ARV_API guint			arv_rt_gv_device_get_packet_size		(ArvRtGvDevice *gv_device, GError **error);
ARV_API void			arv_rt_gv_device_set_packet_size		(ArvRtGvDevice *gv_device, gint packet_size, GError **error);
ARV_API void			arv_rt_gv_device_set_packet_size_adjustment	(ArvRtGvDevice *gv_device,
										 ArvGvPacketSizeAdjustment adjustment);
//ARV_API guint			arv_rt_gv_device_auto_packet_size		(ArvRtGvDevice *gv_device, GError **error);

ARV_API ArvGvStreamOption	arv_rt_gv_device_get_stream_options		(ArvRtGvDevice *gv_device);
ARV_API void			arv_rt_gv_device_set_stream_options		(ArvRtGvDevice *gv_device, ArvGvStreamOption options);

ARV_API void			arv_rt_gv_device_get_current_ip			(ArvRtGvDevice *gv_device, GInetAddress **ip, GInetAddressMask **mask, GInetAddress **gateway, GError **error);
ARV_API void			arv_rt_gv_device_get_persistent_ip		(ArvRtGvDevice *gv_device, GInetAddress **ip, GInetAddressMask **mask, GInetAddress **gateway, GError **error);
ARV_API void			arv_rt_gv_device_set_persistent_ip		(ArvRtGvDevice *gv_device, GInetAddress *ip, GInetAddressMask *mask, GInetAddress *gateway, GError **error);
ARV_API void			arv_rt_gv_device_set_persistent_ip_from_string	(ArvRtGvDevice *gv_device, const char *ip, const char *mask, const char *gateway, GError **error);
ARV_API ArvGvIpConfigurationMode	arv_rt_gv_device_get_ip_configuration_mode	(ArvRtGvDevice *gv_device, GError **error);
ARV_API void			arv_rt_gv_device_set_ip_configuration_mode	(ArvRtGvDevice *gv_device, ArvGvIpConfigurationMode mode, GError **error);

ARV_API gboolean		arv_rt_gv_device_is_controller			(ArvRtGvDevice *gv_device);

G_END_DECLS

#endif
