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

#ifndef ARV_RT_GV_STREAM_H
#define ARV_RT_GV_STREAM_H

#if !defined (ARV_H_INSIDE) && !defined (ARAVIS_COMPILATION)
#error "Only <arv.h> can be included directly."
#endif

#include <arvapi.h>
#include <arvtypes.h>
#include <arvstream.h>
#include <arvgvstream.h>

G_BEGIN_DECLS

#define ARV_TYPE_RT_GV_STREAM             (arv_rt_gv_stream_get_type ())
ARV_API G_DECLARE_FINAL_TYPE (ArvRtGvStream, arv_rt_gv_stream, ARV, RT_GV_STREAM, ArvStream)

ARV_API guint16		arv_rt_gv_stream_get_port	(ArvRtGvStream *gv_stream);
ARV_API void		arv_rt_gv_stream_get_statistics	(ArvRtGvStream *gv_stream,
							 guint64 *n_resent_packets,
							 guint64 *n_missing_packets);

ARV_API int		arv_rt_gv_stream_recv_frame	(ArvStream *gv_stream,
							 int64_t recv_frame_timeout);
ARV_API int		arv_rt_gv_stream_push_buffer	(ArvStream *stream, ArvBuffer *buffer);
ARV_API ArvBuffer *	arv_rt_gv_stream_try_pop_buffer	(ArvStream *stream);
ARV_API void		arv_rt_gv_stream_get_n_buffers	(ArvStream *stream,
							 gint *n_input_buffers,
							 gint *n_output_buffers);

ARV_API int	arv_rt_gv_stream_prealloc_packet_data	(ArvStream *stream, ArvBuffer *buffer);

G_END_DECLS

#endif
