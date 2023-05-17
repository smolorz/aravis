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

#ifndef ARV_RT_GV_STREAM_PRIVATE_H
#define ARV_RT_GV_STREAM_PRIVATE_H

#include <arvgvstreamprivate.h>
#include <arvrtgvdevice.h>
#include <arvrtgvstream.h>
#include <arvstream.h>
#include <gio/gio.h>

G_BEGIN_DECLS

ArvStream * 	arv_rt_gv_stream_new		(ArvRtGvDevice *gv_device, ArvStreamCallback callback, void *callback_data, GError **error);

G_END_DECLS

#endif
