/*
 * libfifo - FIFO and synchronisation primitives
 *
 * Copyright (C) 2026 Gwen Nelson
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of
 * the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#include <stdatomic.h>

#include <libfifo/sync.h>

static fifo_yield_fn_t platform_yield_cb = NULL;

void fifo_platform_yield(void) {
     if (platform_yield_cb != NULL) {
         platform_yield_cb();
	 return;
     }
#if defined(__i386__) || defined(__x86_64__)
     __asm__ volatile ("pause");
#endif
}

void fifo_set_yield_ballback(fifo_yield_fn yield_cb) {
     platform_yield_cb = yield_cb;
}
