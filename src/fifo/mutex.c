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

void fifo_mutex_init(fifo_mutex_t *mutex) {
    atomic_store_explicit(&mutex->state, 0, memory_order_relaxed);
}

void fifo_mutex_lock(fifo_mutex_t *mutex) {
     unsigned int expected;

     for (;;) {
         expected = 0;

         if (atomic_compare_exchange_strong_explicit(&mutex->state, &expected, 1, memory_order_acquire, memory_order_relaxed)) {
	      return; 
	 } else {
              fifo_platform_yield();
	 }
     }
}

bool fifo_mutex_trylock(fifo_mutex_t *mutex) {
     unsigned int expected = 0;

     return atomic_compare_exchange_strong_explicit(&mutex->state,
                                                    &expected,
                                                    1,
                                                    memory_order_acquire,
                                                    memory_order_relaxed);
}

void fifo_mutex_unlock(fifo_mutex_t *mutex) {
     atomic_store_explicit(&mutex->state, 0, memory_order_release);
}
