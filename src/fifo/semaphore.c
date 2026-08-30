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

void fifo_semaphore_init(fifo_semaphore_t *semaphore, size_t initial_count){
     atomic_store_explicit(&semaphore->count, initial_count, memory_order_relaxed);
}

void fifo_semaphore_wait(fifo_semaphore_t *semaphore) {
     size_t count;

     for (;;) {
         count = atomic_load_explicit(&semaphore->count,
                                     memory_order_relaxed);

         while (count != 0) {
            if (atomic_compare_exchange_weak_explicit(
                    &semaphore->count,
                    &count,
                    count - 1,
                    memory_order_acquire,
                    memory_order_relaxed))
                return;
         }

         fifo_platform_yield();
     }
}

bool fifo_semaphore_trywait(fifo_semaphore_t *semaphore) {
     size_t count;

     count = atomic_load_explicit(&semaphore->count,
                                 memory_order_relaxed);

     while (count != 0) {
        if (atomic_compare_exchange_weak_explicit(
                &semaphore->count,
                &count,
                count - 1,
                memory_order_acquire,
                memory_order_relaxed))
            return true;
     }

     return false;
}

void fifo_semaphore_post(fifo_semaphore_t *semaphore) {
     atomic_fetch_add_explicit(&semaphore->count, 1, memory_order_release);
}

