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

#include <libfifo/sync.h>
#include <stdatomic.h>
#include <limits.h>

static bool fifo_ticket_before(unsigned int a, unsigned int b) {
       unsigned int distance = b - a;
       unsigned int half_range = (UINT_MAX / 2U) + 1U;
       return distance != 0 && distance < half_range;
}

void fifo_condition_init(fifo_condition_t *condition) {
     atomic_store_explicit(&condition->next_ticket, 0, memory_order_relaxed);
     atomic_store_explicit(&condition->wake_ticket, 0, memory_order_relaxed);
}

void fifo_condition_wait(fifo_condition_t *condition, fifo_mutex_t *mutex) {
     unsigned int ticket;
     unsigned int target;

     ticket = atomic_fetch_add_explicit(&condition->next_ticket, 1, memory_order_release);

     /*
      * wake_ticket represents the next unwoken ticket.
      *
      * For ticket N to have been released, wake_ticket must have
      * reached at least N + 1 in modular ticket order.
      */
     target = ticket + 1;

     fifo_mutex_unlock(mutex);

     for (;;) {
         unsigned int wake;
         wake = atomic_load_explicit(&condition->wake_ticket, memory_order_acquire);
         if (!fifo_ticket_before(wake, target)) break;
         fifo_platform_yield();
     }
     fifo_mutex_lock(mutex);
}

void fifo_condition_signal(fifo_condition_t *condition) {
     unsigned int wake;
     unsigned int next;

     for (;;) {
         wake = atomic_load_explicit(&condition->wake_ticket, memory_order_relaxed);

         next = atomic_load_explicit(&condition->next_ticket, memory_order_acquire);

         /*
          * No registered waiters.
          *
          * Condition signals aren't remembered for future waiters.
          */
         if (wake == next) return;

         if (atomic_compare_exchange_weak_explicit(
                &condition->wake_ticket,
                &wake,
                wake + 1,
                memory_order_release,
                memory_order_relaxed))
            return;
     }
}

void fifo_condition_broadcast(fifo_condition_t *condition) {
     unsigned int target;
     unsigned int wake;

     /*
      * Broadcast covers every waiter which has registered by this
      * point. Waiters registering later belong to a later generation.
      */
     target = atomic_load_explicit(&condition->next_ticket, memory_order_acquire);

     for (;;) {
         wake = atomic_load_explicit(&condition->wake_ticket, memory_order_relaxed);

         /*
          * We've either reached our snapshot already, or another
          * signal/broadcast has advanced beyond it.
          *
          * Never move wake_ticket backwards.
          */
         if (!fifo_ticket_before(wake, target)) return;

         if (atomic_compare_exchange_weak_explicit(
                &condition->wake_ticket,
                &wake,
                target,
                memory_order_release,
                memory_order_relaxed))
            return;
     }
}
