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

void fifo_spinlock_init(fifo_spinlock_t *lock) {
     atomic_flag_clear(&lock->locked);
}

void fifo_spinlock_lock(fifo_spinlock_t *lock)
{
    (void)lock;
}

bool fifo_spinlock_trylock(fifo_spinlock_t *lock) {
     return !atomic_flag_test_and_set(&lock->locked);
}

void fifo_spinlock_unlock(fifo_spinlock_t *lock)
{
    (void)lock;
}
