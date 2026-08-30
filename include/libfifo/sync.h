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

#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdatomic.h>


/*
 * Platform-specific yield callback stuff
 */

typedef void (*fifo_yield_fn)(void); // platform-specific yield callback, this function should yield or sleep depending on what makes sense
void fifo_set_yield_ballback(fifo_yield_fn yield_cb);

void fifo_platform_yield(void); // this is used by the rest of the library to either run the callback or immediately return if not set

/*
 * Spinlocks
 */

typedef struct fifo_spinlock {
    atomic_flag locked;
} fifo_spinlock_t;

void fifo_spinlock_init(fifo_spinlock_t *lock);

void fifo_spinlock_lock(fifo_spinlock_t *lock);
bool fifo_spinlock_trylock(fifo_spinlock_t *lock);
void fifo_spinlock_unlock(fifo_spinlock_t *lock);


/*
 * Mutexes
 */
typedef struct fifo_mutex {
    /* Platform-independent state will go here. */
    atomic_uint state;
} fifo_mutex_t;

void fifo_mutex_init(fifo_mutex_t *mutex);

void fifo_mutex_lock(fifo_mutex_t *mutex);
bool fifo_mutex_trylock(fifo_mutex_t *mutex);
void fifo_mutex_unlock(fifo_mutex_t *mutex);


/*
 * Semaphores
 */

typedef struct fifo_semaphore {
    atomic_size_t count;
} fifo_semaphore_t;

void fifo_semaphore_init(fifo_semaphore_t *semaphore,
                         size_t initial_count);

void fifo_semaphore_wait(fifo_semaphore_t *semaphore);
bool fifo_semaphore_trywait(fifo_semaphore_t *semaphore);
void fifo_semaphore_post(fifo_semaphore_t *semaphore);


/*
 * Condition variables
 */

typedef struct fifo_condition {
    atomic_uint sequence;
} fifo_condition_t;

void fifo_condition_init(fifo_condition_t *condition);

void fifo_condition_wait(fifo_condition_t *condition,
                         fifo_mutex_t *mutex);

void fifo_condition_signal(fifo_condition_t *condition);
void fifo_condition_broadcast(fifo_condition_t *condition);
