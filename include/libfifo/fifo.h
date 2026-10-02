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

#include <libfifo/sync.h>

typedef struct fifo {
    void **items;

    size_t capacity;
    size_t head;
    size_t tail;
    size_t count;

    fifo_mutex_t lock;
    fifo_condition_t readable;
    fifo_condition_t writable;
} fifo_t;

/*
 * Initialise a FIFO using caller-provided storage.
 *
 * storage must contain space for at least capacity void pointers.
 * libfifo does not allocate or free either the FIFO or its storage.
 */
void fifo_init(fifo_t *fifo,
               void **storage,
               size_t capacity);

/*
 * Replace a FIFO's caller-provided backing storage and capacity.
 *
 * new_storage must provide space for new_capacity void pointers and must be
 * distinct from the FIFO's current storage. The caller retains ownership of
 * both storage arrays; libfifo never allocates or frees them. This operation
 * preserves all queued items in FIFO order and fails if new_capacity is zero
 * or smaller than the current item count. On success, the FIFO uses only
 * new_storage after this function returns, so the caller may then free the old
 * storage. On failure, the FIFO and its original storage remain unchanged.
 *
 * The operation uses the FIFO's internal lock and is safe alongside the other
 * FIFO operations, including the blocking push and pop operations. FIFOs do
 * not otherwise grow automatically.
 */
bool fifo_resize(fifo_t *fifo,
                 void **new_storage,
                 size_t new_capacity);

size_t fifo_capacity(const fifo_t *fifo);
size_t fifo_count(fifo_t *fifo);

bool fifo_empty(fifo_t *fifo);
bool fifo_full(fifo_t *fifo);


/*
 * Non-blocking operations.
 *
 * fifo_push() returns false if the FIFO is full.
 * fifo_pop() and fifo_peek() return false if the FIFO is empty.
 */
bool fifo_push(fifo_t *fifo, void *item);
bool fifo_pop(fifo_t *fifo, void **item);
bool fifo_peek(fifo_t *fifo, void **item);


/*
 * Blocking operations.
 *
 * fifo_push_wait() waits until space is available.
 * fifo_pop_wait() waits until an item is available.
 */
void fifo_push_wait(fifo_t *fifo, void *item);
void *fifo_pop_wait(fifo_t *fifo);
