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

#include <libfifo/fifo.h>

void fifo_init(fifo_t *fifo, void **storage, size_t capacity) {
     fifo->items = storage;
     fifo->capacity = capacity;
     fifo->head = 0;
     fifo->tail = 0;
     fifo->count = 0;

     fifo_mutex_init(&fifo->lock);
     fifo_condition_init(&fifo->readable);
     fifo_condition_init(&fifo->writable);
}

size_t fifo_capacity(const fifo_t *fifo) {
       return fifo->capacity;
}

size_t fifo_count(fifo_t *fifo) {
       size_t count;

       fifo_mutex_lock(&fifo->lock);
       count = fifo->count;
       fifo_mutex_unlock(&fifo->lock);

       return count;
}


bool fifo_empty(fifo_t *fifo) {
     bool empty;

     fifo_mutex_lock(&fifo->lock);
     empty = fifo->count == 0;
     fifo_mutex_unlock(&fifo->lock);

     return empty;
}


bool fifo_full(fifo_t *fifo) {
     bool full;

     fifo_mutex_lock(&fifo->lock);
     full = fifo->count == fifo->capacity;
     fifo_mutex_unlock(&fifo->lock);

     return full;
}


bool fifo_push(fifo_t *fifo, void *item) {
     fifo_mutex_lock(&fifo->lock);

     if (fifo->count == fifo->capacity) {
         fifo_mutex_unlock(&fifo->lock);
         return false;
     }

     fifo->items[fifo->tail] = item;
     fifo->tail = (fifo->tail + 1) % fifo->capacity;
     fifo->count++;

     fifo_condition_signal(&fifo->readable);
     fifo_mutex_unlock(&fifo->lock);

     return true;
}


bool fifo_pop(fifo_t *fifo, void **item) {
     fifo_mutex_lock(&fifo->lock);

     if (fifo->count == 0) {
         fifo_mutex_unlock(&fifo->lock);
         return false;
     }

     *item = fifo->items[fifo->head];
     fifo->head = (fifo->head + 1) % fifo->capacity;
     fifo->count--;

     fifo_condition_signal(&fifo->writable);
     fifo_mutex_unlock(&fifo->lock);

     return true;
}


bool fifo_peek(fifo_t *fifo, void **item) {
     fifo_mutex_lock(&fifo->lock);

     if (fifo->count == 0) {
         fifo_mutex_unlock(&fifo->lock);
         return false;
     }

     *item = fifo->items[fifo->head];

     fifo_mutex_unlock(&fifo->lock);

     return true;
}


void fifo_push_wait(fifo_t *fifo, void *item) {
     fifo_mutex_lock(&fifo->lock);

     while (fifo->count == fifo->capacity) {
       fifo_condition_wait(&fifo->writable, &fifo->lock);
     }

     fifo->items[fifo->tail] = item;
     fifo->tail = (fifo->tail + 1) % fifo->capacity;
     fifo->count++;

     fifo_condition_signal(&fifo->readable);
     fifo_mutex_unlock(&fifo->lock);
}


void *fifo_pop_wait(fifo_t *fifo) {
     void *item;

     fifo_mutex_lock(&fifo->lock);

     while (fifo->count == 0) {
       fifo_condition_wait(&fifo->readable, &fifo->lock);
     }

     item = fifo->items[fifo->head];
     fifo->head = (fifo->head + 1) % fifo->capacity;
     fifo->count--;

     fifo_condition_signal(&fifo->writable);
     fifo_mutex_unlock(&fifo->lock);

     return item;
}
