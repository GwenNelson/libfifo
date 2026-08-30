/*
 * libfifo - FIFO and synchronisation primitives
 *
 * Copyright (C) 2026 Gwen Nelson
 *
 * GPLv2
 */

#include <libfifo/fifo.h>

void fifo_init(fifo_t *fifo,
               void **storage,
               size_t capacity)
{
    (void)fifo;
    (void)storage;
    (void)capacity;
}

size_t fifo_capacity(const fifo_t *fifo)
{
    (void)fifo;
    return 0;
}

size_t fifo_count(fifo_t *fifo)
{
    (void)fifo;
    return 0;
}

bool fifo_empty(fifo_t *fifo)
{
    (void)fifo;
    return false;
}

bool fifo_full(fifo_t *fifo)
{
    (void)fifo;
    return false;
}

bool fifo_push(fifo_t *fifo, void *item)
{
    (void)fifo;
    (void)item;
    return false;
}

bool fifo_pop(fifo_t *fifo, void **item)
{
    (void)fifo;
    (void)item;
    return false;
}

bool fifo_peek(fifo_t *fifo, void **item)
{
    (void)fifo;
    (void)item;
    return false;
}

void fifo_push_wait(fifo_t *fifo, void *item)
{
    (void)fifo;
    (void)item;
}

void *fifo_pop_wait(fifo_t *fifo)
{
    (void)fifo;
    return NULL;
}
