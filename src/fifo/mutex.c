/*
 * libfifo - FIFO and synchronisation primitives
 *
 * Copyright (C) 2026 Gwen Nelson
 *
 * GPLv2
 */

#include <libfifo/sync.h>

void fifo_mutex_init(fifo_mutex_t *mutex)
{
    (void)mutex;
}

void fifo_mutex_lock(fifo_mutex_t *mutex)
{
    (void)mutex;
}

bool fifo_mutex_trylock(fifo_mutex_t *mutex)
{
    (void)mutex;
    return false;
}

void fifo_mutex_unlock(fifo_mutex_t *mutex)
{
    (void)mutex;
}
