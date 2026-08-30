/*
 * libfifo - FIFO and synchronisation primitives
 *
 * Copyright (C) 2026 Gwen Nelson
 *
 * GPLv2
 */

#include <libfifo/sync.h>

void fifo_spinlock_init(fifo_spinlock_t *lock)
{
    (void)lock;
}

void fifo_spinlock_lock(fifo_spinlock_t *lock)
{
    (void)lock;
}

bool fifo_spinlock_trylock(fifo_spinlock_t *lock)
{
    (void)lock;
    return false;
}

void fifo_spinlock_unlock(fifo_spinlock_t *lock)
{
    (void)lock;
}
