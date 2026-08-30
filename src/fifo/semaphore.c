/*
 * libfifo - FIFO and synchronisation primitives
 *
 * Copyright (C) 2026 Gwen Nelson
 *
 * GPLv2
 */

#include <libfifo/sync.h>

void fifo_semaphore_init(fifo_semaphore_t *semaphore,
                         size_t initial_count)
{
    (void)semaphore;
    (void)initial_count;
}

void fifo_semaphore_wait(fifo_semaphore_t *semaphore)
{
    (void)semaphore;
}

bool fifo_semaphore_trywait(fifo_semaphore_t *semaphore)
{
    (void)semaphore;
    return false;
}

void fifo_semaphore_post(fifo_semaphore_t *semaphore)
{
    (void)semaphore;
}
