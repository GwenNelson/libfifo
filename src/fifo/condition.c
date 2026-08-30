/*
 * libfifo - FIFO and synchronisation primitives
 *
 * Copyright (C) 2026 Gwen Nelson
 *
 * GPLv2
 */

#include <libfifo/sync.h>

void fifo_condition_init(fifo_condition_t *condition)
{
    (void)condition;
}

void fifo_condition_wait(fifo_condition_t *condition,
                         fifo_mutex_t *mutex)
{
    (void)condition;
    (void)mutex;
}

void fifo_condition_signal(fifo_condition_t *condition)
{
    (void)condition;
}

void fifo_condition_broadcast(fifo_condition_t *condition)
{
    (void)condition;
}
