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
#include <libfifo/sync.h>

#include <stdint.h>
#include <stdio.h>


static const char *failed_assert = NULL;


#ifdef TEST_GDB

#define TEST_FAILURE_TRAP()                                               \
    do {                                                                  \
        volatile int test_failure_spin = 1;                               \
        while (test_failure_spin)                                         \
            ;                                                             \
    } while (0);

#else

#define TEST_FAILURE_TRAP()                                               \
    do {                                                                  \
    } while (0);

#endif


#define ASSERT(desc, cond)                                                \
    do {                                                                  \
        if (!(cond)) {                                                    \
            failed_assert = (desc);                                       \
            TEST_FAILURE_TRAP();                                          \
            return 1;                                                     \
        }                                                                 \
    } while (0);


#define TEST(desc, f)                                                     \
    do {                                                                  \
        int result;                                                       \
                                                                          \
        failed_assert = NULL;                                             \
        fprintf(stdout, "Testing: %-58s", desc);                          \
        result = (f)();                                                   \
                                                                          \
        if (result == 0) {                                                \
            passed_tests++;                                               \
            fprintf(stdout, "PASS\n");                                    \
        } else {                                                          \
            failed_tests++;                                               \
            fprintf(stdout, "FAIL");                                      \
            if (failed_assert != NULL)                                    \
                fprintf(stdout, "  -  %s", failed_assert);                \
            fprintf(stdout, "\n");                                        \
        }                                                                 \
                                                                          \
        total_tests++;                                                    \
    } while (0);


/*
 * Spinlock tests
 */

static int test_spinlock_init_unlocked(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("new spinlock must be lockable",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_spinlock_trylock_locked(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("first trylock must succeed",
           fifo_spinlock_trylock(&lock));

    ASSERT("second trylock must fail",
           !fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_spinlock_unlock(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("initial trylock must succeed",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    ASSERT("spinlock must be lockable after unlock",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_spinlock_lock(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("new spinlock must initially be lockable",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    fifo_spinlock_lock(&lock);

    ASSERT("trylock must fail while lock is held",
           !fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    ASSERT("spinlock must be lockable after lock/unlock",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_spinlock_repeated(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    for (size_t i = 0; i < 10000; i++) {
        ASSERT("repeated trylock must succeed",
               fifo_spinlock_trylock(&lock));

        fifo_spinlock_unlock(&lock);
    }

    return 0;
}


/*
 * Mutex tests
 */

static int test_mutex_init_unlocked(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("new mutex must be lockable",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_mutex_trylock_locked(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("first trylock must succeed",
           fifo_mutex_trylock(&mutex));

    ASSERT("second trylock must fail",
           !fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_mutex_unlock(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("initial trylock must succeed",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    ASSERT("mutex must be lockable after unlock",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_mutex_lock(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("new mutex must initially be lockable",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    fifo_mutex_lock(&mutex);

    ASSERT("trylock must fail while mutex is held",
           !fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    ASSERT("mutex must be lockable after lock/unlock",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_mutex_repeated(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    for (size_t i = 0; i < 10000; i++) {
        ASSERT("repeated mutex trylock must succeed",
               fifo_mutex_trylock(&mutex));

        fifo_mutex_unlock(&mutex);
    }

    return 0;
}


/*
 * Semaphore tests
 */

static int test_semaphore_zero(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    ASSERT("zero semaphore must reject trywait",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("posted zero semaphore must accept trywait",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("consumed semaphore must reject trywait",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_semaphore_one(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 1);

    ASSERT("semaphore with count one must accept trywait",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("count-one semaphore must then be empty",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_semaphore_consumes(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 2);

    ASSERT("first trywait must succeed",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("second trywait must succeed",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("third trywait must fail",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_semaphore_initial_count(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 8);

    for (size_t i = 0; i < 8; i++) {
        ASSERT("trywait within initial count must succeed",
               fifo_semaphore_trywait(&semaphore));
    }

    ASSERT("trywait beyond initial count must fail",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_semaphore_post_zero(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    fifo_semaphore_post(&semaphore);

    ASSERT("post must make zero semaphore available",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("posted count must be consumed",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_semaphore_post_consumed(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 1);

    ASSERT("initial trywait must succeed",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("semaphore must be empty after trywait",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("post must restore consumed semaphore",
           fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_semaphore_multiple_posts(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    for (size_t i = 0; i < 16; i++)
        fifo_semaphore_post(&semaphore);

    for (size_t i = 0; i < 16; i++) {
        ASSERT("posted semaphore count must be consumable",
               fifo_semaphore_trywait(&semaphore));
    }

    ASSERT("semaphore must be empty after consuming posts",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


/*
 * Condition variable tests
 *
 * There is no useful externally observable synchronous state for a
 * condition variable yet. Keep the initialisation test, but require
 * functioning synchronisation machinery around it so a completely
 * stubbed libfifo cannot accidentally report success.
 */

static int test_condition_init(void)
{
    fifo_condition_t condition;
    fifo_mutex_t mutex;

    fifo_condition_init(&condition);
    fifo_mutex_init(&mutex);

    ASSERT("mutex associated with condition machinery must work",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    ASSERT("mutex must remain usable after condition initialisation",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


/*
 * FIFO initialisation and state tests
 */

static int test_fifo_capacity(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("capacity must equal supplied capacity",
           fifo_capacity(&fifo) == 4);

    ASSERT("different capacity must not be reported",
           fifo_capacity(&fifo) != 1);

    return 0;
}


static int test_fifo_initial_count(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("new FIFO must accept a push",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("count must become one after push",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_initial_empty(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("new FIFO must be empty",
           fifo_empty(&fifo));

    ASSERT("new FIFO must accept a push",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("FIFO must cease being empty after push",
           !fifo_empty(&fifo));

    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    return 0;
}


static int test_fifo_initial_not_full(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("new FIFO must not be full",
           !fifo_full(&fifo));

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("setup push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("FIFO must eventually become full",
           fifo_full(&fifo));

    return 0;
}


static int test_fifo_capacity_one(void)
{
    fifo_t fifo;
    void *storage[1];

    fifo_init(&fifo, storage, 1);

    ASSERT("capacity one must be preserved",
           fifo_capacity(&fifo) == 1);

    ASSERT("capacity-one FIFO must accept one item",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("capacity-one FIFO must become full",
           fifo_full(&fifo));

    return 0;
}


/*
 * FIFO push tests
 */

static int test_fifo_push(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("push into empty FIFO must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("successful push must produce one item",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_push_increments_count(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("push must increment count",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_push_makes_nonempty(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("FIFO must not be empty after push",
           !fifo_empty(&fifo));

    return 0;
}


static int test_fifo_push_to_capacity(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("push up to capacity must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("count must equal capacity",
           fifo_count(&fifo) == 4);

    ASSERT("FIFO must report full",
           fifo_full(&fifo));

    return 0;
}


static int test_fifo_full_at_capacity(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("push up to capacity must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("FIFO must report full at capacity",
           fifo_full(&fifo));

    ASSERT("overflow push must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)5));

    return 0;
}


static int test_fifo_push_when_full(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("setup push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("FIFO must actually be full before overflow test",
           fifo_full(&fifo));

    ASSERT("push beyond capacity must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)5));

    return 0;
}


static int test_fifo_failed_push_preserves_count(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("setup push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("FIFO must contain four items",
           fifo_count(&fifo) == 4);

    ASSERT("overflow push must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)5));

    ASSERT("failed push must preserve count",
           fifo_count(&fifo) == 4);

    return 0;
}


static int test_fifo_push_null(void)
{
    fifo_t fifo;
    void *storage[2];

    fifo_init(&fifo, storage, 2);

    ASSERT("NULL must be accepted as an item",
           fifo_push(&fifo, NULL));

    ASSERT("NULL item must increment count",
           fifo_count(&fifo) == 1);

    ASSERT("FIFO containing NULL must not be empty",
           !fifo_empty(&fifo));

    return 0;
}


/*
 * FIFO peek tests
 */

static int test_fifo_peek_empty(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("setup item must be correct",
           (uintptr_t)item == 1);

    ASSERT("FIFO must now be empty",
           fifo_empty(&fifo));

    ASSERT("peek on empty FIFO must fail",
           !fifo_peek(&fifo, &item));

    return 0;
}


static int test_fifo_peek(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item = NULL;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)123));

    ASSERT("peek must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek must return first item",
           (uintptr_t)item == 123);

    return 0;
}


static int test_fifo_peek_preserves_count(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("count must be one before peek",
           fifo_count(&fifo) == 1);

    ASSERT("peek must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek must not change count",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_repeated_peek(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)99));

    for (size_t i = 0; i < 100; i++) {
        ASSERT("repeated peek must succeed",
               fifo_peek(&fifo, &item));

        ASSERT("repeated peek must return same item",
               (uintptr_t)item == 99);
    }

    ASSERT("repeated peek must not consume item",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_peek_null_item(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item = (void *)(uintptr_t)1;

    fifo_init(&fifo, storage, 2);

    ASSERT("NULL push must succeed",
           fifo_push(&fifo, NULL));

    ASSERT("FIFO must contain the NULL item",
           fifo_count(&fifo) == 1);

    ASSERT("peek of NULL item must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek must return NULL item",
           item == NULL);

    return 0;
}


/*
 * FIFO pop tests
 */

static int test_fifo_pop_empty(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("setup pop must return correct item",
           (uintptr_t)item == 1);

    ASSERT("second pop on empty FIFO must fail",
           !fifo_pop(&fifo, &item));

    return 0;
}


static int test_fifo_pop(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item = NULL;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)123));

    ASSERT("pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop must return pushed item",
           (uintptr_t)item == 123);

    return 0;
}


static int test_fifo_pop_decrements_count(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("count must be two before pop",
           fifo_count(&fifo) == 2);

    ASSERT("pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop must decrement count",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_pop_last_makes_empty(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("FIFO must not be empty after setup push",
           !fifo_empty(&fifo));

    ASSERT("pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("FIFO must be empty after last pop",
           fifo_empty(&fifo));

    return 0;
}


static int test_fifo_pop_from_full_clears_full(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item;

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("setup FIFO must be full",
           fifo_full(&fifo));

    ASSERT("pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("FIFO must cease being full after pop",
           !fifo_full(&fifo));

    return 0;
}


static int test_fifo_pop_null_item(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item = (void *)(uintptr_t)1;

    fifo_init(&fifo, storage, 2);

    ASSERT("NULL push must succeed",
           fifo_push(&fifo, NULL));

    ASSERT("FIFO must contain one item",
           fifo_count(&fifo) == 1);

    ASSERT("NULL pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("popped item must be NULL",
           item == NULL);

    return 0;
}


/*
 * FIFO ordering tests
 */

static int test_fifo_two_item_order(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item;

    fifo_init(&fifo, storage, 2);

    ASSERT("push first must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("push second must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("first pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("first pop must return first item",
           (uintptr_t)item == 1);

    ASSERT("second pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("second pop must return second item",
           (uintptr_t)item == 2);

    return 0;
}


static int test_fifo_many_item_order(void)
{
    fifo_t fifo;
    void *storage[32];
    void *item;

    fifo_init(&fifo, storage, 32);

    for (uintptr_t i = 1; i <= 32; i++) {
        ASSERT("ordered push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("FIFO must contain all ordered items",
           fifo_count(&fifo) == 32);

    for (uintptr_t i = 1; i <= 32; i++) {
        ASSERT("ordered pop must succeed",
               fifo_pop(&fifo, &item));

        ASSERT("FIFO ordering must be preserved",
               (uintptr_t)item == i);
    }

    return 0;
}


/*
 * FIFO capacity-one edge cases
 */

static int test_fifo_one_push_full(void)
{
    fifo_t fifo;
    void *storage[1];

    fifo_init(&fifo, storage, 1);

    ASSERT("capacity-one push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("capacity-one FIFO must become full",
           fifo_full(&fifo));

    ASSERT("capacity-one FIFO must contain one item",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_one_second_push_fails(void)
{
    fifo_t fifo;
    void *storage[1];

    fifo_init(&fifo, storage, 1);

    ASSERT("first push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("FIFO must be full after first push",
           fifo_full(&fifo));

    ASSERT("second push must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("failed second push must preserve count",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_fifo_one_reuse(void)
{
    fifo_t fifo;
    void *storage[1];
    void *item;

    fifo_init(&fifo, storage, 1);

    for (uintptr_t i = 1; i <= 1000; i++) {
        ASSERT("capacity-one push must succeed",
               fifo_push(&fifo, (void *)i));

        ASSERT("capacity-one FIFO must become full",
               fifo_full(&fifo));

        ASSERT("capacity-one pop must succeed",
               fifo_pop(&fifo, &item));

        ASSERT("capacity-one item must survive",
               (uintptr_t)item == i);

        ASSERT("capacity-one FIFO must become empty",
               fifo_empty(&fifo));
    }

    return 0;
}


/*
 * FIFO wraparound tests
 */

static int test_fifo_single_wraparound(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("initial push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("FIFO must be full before wraparound",
           fifo_full(&fifo));

    ASSERT("pop before wrap one must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("first pre-wrap item must be correct",
           (uintptr_t)item == 1);

    ASSERT("pop before wrap two must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("second pre-wrap item must be correct",
           (uintptr_t)item == 2);

    ASSERT("first wrapped push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)5));

    ASSERT("second wrapped push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)6));

    for (uintptr_t i = 3; i <= 6; i++) {
        ASSERT("wrapped pop must succeed",
               fifo_pop(&fifo, &item));

        ASSERT("wrapped FIFO ordering must survive",
               (uintptr_t)item == i);
    }

    return 0;
}


static int test_fifo_repeated_wraparound(void)
{
    fifo_t fifo;
    void *storage[7];
    void *item;

    fifo_init(&fifo, storage, 7);

    for (uintptr_t round = 0; round < 10000; round++) {
        for (uintptr_t i = 0; i < 7; i++) {
            uintptr_t value = round * 7 + i + 1;

            ASSERT("stress push must succeed",
                   fifo_push(&fifo, (void *)value));
        }

        ASSERT("FIFO must be full during stress round",
               fifo_full(&fifo));

        for (uintptr_t i = 0; i < 7; i++) {
            uintptr_t expected = round * 7 + i + 1;

            ASSERT("stress pop must succeed",
                   fifo_pop(&fifo, &item));

            ASSERT("stress ordering must survive",
                   (uintptr_t)item == expected);
        }

        ASSERT("FIFO must be empty after stress round",
               fifo_empty(&fifo));
    }

    return 0;
}


static int test_fifo_interleaved_wraparound(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    for (uintptr_t i = 1; i <= 10000; i++) {
        ASSERT("interleaved push must succeed",
               fifo_push(&fifo, (void *)i));

        ASSERT("FIFO must contain pushed item",
               fifo_count(&fifo) == 1);

        ASSERT("interleaved pop must succeed",
               fifo_pop(&fifo, &item));

        ASSERT("interleaved item must survive",
               (uintptr_t)item == i);

        ASSERT("FIFO must return to empty state",
               fifo_empty(&fifo));
    }

    return 0;
}


/*
 * FIFO state transition tests
 */

static int test_fifo_empty_full_empty(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("FIFO must begin empty",
           fifo_empty(&fifo));

    ASSERT("FIFO must accept items",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("FIFO must leave empty state",
           !fifo_empty(&fifo));

    ASSERT("second push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("third push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)3));

    ASSERT("fourth push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)4));

    ASSERT("FIFO must become full",
           fifo_full(&fifo));

    for (size_t i = 0; i < 4; i++) {
        ASSERT("drain pop must succeed",
               fifo_pop(&fifo, &item));
    }

    ASSERT("FIFO must become empty again",
           fifo_empty(&fifo));

    ASSERT("FIFO must no longer be full",
           !fifo_full(&fifo));

    return 0;
}


static int test_fifo_count_transitions(void)
{
    fifo_t fifo;
    void *storage[8];
    void *item;

    fifo_init(&fifo, storage, 8);

    for (size_t i = 0; i < 8; i++) {
        ASSERT("count before push must be correct",
               fifo_count(&fifo) == i);

        ASSERT("push must succeed",
               fifo_push(&fifo, (void *)(uintptr_t)(i + 1)));

        ASSERT("count after push must be correct",
               fifo_count(&fifo) == i + 1);
    }

    for (size_t i = 8; i > 0; i--) {
        ASSERT("count before pop must be correct",
               fifo_count(&fifo) == i);

        ASSERT("pop must succeed",
               fifo_pop(&fifo, &item));

        ASSERT("count after pop must be correct",
               fifo_count(&fifo) == i - 1);
    }

    return 0;
}


static int test_fifo_failed_pop_preserves_state(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)123));

    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("setup pop must return correct value",
           (uintptr_t)item == 123);

    ASSERT("FIFO must now be empty",
           fifo_empty(&fifo));

    ASSERT("failed empty pop must actually fail",
           !fifo_pop(&fifo, &item));

    ASSERT("failed pop must preserve zero count",
           fifo_count(&fifo) == 0);

    ASSERT("failed pop must preserve empty state",
           fifo_empty(&fifo));

    return 0;
}


/*
 * FIFO reuse tests
 */

static int test_fifo_reinitialise(void)
{
    fifo_t fifo;
    void *storage_a[4];
    void *storage_b[8];
    void *item;

    fifo_init(&fifo, storage_a, 4);

    ASSERT("initial capacity must be four",
           fifo_capacity(&fifo) == 4);

    ASSERT("initial push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)123));

    ASSERT("initial FIFO must contain item",
           fifo_count(&fifo) == 1);

    fifo_init(&fifo, storage_b, 8);

    ASSERT("reinitialised capacity must change",
           fifo_capacity(&fifo) == 8);

    ASSERT("reinitialised FIFO must have zero count",
           fifo_count(&fifo) == 0);

    ASSERT("reinitialised FIFO must be empty",
           fifo_empty(&fifo));

    ASSERT("reinitialised FIFO must accept new item",
           fifo_push(&fifo, (void *)(uintptr_t)456));

    ASSERT("reinitialised FIFO pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("reinitialised FIFO must use new data",
           (uintptr_t)item == 456);

    return 0;
}


static int test_fifo_reuse_after_drain(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    for (uintptr_t round = 0; round < 1000; round++) {
        for (uintptr_t i = 0; i < 4; i++) {
            ASSERT("reuse push must succeed",
                   fifo_push(&fifo,
                             (void *)(uintptr_t)(round * 4 + i + 1)));
        }

        ASSERT("FIFO must be full after fill",
               fifo_full(&fifo));

        for (uintptr_t i = 0; i < 4; i++) {
            uintptr_t expected = round * 4 + i + 1;

            ASSERT("reuse pop must succeed",
                   fifo_pop(&fifo, &item));

            ASSERT("reuse ordering must survive",
                   (uintptr_t)item == expected);
        }

        ASSERT("FIFO must be empty after drain",
               fifo_empty(&fifo));
    }

    return 0;
}




/*
 * ======================================================================
 * Individual function exhaustive tests
 * ======================================================================
 *
 * These tests deliberately exercise one public operation across every
 * meaningful synchronous state we can construct without invoking undefined
 * behaviour or requiring another thread to make progress.
 *
 * Blocking operations are tested only when they must complete immediately.
 * Misuse cases such as unlocking an unlocked lock, recursive locking, and
 * zero-capacity FIFOs are intentionally not specified by this test suite.
 */


/*
 * fifo_spinlock_init()
 */

static int test_individual_spinlock_init_lockable(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("fifo_spinlock_init must produce an unlocked lock",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_individual_spinlock_init_independent(void)
{
    fifo_spinlock_t a;
    fifo_spinlock_t b;

    fifo_spinlock_init(&a);
    fifo_spinlock_init(&b);

    ASSERT("first independently initialised spinlock must lock",
           fifo_spinlock_trylock(&a));

    ASSERT("locking first spinlock must not affect second",
           fifo_spinlock_trylock(&b));

    fifo_spinlock_unlock(&b);
    fifo_spinlock_unlock(&a);

    return 0;
}


/*
 * fifo_spinlock_trylock()
 */

static int test_individual_spinlock_trylock_unlocked(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("trylock on unlocked spinlock must succeed",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_individual_spinlock_trylock_held(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("setup trylock must acquire spinlock",
           fifo_spinlock_trylock(&lock));

    ASSERT("trylock on held spinlock must fail",
           !fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_individual_spinlock_trylock_after_unlock(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("first trylock must succeed",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    ASSERT("trylock after unlock must succeed",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


/*
 * fifo_spinlock_lock()
 */

static int test_individual_spinlock_lock_unlocked(void)
{
    fifo_spinlock_t lock;
    bool was_locked;

    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    fifo_spinlock_lock(&lock);

    was_locked = atomic_flag_test_and_set_explicit(&lock.locked,
                                                    memory_order_relaxed);

    ASSERT("spinlock_lock must set the lock state",
           was_locked);

    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    return 0;
}


static int test_individual_spinlock_lock_reusable(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    for (size_t i = 0; i < 1000; i++) {
        fifo_spinlock_lock(&lock);

        ASSERT("locked spinlock must reject trylock",
               !fifo_spinlock_trylock(&lock));

        fifo_spinlock_unlock(&lock);
    }

    ASSERT("spinlock must remain usable after repeated lock/unlock",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


/*
 * fifo_spinlock_unlock()
 */

static int test_individual_spinlock_unlock_trylocked(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);

    ASSERT("setup trylock must succeed",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    ASSERT("unlock must release trylocked spinlock",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


static int test_individual_spinlock_unlock_locked(void)
{
    fifo_spinlock_t lock;

    fifo_spinlock_init(&lock);
    fifo_spinlock_lock(&lock);
    fifo_spinlock_unlock(&lock);

    ASSERT("unlock must release lock-acquired spinlock",
           fifo_spinlock_trylock(&lock));

    fifo_spinlock_unlock(&lock);

    return 0;
}


/*
 * fifo_mutex_init()
 */

static int test_individual_mutex_init_lockable(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("fifo_mutex_init must produce an unlocked mutex",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_individual_mutex_init_independent(void)
{
    fifo_mutex_t a;
    fifo_mutex_t b;

    fifo_mutex_init(&a);
    fifo_mutex_init(&b);

    ASSERT("first independently initialised mutex must lock",
           fifo_mutex_trylock(&a));

    ASSERT("locking first mutex must not affect second",
           fifo_mutex_trylock(&b));

    fifo_mutex_unlock(&b);
    fifo_mutex_unlock(&a);

    return 0;
}


/*
 * fifo_mutex_trylock()
 */

static int test_individual_mutex_trylock_unlocked(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("trylock on unlocked mutex must succeed",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_individual_mutex_trylock_held(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("setup trylock must acquire mutex",
           fifo_mutex_trylock(&mutex));

    ASSERT("trylock on held mutex must fail",
           !fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_individual_mutex_trylock_after_unlock(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("first trylock must succeed",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    ASSERT("trylock after unlock must succeed",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


/*
 * fifo_mutex_lock()
 */

static int test_individual_mutex_lock_unlocked(void)
{
    fifo_mutex_t mutex;

    atomic_store_explicit(&mutex.state, 0, memory_order_relaxed);

    fifo_mutex_lock(&mutex);

    ASSERT("mutex_lock must change unlocked state to locked",
           atomic_load_explicit(&mutex.state,
                                memory_order_relaxed) != 0);

    return 0;
}


static int test_individual_mutex_lock_reusable(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    for (size_t i = 0; i < 1000; i++) {
        fifo_mutex_lock(&mutex);

        ASSERT("locked mutex must reject trylock",
               !fifo_mutex_trylock(&mutex));

        fifo_mutex_unlock(&mutex);
    }

    ASSERT("mutex must remain usable after repeated lock/unlock",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


/*
 * fifo_mutex_unlock()
 */

static int test_individual_mutex_unlock_trylocked(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);

    ASSERT("setup trylock must succeed",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    ASSERT("unlock must release trylocked mutex",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


static int test_individual_mutex_unlock_locked(void)
{
    fifo_mutex_t mutex;

    fifo_mutex_init(&mutex);
    fifo_mutex_lock(&mutex);
    fifo_mutex_unlock(&mutex);

    ASSERT("unlock must release lock-acquired mutex",
           fifo_mutex_trylock(&mutex));

    fifo_mutex_unlock(&mutex);

    return 0;
}


/*
 * fifo_semaphore_init()
 */

static int test_individual_semaphore_init_zero(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    ASSERT("zero initial count must reject trywait",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("zero-count semaphore must work after post",
           fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_init_one(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 1);

    ASSERT("count-one semaphore must permit one trywait",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("count-one semaphore must then be exhausted",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_init_many(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 257);

    for (size_t i = 0; i < 257; i++) {
        ASSERT("every unit of initial semaphore count must be available",
               fifo_semaphore_trywait(&semaphore));
    }

    ASSERT("semaphore must exhaust exactly at initial count",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


/*
 * fifo_semaphore_trywait()
 */

static int test_individual_semaphore_trywait_empty(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    ASSERT("trywait on empty semaphore must fail",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("failed trywait must not corrupt later availability",
           fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_trywait_single(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 1);

    ASSERT("trywait with one available unit must succeed",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("successful trywait must consume the only unit",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_trywait_many(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 4);

    ASSERT("first trywait must succeed", fifo_semaphore_trywait(&semaphore));
    ASSERT("second trywait must succeed", fifo_semaphore_trywait(&semaphore));
    ASSERT("third trywait must succeed", fifo_semaphore_trywait(&semaphore));
    ASSERT("fourth trywait must succeed", fifo_semaphore_trywait(&semaphore));
    ASSERT("fifth trywait must fail", !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_trywait_after_post(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    ASSERT("empty setup semaphore must reject trywait",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("trywait must consume a newly posted unit",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("posted unit must be consumed exactly once",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


/*
 * fifo_semaphore_wait()
 */

static int test_individual_semaphore_wait_one(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 1);
    fifo_semaphore_wait(&semaphore);

    ASSERT("wait must consume one available semaphore unit",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("semaphore must remain usable after wait",
           fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_wait_many(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 3);
    fifo_semaphore_wait(&semaphore);

    ASSERT("first remaining unit after wait must be available",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("second remaining unit after wait must be available",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("wait must have consumed exactly one of three units",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_wait_after_post(void)
{
    fifo_semaphore_t semaphore;

    atomic_store_explicit(&semaphore.count, 1, memory_order_relaxed);

    fifo_semaphore_wait(&semaphore);

    ASSERT("semaphore_wait must consume the available unit",
           atomic_load_explicit(&semaphore.count,
                                memory_order_relaxed) == 0);

    return 0;
}


/*
 * fifo_semaphore_post()
 */

static int test_individual_semaphore_post_zero(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);
    fifo_semaphore_post(&semaphore);

    ASSERT("post on zero semaphore must add one unit",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("single post must add exactly one unit",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_post_nonzero(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 2);
    fifo_semaphore_post(&semaphore);

    ASSERT("first of three units must be available",
           fifo_semaphore_trywait(&semaphore));
    ASSERT("second of three units must be available",
           fifo_semaphore_trywait(&semaphore));
    ASSERT("posted third unit must be available",
           fifo_semaphore_trywait(&semaphore));
    ASSERT("three total units must then be exhausted",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_post_accumulates(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 0);

    for (size_t i = 0; i < 100; i++)
        fifo_semaphore_post(&semaphore);

    for (size_t i = 0; i < 100; i++) {
        ASSERT("every posted unit must accumulate",
               fifo_semaphore_trywait(&semaphore));
    }

    ASSERT("all accumulated posts must be consumed exactly once",
           !fifo_semaphore_trywait(&semaphore));

    return 0;
}


static int test_individual_semaphore_post_after_consumption(void)
{
    fifo_semaphore_t semaphore;

    fifo_semaphore_init(&semaphore, 1);

    ASSERT("setup trywait must consume initial unit",
           fifo_semaphore_trywait(&semaphore));

    ASSERT("setup semaphore must now be empty",
           !fifo_semaphore_trywait(&semaphore));

    fifo_semaphore_post(&semaphore);

    ASSERT("post after consumption must restore availability",
           fifo_semaphore_trywait(&semaphore));

    return 0;
}


/*
 * fifo_condition_init(), fifo_condition_signal(),
 * fifo_condition_broadcast()
 *
 * The sequence field is part of the current public structure and is the only
 * synchronous observable state of a condition variable. These tests therefore
 * deliberately verify the sequence-generation behaviour.
 */

static int test_individual_condition_init_sequence(void)
{
    fifo_condition_t condition;

    atomic_store_explicit(&condition.sequence,
                          0x5a5a5a5aU,
                          memory_order_relaxed);

    fifo_condition_init(&condition);

    ASSERT("condition_init must reset a nonzero sequence to zero",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 0);

    return 0;
}


static int test_individual_condition_signal_changes_sequence(void)
{
    fifo_condition_t condition;
    unsigned int before;
    unsigned int after;

    fifo_condition_init(&condition);

    before = atomic_load_explicit(&condition.sequence,
                                  memory_order_relaxed);

    fifo_condition_signal(&condition);

    after = atomic_load_explicit(&condition.sequence,
                                 memory_order_relaxed);

    ASSERT("condition signal must advance sequence",
           after != before);

    return 0;
}


static int test_individual_condition_signal_repeated(void)
{
    fifo_condition_t condition;
    unsigned int previous;

    fifo_condition_init(&condition);

    previous = atomic_load_explicit(&condition.sequence,
                                    memory_order_relaxed);

    for (size_t i = 0; i < 100; i++) {
        unsigned int current;

        fifo_condition_signal(&condition);

        current = atomic_load_explicit(&condition.sequence,
                                       memory_order_relaxed);

        ASSERT("every condition signal must advance sequence",
               current != previous);

        previous = current;
    }

    return 0;
}


static int test_individual_condition_broadcast_changes_sequence(void)
{
    fifo_condition_t condition;
    unsigned int before;
    unsigned int after;

    fifo_condition_init(&condition);

    before = atomic_load_explicit(&condition.sequence,
                                  memory_order_relaxed);

    fifo_condition_broadcast(&condition);

    after = atomic_load_explicit(&condition.sequence,
                                 memory_order_relaxed);

    ASSERT("condition broadcast must advance sequence",
           after != before);

    return 0;
}


static int test_individual_condition_signal_broadcast_sequence(void)
{
    fifo_condition_t condition;
    unsigned int initial;
    unsigned int after_signal;
    unsigned int after_broadcast;

    fifo_condition_init(&condition);

    initial = atomic_load_explicit(&condition.sequence,
                                   memory_order_relaxed);

    fifo_condition_signal(&condition);

    after_signal = atomic_load_explicit(&condition.sequence,
                                        memory_order_relaxed);

    fifo_condition_broadcast(&condition);

    after_broadcast = atomic_load_explicit(&condition.sequence,
                                           memory_order_relaxed);

    ASSERT("signal must advance condition sequence",
           after_signal != initial);

    ASSERT("broadcast must advance sequence again",
           after_broadcast != after_signal);

    return 0;
}


/*
 * fifo_init()
 */

static int test_individual_fifo_init_capacity_one(void)
{
    fifo_t fifo;
    void *storage[1];

    fifo_init(&fifo, storage, 1);

    ASSERT("capacity-one init must preserve capacity",
           fifo_capacity(&fifo) == 1);
    ASSERT("capacity-one init must start empty",
           fifo_empty(&fifo));
    ASSERT("capacity-one init must start with zero count",
           fifo_count(&fifo) == 0);
    ASSERT("capacity-one init must not start full",
           !fifo_full(&fifo));

    ASSERT("capacity-one initialised FIFO must be usable",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_init_typical(void)
{
    fifo_t fifo;
    void *storage[8];

    fifo_init(&fifo, storage, 8);

    ASSERT("typical init must preserve capacity",
           fifo_capacity(&fifo) == 8);
    ASSERT("typical init must start with zero count",
           fifo_count(&fifo) == 0);
    ASSERT("typical init must start empty",
           fifo_empty(&fifo));
    ASSERT("typical init must start not full",
           !fifo_full(&fifo));

    ASSERT("typical initialised FIFO must accept data",
           fifo_push(&fifo, (void *)(uintptr_t)123));

    return 0;
}


static int test_individual_fifo_init_odd_capacity(void)
{
    fifo_t fifo;
    void *storage[17];

    fifo_init(&fifo, storage, 17);

    ASSERT("odd capacity must be preserved exactly",
           fifo_capacity(&fifo) == 17);

    for (uintptr_t i = 1; i <= 17; i++) {
        ASSERT("odd-capacity FIFO must accept every slot",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("odd-capacity FIFO must become full at exact capacity",
           fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_init_reinitialise_nonempty(void)
{
    fifo_t fifo;
    void *storage_a[3];
    void *storage_b[5];

    fifo_init(&fifo, storage_a, 3);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    fifo_init(&fifo, storage_b, 5);

    ASSERT("reinit must replace capacity",
           fifo_capacity(&fifo) == 5);
    ASSERT("reinit must reset count",
           fifo_count(&fifo) == 0);
    ASSERT("reinit must restore empty state",
           fifo_empty(&fifo));
    ASSERT("reinit must clear full state",
           !fifo_full(&fifo));

    ASSERT("reinitialised FIFO must accept new data",
           fifo_push(&fifo, (void *)(uintptr_t)9));

    return 0;
}


/*
 * fifo_capacity()
 */

static int test_individual_fifo_capacity_empty(void)
{
    fifo_t fifo;
    void *storage[6];

    fifo_init(&fifo, storage, 6);

    ASSERT("capacity must be correct while empty",
           fifo_capacity(&fifo) == 6);

    ASSERT("FIFO must also be operational",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_capacity_partial(void)
{
    fifo_t fifo;
    void *storage[6];

    fifo_init(&fifo, storage, 6);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("second setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("capacity must not change while partially full",
           fifo_capacity(&fifo) == 6);

    return 0;
}


static int test_individual_fifo_capacity_full(void)
{
    fifo_t fifo;
    void *storage[6];

    fifo_init(&fifo, storage, 6);

    for (uintptr_t i = 1; i <= 6; i++) {
        ASSERT("setup fill push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("setup FIFO must be full",
           fifo_full(&fifo));

    ASSERT("capacity must not change while full",
           fifo_capacity(&fifo) == 6);

    return 0;
}


static int test_individual_fifo_capacity_wrapped(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("setup fill push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("setup pop one must succeed",
           fifo_pop(&fifo, &item));
    ASSERT("setup pop two must succeed",
           fifo_pop(&fifo, &item));
    ASSERT("wrapped push five must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)5));
    ASSERT("wrapped push six must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)6));

    ASSERT("capacity must remain fixed after wraparound",
           fifo_capacity(&fifo) == 4);

    return 0;
}


/*
 * fifo_count()
 */

static int test_individual_fifo_count_empty(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("empty FIFO count must be zero",
           fifo_count(&fifo) == 0);

    ASSERT("FIFO must be operational after count query",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_count_partial(void)
{
    fifo_t fifo;
    void *storage[5];

    fifo_init(&fifo, storage, 5);

    for (size_t i = 1; i <= 3; i++) {
        ASSERT("partial setup push must succeed",
               fifo_push(&fifo, (void *)(uintptr_t)i));

        ASSERT("count must match each partial fill state",
               fifo_count(&fifo) == i);
    }

    return 0;
}


static int test_individual_fifo_count_full(void)
{
    fifo_t fifo;
    void *storage[5];

    fifo_init(&fifo, storage, 5);

    for (uintptr_t i = 1; i <= 5; i++) {
        ASSERT("full setup push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("full FIFO count must equal capacity",
           fifo_count(&fifo) == 5);

    return 0;
}


static int test_individual_fifo_count_after_pop(void)
{
    fifo_t fifo;
    void *storage[5];
    void *item;

    fifo_init(&fifo, storage, 5);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("setup push must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("count must decrement after pop",
           fifo_count(&fifo) == 3);

    ASSERT("second setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("count must decrement after each pop",
           fifo_count(&fifo) == 2);

    return 0;
}


static int test_individual_fifo_count_failed_push(void)
{
    fifo_t fifo;
    void *storage[2];

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("overflow push must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)3));

    ASSERT("failed push must leave count at capacity",
           fifo_count(&fifo) == 2);

    return 0;
}


static int test_individual_fifo_count_failed_pop(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item;

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("empty pop must fail",
           !fifo_pop(&fifo, &item));

    ASSERT("failed empty pop must preserve zero count",
           fifo_count(&fifo) == 0);

    return 0;
}


static int test_individual_fifo_count_wrapped(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("pop one must succeed", fifo_pop(&fifo, &item));
    ASSERT("pop two must succeed", fifo_pop(&fifo, &item));
    ASSERT("wrapped push four must succeed", fifo_push(&fifo, (void *)(uintptr_t)4));
    ASSERT("wrapped push five must succeed", fifo_push(&fifo, (void *)(uintptr_t)5));

    ASSERT("wrapped FIFO count must reflect live items only",
           fifo_count(&fifo) == 3);

    return 0;
}


/*
 * fifo_empty()
 */

static int test_individual_fifo_empty_initial(void)
{
    fifo_t fifo;
    void *storage[3];

    fifo_init(&fifo, storage, 3);

    ASSERT("new FIFO must report empty",
           fifo_empty(&fifo));

    ASSERT("new FIFO must still accept data",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_empty_partial(void)
{
    fifo_t fifo;
    void *storage[3];

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("partially filled FIFO must not report empty",
           !fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_empty_full(void)
{
    fifo_t fifo;
    void *storage[2];

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("full FIFO must not report empty",
           !fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_empty_after_drain(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("setup pop one must succeed",
           fifo_pop(&fifo, &item));
    ASSERT("FIFO with one remaining item must not be empty",
           !fifo_empty(&fifo));

    ASSERT("setup pop two must succeed",
           fifo_pop(&fifo, &item));
    ASSERT("drained FIFO must report empty",
           fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_empty_null_item(void)
{
    fifo_t fifo;
    void *storage[2];

    fifo_init(&fifo, storage, 2);

    ASSERT("NULL push must succeed",
           fifo_push(&fifo, NULL));

    ASSERT("FIFO containing NULL must not report empty",
           !fifo_empty(&fifo));

    return 0;
}


/*
 * fifo_full()
 */

static int test_individual_fifo_full_initial(void)
{
    fifo_t fifo;
    void *storage[3];

    fifo_init(&fifo, storage, 3);

    ASSERT("new FIFO must not report full",
           !fifo_full(&fifo));

    ASSERT("FIFO must be operational after full query",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_full_partial(void)
{
    fifo_t fifo;
    void *storage[3];

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("partially filled FIFO must not report full",
           !fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_full_exact(void)
{
    fifo_t fifo;
    void *storage[3];

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("setup push three must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)3));

    ASSERT("FIFO at exact capacity must report full",
           fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_full_after_failed_push(void)
{
    fifo_t fifo;
    void *storage[2];

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("overflow push must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)3));

    ASSERT("failed overflow must leave FIFO full",
           fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_full_after_pop(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item;

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("setup FIFO must be full",
           fifo_full(&fifo));
    ASSERT("setup pop must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop from full FIFO must clear full state",
           !fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_full_capacity_one(void)
{
    fifo_t fifo;
    void *storage[1];

    fifo_init(&fifo, storage, 1);

    ASSERT("capacity-one FIFO must initially not be full",
           !fifo_full(&fifo));

    ASSERT("capacity-one push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("capacity-one FIFO must be full after one push",
           fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_full_after_wrap_refill(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("setup pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("FIFO must not be full after pop", !fifo_full(&fifo));
    ASSERT("wrapped refill push must succeed", fifo_push(&fifo, (void *)(uintptr_t)4));

    ASSERT("wrapped refill to capacity must restore full state",
           fifo_full(&fifo));

    return 0;
}


/*
 * fifo_push()
 */

static int test_individual_fifo_push_empty(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("push into empty FIFO must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)11));

    ASSERT("push into empty FIFO must increment count",
           fifo_count(&fifo) == 1);

    ASSERT("pushed value must be retrievable",
           fifo_pop(&fifo, &item));

    ASSERT("retrieved pushed value must match",
           (uintptr_t)item == 11);

    return 0;
}


static int test_individual_fifo_push_partial(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    ASSERT("first push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push into partially full FIFO must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("partial push must update count",
           fifo_count(&fifo) == 2);

    ASSERT("partial FIFO must not be full",
           !fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_push_exact_capacity(void)
{
    fifo_t fifo;
    void *storage[4];

    fifo_init(&fifo, storage, 4);

    for (uintptr_t i = 1; i <= 4; i++) {
        ASSERT("push through exact capacity must succeed",
               fifo_push(&fifo, (void *)i));
    }

    ASSERT("last legal push must make FIFO full",
           fifo_full(&fifo));

    ASSERT("last legal push must make count equal capacity",
           fifo_count(&fifo) == 4);

    return 0;
}


static int test_individual_fifo_push_full_failure(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item;

    fifo_init(&fifo, storage, 2);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("push into full FIFO must fail",
           !fifo_push(&fifo, (void *)(uintptr_t)99));

    ASSERT("failed full push must preserve count",
           fifo_count(&fifo) == 2);

    ASSERT("first original item must survive failed push",
           fifo_pop(&fifo, &item));
    ASSERT("first original item must remain unchanged",
           (uintptr_t)item == 1);

    ASSERT("second original item must survive failed push",
           fifo_pop(&fifo, &item));
    ASSERT("second original item must remain unchanged",
           (uintptr_t)item == 2);

    return 0;
}


static int test_individual_fifo_push_null(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item = (void *)(uintptr_t)123;

    fifo_init(&fifo, storage, 2);

    ASSERT("push must accept NULL as data",
           fifo_push(&fifo, NULL));

    ASSERT("NULL push must count as an item",
           fifo_count(&fifo) == 1);

    ASSERT("NULL item must be poppable",
           fifo_pop(&fifo, &item));

    ASSERT("popped NULL item must remain NULL",
           item == NULL);

    return 0;
}


static int test_individual_fifo_push_after_pop_wrap(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("setup pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("setup pop must return one", (uintptr_t)item == 1);

    ASSERT("push into freed wrapped slot must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)4));

    ASSERT("wrapped push must restore full count",
           fifo_count(&fifo) == 3);

    ASSERT("next item after wrap must be two",
           fifo_pop(&fifo, &item));
    ASSERT("wrapped ordering must preserve two",
           (uintptr_t)item == 2);

    ASSERT("next item after wrap must be three",
           fifo_pop(&fifo, &item));
    ASSERT("wrapped ordering must preserve three",
           (uintptr_t)item == 3);

    ASSERT("wrapped item must come last",
           fifo_pop(&fifo, &item));
    ASSERT("wrapped item must equal four",
           (uintptr_t)item == 4);

    return 0;
}


static int test_individual_fifo_push_capacity_one(void)
{
    fifo_t fifo;
    void *storage[1];

    fifo_init(&fifo, storage, 1);

    ASSERT("capacity-one FIFO must accept first push",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    ASSERT("capacity-one FIFO must reject second push",
           !fifo_push(&fifo, (void *)(uintptr_t)2));

    ASSERT("capacity-one failed push must preserve one item",
           fifo_count(&fifo) == 1);

    return 0;
}


/*
 * fifo_pop()
 */

static int test_individual_fifo_pop_empty_failure(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("pop from empty FIFO must fail",
           !fifo_pop(&fifo, &item));

    ASSERT("failed empty pop must preserve empty state",
           fifo_empty(&fifo));

    ASSERT("failed empty pop must preserve zero count",
           fifo_count(&fifo) == 0);

    ASSERT("FIFO must remain usable after failed pop",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_pop_single(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item = NULL;

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)42));

    ASSERT("pop of sole item must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop of sole item must return value",
           (uintptr_t)item == 42);

    ASSERT("pop of sole item must leave FIFO empty",
           fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_pop_partial(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));

    ASSERT("pop from partial FIFO must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop from partial FIFO must return oldest item",
           (uintptr_t)item == 1);

    ASSERT("partial pop must decrement count",
           fifo_count(&fifo) == 2);

    return 0;
}


static int test_individual_fifo_pop_full(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("setup FIFO must be full", fifo_full(&fifo));

    ASSERT("pop from full FIFO must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop from full FIFO must return oldest item",
           (uintptr_t)item == 1);

    ASSERT("pop from full FIFO must clear full state",
           !fifo_full(&fifo));

    ASSERT("pop from full FIFO must decrement count",
           fifo_count(&fifo) == 2);

    return 0;
}


static int test_individual_fifo_pop_null(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item = (void *)(uintptr_t)99;

    fifo_init(&fifo, storage, 2);

    ASSERT("setup NULL push must succeed",
           fifo_push(&fifo, NULL));

    ASSERT("pop of stored NULL must succeed",
           fifo_pop(&fifo, &item));

    ASSERT("pop must return stored NULL distinctly from failure",
           item == NULL);

    ASSERT("NULL pop must consume the item",
           fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_pop_wrapped(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("pre-wrap pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("pre-wrap pop must return one", (uintptr_t)item == 1);
    ASSERT("wrapped push must succeed", fifo_push(&fifo, (void *)(uintptr_t)4));

    ASSERT("wrapped pop one must succeed", fifo_pop(&fifo, &item));
    ASSERT("wrapped pop one must return two", (uintptr_t)item == 2);
    ASSERT("wrapped pop two must succeed", fifo_pop(&fifo, &item));
    ASSERT("wrapped pop two must return three", (uintptr_t)item == 3);
    ASSERT("wrapped pop three must succeed", fifo_pop(&fifo, &item));
    ASSERT("wrapped pop three must return four", (uintptr_t)item == 4);

    ASSERT("wrapped FIFO must end empty",
           fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_pop_capacity_one_reuse(void)
{
    fifo_t fifo;
    void *storage[1];
    void *item;

    fifo_init(&fifo, storage, 1);

    for (uintptr_t i = 1; i <= 100; i++) {
        ASSERT("capacity-one push must succeed",
               fifo_push(&fifo, (void *)i));

        ASSERT("capacity-one pop must succeed",
               fifo_pop(&fifo, &item));

        ASSERT("capacity-one pop must return current value",
               (uintptr_t)item == i);
    }

    ASSERT("capacity-one FIFO must finish empty",
           fifo_empty(&fifo));

    return 0;
}


/*
 * fifo_peek()
 */

static int test_individual_fifo_peek_empty_failure(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("peek on empty FIFO must fail",
           !fifo_peek(&fifo, &item));

    ASSERT("failed empty peek must preserve empty state",
           fifo_empty(&fifo));

    ASSERT("FIFO must remain usable after failed peek",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    return 0;
}


static int test_individual_fifo_peek_single(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item = NULL;

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)42));

    ASSERT("peek of sole item must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek of sole item must return value",
           (uintptr_t)item == 42);

    ASSERT("peek of sole item must not consume it",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_individual_fifo_peek_partial(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));

    ASSERT("peek of partial FIFO must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek of partial FIFO must return oldest item",
           (uintptr_t)item == 1);

    ASSERT("partial peek must preserve count",
           fifo_count(&fifo) == 3);

    return 0;
}


static int test_individual_fifo_peek_full(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("setup FIFO must be full", fifo_full(&fifo));

    ASSERT("peek of full FIFO must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek of full FIFO must return oldest item",
           (uintptr_t)item == 1);

    ASSERT("peek of full FIFO must preserve full state",
           fifo_full(&fifo));

    return 0;
}


static int test_individual_fifo_peek_null(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item = (void *)(uintptr_t)99;

    fifo_init(&fifo, storage, 2);

    ASSERT("setup NULL push must succeed",
           fifo_push(&fifo, NULL));

    ASSERT("peek of stored NULL must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek must return stored NULL distinctly from failure",
           item == NULL);

    ASSERT("peek of NULL must not consume it",
           fifo_count(&fifo) == 1);

    return 0;
}


static int test_individual_fifo_peek_wrapped(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("pre-wrap pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("pre-wrap pop must return one", (uintptr_t)item == 1);
    ASSERT("wrapped push must succeed", fifo_push(&fifo, (void *)(uintptr_t)4));

    ASSERT("peek after wrap must succeed",
           fifo_peek(&fifo, &item));

    ASSERT("peek after wrap must return logical head",
           (uintptr_t)item == 2);

    ASSERT("peek after wrap must preserve full count",
           fifo_count(&fifo) == 3);

    return 0;
}


static int test_individual_fifo_peek_repeated(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)77));

    for (size_t i = 0; i < 1000; i++) {
        ASSERT("repeated peek must succeed",
               fifo_peek(&fifo, &item));

        ASSERT("repeated peek must always return same item",
               (uintptr_t)item == 77);

        ASSERT("repeated peek must preserve count",
               fifo_count(&fifo) == 1);
    }

    return 0;
}


/*
 * fifo_push_wait()
 *
 * Only immediately-completing states are tested here. Testing the full-FIFO
 * blocking path requires a second execution context and belongs in the later
 * threaded/platform test suite.
 */

static int test_individual_fifo_push_wait_empty(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    fifo_push_wait(&fifo, (void *)(uintptr_t)12);

    ASSERT("push_wait into empty FIFO must enqueue item",
           fifo_count(&fifo) == 1);

    ASSERT("push_wait item must be retrievable",
           fifo_pop(&fifo, &item));

    ASSERT("push_wait must preserve item value",
           (uintptr_t)item == 12);

    return 0;
}


static int test_individual_fifo_push_wait_partial(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));

    fifo_push_wait(&fifo, (void *)(uintptr_t)2);

    ASSERT("push_wait into partial FIFO must increment count",
           fifo_count(&fifo) == 2);

    ASSERT("first pop must succeed",
           fifo_pop(&fifo, &item));
    ASSERT("first item must remain first",
           (uintptr_t)item == 1);

    ASSERT("second pop must succeed",
           fifo_pop(&fifo, &item));
    ASSERT("push_wait item must follow existing item",
           (uintptr_t)item == 2);

    return 0;
}


static int test_individual_fifo_push_wait_last_slot(void)
{
    fifo_t fifo;
    void *storage[3];

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push one must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("setup push two must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)2));

    fifo_push_wait(&fifo, (void *)(uintptr_t)3);

    ASSERT("push_wait into last free slot must fill FIFO",
           fifo_full(&fifo));

    ASSERT("push_wait into last free slot must reach capacity",
           fifo_count(&fifo) == 3);

    return 0;
}


static int test_individual_fifo_push_wait_null(void)
{
    fifo_t fifo;
    void *storage[2];
    void *item = (void *)(uintptr_t)1;

    fifo_init(&fifo, storage, 2);

    fifo_push_wait(&fifo, NULL);

    ASSERT("push_wait must accept NULL as an item",
           fifo_count(&fifo) == 1);

    ASSERT("push_wait NULL item must be poppable",
           fifo_pop(&fifo, &item));

    ASSERT("push_wait NULL item must remain NULL",
           item == NULL);

    return 0;
}


static int test_individual_fifo_push_wait_wrapped_space(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("setup pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("setup pop must return one", (uintptr_t)item == 1);

    fifo_push_wait(&fifo, (void *)(uintptr_t)4);

    ASSERT("push_wait into wrapped free slot must refill FIFO",
           fifo_full(&fifo));

    ASSERT("next pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("existing two must remain first", (uintptr_t)item == 2);
    ASSERT("next pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("existing three must remain second", (uintptr_t)item == 3);
    ASSERT("wrapped push_wait item pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("wrapped push_wait item must be last", (uintptr_t)item == 4);

    return 0;
}


/*
 * fifo_pop_wait()
 *
 * Only immediately-completing states are tested here. Testing the empty-FIFO
 * blocking path requires another execution context.
 */

static int test_individual_fifo_pop_wait_single(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("setup push must succeed",
           fifo_push(&fifo, (void *)(uintptr_t)23));

    item = fifo_pop_wait(&fifo);

    ASSERT("pop_wait must return sole available item",
           (uintptr_t)item == 23);

    ASSERT("pop_wait must consume sole item",
           fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_pop_wait_partial(void)
{
    fifo_t fifo;
    void *storage[4];
    void *item;

    fifo_init(&fifo, storage, 4);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));

    item = fifo_pop_wait(&fifo);

    ASSERT("pop_wait from partial FIFO must return oldest item",
           (uintptr_t)item == 1);

    ASSERT("pop_wait from partial FIFO must decrement count",
           fifo_count(&fifo) == 2);

    return 0;
}


static int test_individual_fifo_pop_wait_full(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("setup FIFO must be full", fifo_full(&fifo));

    item = fifo_pop_wait(&fifo);

    ASSERT("pop_wait from full FIFO must return oldest item",
           (uintptr_t)item == 1);

    ASSERT("pop_wait from full FIFO must clear full state",
           !fifo_full(&fifo));

    ASSERT("pop_wait from full FIFO must decrement count",
           fifo_count(&fifo) == 2);

    return 0;
}


static int test_individual_fifo_pop_wait_null(void)
{
    fifo_t fifo;
    void *storage[2];

    fifo_init(&fifo, storage, 2);

    ASSERT("setup NULL push must succeed",
           fifo_push(&fifo, NULL));

    ASSERT("pop_wait must return stored NULL item",
           fifo_pop_wait(&fifo) == NULL);

    ASSERT("pop_wait of NULL must still consume item",
           fifo_empty(&fifo));

    return 0;
}


static int test_individual_fifo_pop_wait_wrapped(void)
{
    fifo_t fifo;
    void *storage[3];
    void *item;

    fifo_init(&fifo, storage, 3);

    ASSERT("push one must succeed", fifo_push(&fifo, (void *)(uintptr_t)1));
    ASSERT("push two must succeed", fifo_push(&fifo, (void *)(uintptr_t)2));
    ASSERT("push three must succeed", fifo_push(&fifo, (void *)(uintptr_t)3));
    ASSERT("pre-wrap pop must succeed", fifo_pop(&fifo, &item));
    ASSERT("pre-wrap pop must return one", (uintptr_t)item == 1);
    ASSERT("wrapped push must succeed", fifo_push(&fifo, (void *)(uintptr_t)4));

    item = fifo_pop_wait(&fifo);

    ASSERT("pop_wait after wrap must return logical head",
           (uintptr_t)item == 2);

    ASSERT("pop_wait after wrap must decrement count",
           fifo_count(&fifo) == 2);

    return 0;
}


int main(int argc, char **argv)
{
    int passed_tests = 0;
    int failed_tests = 0;
    int total_tests = 0;

    (void)argc;
    (void)argv;

    fprintf(stdout, "\n");
    fprintf(stdout, "libfifo test suite\n");
    fprintf(stdout, "======================================================================\n\n");

   fprintf(stdout, "======================================================================\n");
    fprintf(stdout, "Individual function exhaustive tests\n");
    fprintf(stdout, "======================================================================\n");

    fprintf(stdout, "\nfifo_spinlock_init()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("spinlock_init produces lockable state",           test_individual_spinlock_init_lockable)
    TEST("spinlock_init instances are independent",         test_individual_spinlock_init_independent)

    fprintf(stdout, "\nfifo_spinlock_trylock()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("spinlock_trylock succeeds when unlocked",         test_individual_spinlock_trylock_unlocked)
    TEST("spinlock_trylock fails when held",                test_individual_spinlock_trylock_held)
    TEST("spinlock_trylock succeeds after unlock",          test_individual_spinlock_trylock_after_unlock)

    fprintf(stdout, "\nfifo_spinlock_lock()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("spinlock_lock acquires unlocked lock",            test_individual_spinlock_lock_unlocked)
    TEST("spinlock_lock remains reusable",                  test_individual_spinlock_lock_reusable)

    fprintf(stdout, "\nfifo_spinlock_unlock()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("spinlock_unlock releases trylocked lock",         test_individual_spinlock_unlock_trylocked)
    TEST("spinlock_unlock releases blocking lock",          test_individual_spinlock_unlock_locked)

    fprintf(stdout, "\nfifo_mutex_init()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("mutex_init produces lockable state",              test_individual_mutex_init_lockable)
    TEST("mutex_init instances are independent",            test_individual_mutex_init_independent)

    fprintf(stdout, "\nfifo_mutex_trylock()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("mutex_trylock succeeds when unlocked",            test_individual_mutex_trylock_unlocked)
    TEST("mutex_trylock fails when held",                   test_individual_mutex_trylock_held)
    TEST("mutex_trylock succeeds after unlock",             test_individual_mutex_trylock_after_unlock)

    fprintf(stdout, "\nfifo_mutex_lock()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("mutex_lock acquires unlocked mutex",              test_individual_mutex_lock_unlocked)
    TEST("mutex_lock remains reusable",                     test_individual_mutex_lock_reusable)

    fprintf(stdout, "\nfifo_mutex_unlock()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("mutex_unlock releases trylocked mutex",           test_individual_mutex_unlock_trylocked)
    TEST("mutex_unlock releases blocking mutex",            test_individual_mutex_unlock_locked)

    fprintf(stdout, "\nfifo_semaphore_init()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("semaphore_init handles zero count",               test_individual_semaphore_init_zero)
    TEST("semaphore_init handles count one",                test_individual_semaphore_init_one)
    TEST("semaphore_init handles large count",              test_individual_semaphore_init_many)

    fprintf(stdout, "\nfifo_semaphore_trywait()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("semaphore_trywait fails at zero",                 test_individual_semaphore_trywait_empty)
    TEST("semaphore_trywait consumes one",                  test_individual_semaphore_trywait_single)
    TEST("semaphore_trywait consumes many exactly",         test_individual_semaphore_trywait_many)
    TEST("semaphore_trywait works after post",              test_individual_semaphore_trywait_after_post)

    fprintf(stdout, "\nfifo_semaphore_wait()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("semaphore_wait consumes count one",               test_individual_semaphore_wait_one)
    TEST("semaphore_wait consumes exactly one of many",     test_individual_semaphore_wait_many)
    TEST("semaphore_wait consumes posted unit",             test_individual_semaphore_wait_after_post)

    fprintf(stdout, "\nfifo_semaphore_post()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("semaphore_post increments zero",                  test_individual_semaphore_post_zero)
    TEST("semaphore_post increments nonzero",               test_individual_semaphore_post_nonzero)
    TEST("semaphore_post accumulates repeatedly",           test_individual_semaphore_post_accumulates)
    TEST("semaphore_post restores consumed count",          test_individual_semaphore_post_after_consumption)

    fprintf(stdout, "\nfifo_condition_init()/signal()/broadcast()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("condition_init resets sequence",                  test_individual_condition_init_sequence)
    TEST("condition_signal advances sequence",              test_individual_condition_signal_changes_sequence)
    TEST("condition_signal advances every time",            test_individual_condition_signal_repeated)
    TEST("condition_broadcast advances sequence",           test_individual_condition_broadcast_changes_sequence)
    TEST("condition signal/broadcast both advance",         test_individual_condition_signal_broadcast_sequence)

    fprintf(stdout, "\nfifo_init()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_init handles capacity one",                  test_individual_fifo_init_capacity_one)
    TEST("fifo_init handles typical capacity",              test_individual_fifo_init_typical)
    TEST("fifo_init handles odd capacity",                  test_individual_fifo_init_odd_capacity)
    TEST("fifo_init resets nonempty FIFO on reinit",        test_individual_fifo_init_reinitialise_nonempty)

    fprintf(stdout, "\nfifo_capacity()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_capacity is correct while empty",            test_individual_fifo_capacity_empty)
    TEST("fifo_capacity is correct while partial",          test_individual_fifo_capacity_partial)
    TEST("fifo_capacity is correct while full",             test_individual_fifo_capacity_full)
    TEST("fifo_capacity survives wraparound",               test_individual_fifo_capacity_wrapped)

    fprintf(stdout, "\nfifo_count()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_count reports empty",                        test_individual_fifo_count_empty)
    TEST("fifo_count tracks partial fill",                  test_individual_fifo_count_partial)
    TEST("fifo_count reports full capacity",                test_individual_fifo_count_full)
    TEST("fifo_count decrements after pop",                 test_individual_fifo_count_after_pop)
    TEST("fifo_count survives failed push",                 test_individual_fifo_count_failed_push)
    TEST("fifo_count survives failed pop",                  test_individual_fifo_count_failed_pop)
    TEST("fifo_count survives wraparound",                  test_individual_fifo_count_wrapped)

    fprintf(stdout, "\nfifo_empty()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_empty reports initial empty",                test_individual_fifo_empty_initial)
    TEST("fifo_empty rejects partial state",                test_individual_fifo_empty_partial)
    TEST("fifo_empty rejects full state",                   test_individual_fifo_empty_full)
    TEST("fifo_empty reports drained state",                test_individual_fifo_empty_after_drain)
    TEST("fifo_empty treats NULL as an item",               test_individual_fifo_empty_null_item)

    fprintf(stdout, "\nfifo_full()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_full rejects initial state",                 test_individual_fifo_full_initial)
    TEST("fifo_full rejects partial state",                 test_individual_fifo_full_partial)
    TEST("fifo_full reports exact capacity",                test_individual_fifo_full_exact)
    TEST("fifo_full survives failed overflow",              test_individual_fifo_full_after_failed_push)
    TEST("fifo_full clears after pop",                      test_individual_fifo_full_after_pop)
    TEST("fifo_full handles capacity one",                  test_individual_fifo_full_capacity_one)
    TEST("fifo_full restores after wrapped refill",         test_individual_fifo_full_after_wrap_refill)

    fprintf(stdout, "\nfifo_push()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_push handles empty FIFO",                    test_individual_fifo_push_empty)
    TEST("fifo_push handles partial FIFO",                  test_individual_fifo_push_partial)
    TEST("fifo_push handles final legal slot",              test_individual_fifo_push_exact_capacity)
    TEST("fifo_push fails cleanly when full",               test_individual_fifo_push_full_failure)
    TEST("fifo_push accepts NULL",                          test_individual_fifo_push_null)
    TEST("fifo_push reuses wrapped free slot",              test_individual_fifo_push_after_pop_wrap)
    TEST("fifo_push handles capacity one",                  test_individual_fifo_push_capacity_one)

    fprintf(stdout, "\nfifo_pop()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_pop fails cleanly when empty",               test_individual_fifo_pop_empty_failure)
    TEST("fifo_pop handles sole item",                      test_individual_fifo_pop_single)
    TEST("fifo_pop handles partial FIFO",                   test_individual_fifo_pop_partial)
    TEST("fifo_pop handles full FIFO",                      test_individual_fifo_pop_full)
    TEST("fifo_pop returns stored NULL",                    test_individual_fifo_pop_null)
    TEST("fifo_pop handles wrapped FIFO",                   test_individual_fifo_pop_wrapped)
    TEST("fifo_pop handles capacity-one reuse",             test_individual_fifo_pop_capacity_one_reuse)

    fprintf(stdout, "\nfifo_peek()\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_peek fails cleanly when empty",              test_individual_fifo_peek_empty_failure)
    TEST("fifo_peek handles sole item",                     test_individual_fifo_peek_single)
    TEST("fifo_peek handles partial FIFO",                  test_individual_fifo_peek_partial)
    TEST("fifo_peek handles full FIFO",                     test_individual_fifo_peek_full)
    TEST("fifo_peek returns stored NULL",                   test_individual_fifo_peek_null)
    TEST("fifo_peek handles wrapped FIFO",                  test_individual_fifo_peek_wrapped)
    TEST("fifo_peek remains non-consuming repeatedly",      test_individual_fifo_peek_repeated)

    fprintf(stdout, "\nfifo_push_wait() immediate states\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_push_wait handles empty FIFO",               test_individual_fifo_push_wait_empty)
    TEST("fifo_push_wait handles partial FIFO",             test_individual_fifo_push_wait_partial)
    TEST("fifo_push_wait fills last free slot",             test_individual_fifo_push_wait_last_slot)
    TEST("fifo_push_wait accepts NULL",                     test_individual_fifo_push_wait_null)
    TEST("fifo_push_wait handles wrapped free slot",        test_individual_fifo_push_wait_wrapped_space)

    fprintf(stdout, "\nfifo_pop_wait() immediate states\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("fifo_pop_wait handles sole item",                 test_individual_fifo_pop_wait_single)
    TEST("fifo_pop_wait handles partial FIFO",              test_individual_fifo_pop_wait_partial)
    TEST("fifo_pop_wait handles full FIFO",                 test_individual_fifo_pop_wait_full)
    TEST("fifo_pop_wait returns stored NULL",               test_individual_fifo_pop_wait_null)
    TEST("fifo_pop_wait handles wrapped FIFO",              test_individual_fifo_pop_wait_wrapped)

    fprintf(stdout,"\n");

    fprintf(stdout, "Spinlocks\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Initialised spinlock is unlocked",           test_spinlock_init_unlocked)
    TEST("Trylock fails on locked spinlock",           test_spinlock_trylock_locked)
    TEST("Unlock makes spinlock available",            test_spinlock_unlock)
    TEST("Lock acquires spinlock",                     test_spinlock_lock)
    TEST("Repeated spinlock acquire/release",          test_spinlock_repeated)

    fprintf(stdout, "\nMutexes\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Initialised mutex is unlocked",              test_mutex_init_unlocked)
    TEST("Trylock fails on locked mutex",              test_mutex_trylock_locked)
    TEST("Unlock makes mutex available",               test_mutex_unlock)
    TEST("Lock acquires mutex",                        test_mutex_lock)
    TEST("Repeated mutex acquire/release",             test_mutex_repeated)

    fprintf(stdout, "\nSemaphores\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Zero-count semaphore rejects trywait",       test_semaphore_zero)
    TEST("Count-one semaphore accepts trywait",        test_semaphore_one)
    TEST("Trywait consumes semaphore count",           test_semaphore_consumes)
    TEST("Initial semaphore count is honoured",        test_semaphore_initial_count)
    TEST("Post increments zero semaphore",             test_semaphore_post_zero)
    TEST("Post restores consumed semaphore",           test_semaphore_post_consumed)
    TEST("Multiple semaphore posts accumulate",        test_semaphore_multiple_posts)

    fprintf(stdout, "\nCondition variables\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Condition variable initialisation",          test_condition_init)

    fprintf(stdout, "\nFIFO initialisation and state\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("FIFO stores capacity",                       test_fifo_capacity)
    TEST("FIFO initial count is zero",                 test_fifo_initial_count)
    TEST("FIFO initially reports empty",               test_fifo_initial_empty)
    TEST("FIFO initially reports not full",            test_fifo_initial_not_full)
    TEST("FIFO supports capacity one",                 test_fifo_capacity_one)

    fprintf(stdout, "\nFIFO push\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Push into empty FIFO succeeds",              test_fifo_push)
    TEST("Push increments FIFO count",                 test_fifo_push_increments_count)
    TEST("Push clears empty state",                    test_fifo_push_makes_nonempty)
    TEST("Push succeeds up to capacity",               test_fifo_push_to_capacity)
    TEST("FIFO reports full at capacity",              test_fifo_full_at_capacity)
    TEST("Push beyond capacity fails",                 test_fifo_push_when_full)
    TEST("Failed push preserves count",                test_fifo_failed_push_preserves_count)
    TEST("FIFO accepts NULL item",                     test_fifo_push_null)

    fprintf(stdout, "\nFIFO peek\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Peek on empty FIFO fails",                   test_fifo_peek_empty)
    TEST("Peek returns first item",                    test_fifo_peek)
    TEST("Peek preserves FIFO count",                  test_fifo_peek_preserves_count)
    TEST("Repeated peek does not consume",             test_fifo_repeated_peek)
    TEST("Peek distinguishes stored NULL",             test_fifo_peek_null_item)

    fprintf(stdout, "\nFIFO pop\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Pop on empty FIFO fails",                    test_fifo_pop_empty)
    TEST("Pop returns pushed item",                    test_fifo_pop)
    TEST("Pop decrements FIFO count",                  test_fifo_pop_decrements_count)
    TEST("Popping last item makes FIFO empty",         test_fifo_pop_last_makes_empty)
    TEST("Pop clears full state",                      test_fifo_pop_from_full_clears_full)
    TEST("Pop returns stored NULL item",               test_fifo_pop_null_item)

    fprintf(stdout, "\nFIFO ordering\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Two items preserve FIFO ordering",           test_fifo_two_item_order)
    TEST("Many items preserve FIFO ordering",          test_fifo_many_item_order)

    fprintf(stdout, "\nFIFO capacity-one edge cases\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("Capacity-one FIFO becomes full",             test_fifo_one_push_full)
    TEST("Capacity-one FIFO rejects second push",      test_fifo_one_second_push_fails)
    TEST("Capacity-one FIFO survives repeated reuse",  test_fifo_one_reuse)

    fprintf(stdout, "\nFIFO wraparound\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("FIFO survives single wraparound",            test_fifo_single_wraparound)
    TEST("FIFO survives repeated wraparound",          test_fifo_repeated_wraparound)
    TEST("FIFO survives interleaved wraparound",       test_fifo_interleaved_wraparound)

    fprintf(stdout, "\nFIFO state transitions\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("FIFO transitions empty-full-empty",          test_fifo_empty_full_empty)
    TEST("FIFO count tracks every transition",         test_fifo_count_transitions)
    TEST("Failed empty pop preserves state",           test_fifo_failed_pop_preserves_state)

    fprintf(stdout, "\nFIFO reuse\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");

    TEST("FIFO can be reinitialised",                  test_fifo_reinitialise)
    TEST("FIFO survives repeated fill/drain",          test_fifo_reuse_after_drain)


 
    fprintf(stdout, "\n");
    fprintf(stdout, "======================================================================\n");
    fprintf(stdout, "Tests: %-4d  Passed: %-4d  Failed: %-4d\n",
            total_tests,
            passed_tests,
            failed_tests);
    fprintf(stdout, "======================================================================\n");

    return failed_tests > 0 ? 1 : 0;
}
