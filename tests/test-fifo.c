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
        fprintf(stderr, "Testing: %-58s", desc);                          \
        result = (f)();                                                   \
                                                                          \
        if (result == 0) {                                                \
            passed_tests++;                                               \
            fprintf(stderr, "PASS\n");                                    \
        } else {                                                          \
            failed_tests++;                                               \
            fprintf(stderr, "FAIL");                                      \
            if (failed_assert != NULL)                                    \
                fprintf(stderr, "  -  %s", failed_assert);                \
            fprintf(stderr, "\n");                                        \
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


int main(int argc, char **argv)
{
    int passed_tests = 0;
    int failed_tests = 0;
    int total_tests = 0;

    (void)argc;
    (void)argv;

    fprintf(stderr, "\n");
    fprintf(stderr, "libfifo test suite\n");
    fprintf(stderr, "======================================================================\n\n");

    fprintf(stderr, "Spinlocks\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Initialised spinlock is unlocked",           test_spinlock_init_unlocked)
    TEST("Trylock fails on locked spinlock",           test_spinlock_trylock_locked)
    TEST("Unlock makes spinlock available",            test_spinlock_unlock)
    TEST("Lock acquires spinlock",                     test_spinlock_lock)
    TEST("Repeated spinlock acquire/release",          test_spinlock_repeated)

    fprintf(stderr, "\nMutexes\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Initialised mutex is unlocked",              test_mutex_init_unlocked)
    TEST("Trylock fails on locked mutex",              test_mutex_trylock_locked)
    TEST("Unlock makes mutex available",               test_mutex_unlock)
    TEST("Lock acquires mutex",                        test_mutex_lock)
    TEST("Repeated mutex acquire/release",             test_mutex_repeated)

    fprintf(stderr, "\nSemaphores\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Zero-count semaphore rejects trywait",       test_semaphore_zero)
    TEST("Count-one semaphore accepts trywait",        test_semaphore_one)
    TEST("Trywait consumes semaphore count",           test_semaphore_consumes)
    TEST("Initial semaphore count is honoured",        test_semaphore_initial_count)
    TEST("Post increments zero semaphore",             test_semaphore_post_zero)
    TEST("Post restores consumed semaphore",           test_semaphore_post_consumed)
    TEST("Multiple semaphore posts accumulate",        test_semaphore_multiple_posts)

    fprintf(stderr, "\nCondition variables\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Condition variable initialisation",          test_condition_init)

    fprintf(stderr, "\nFIFO initialisation and state\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("FIFO stores capacity",                       test_fifo_capacity)
    TEST("FIFO initial count is zero",                 test_fifo_initial_count)
    TEST("FIFO initially reports empty",               test_fifo_initial_empty)
    TEST("FIFO initially reports not full",            test_fifo_initial_not_full)
    TEST("FIFO supports capacity one",                 test_fifo_capacity_one)

    fprintf(stderr, "\nFIFO push\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Push into empty FIFO succeeds",              test_fifo_push)
    TEST("Push increments FIFO count",                 test_fifo_push_increments_count)
    TEST("Push clears empty state",                    test_fifo_push_makes_nonempty)
    TEST("Push succeeds up to capacity",               test_fifo_push_to_capacity)
    TEST("FIFO reports full at capacity",              test_fifo_full_at_capacity)
    TEST("Push beyond capacity fails",                 test_fifo_push_when_full)
    TEST("Failed push preserves count",                test_fifo_failed_push_preserves_count)
    TEST("FIFO accepts NULL item",                     test_fifo_push_null)

    fprintf(stderr, "\nFIFO peek\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Peek on empty FIFO fails",                   test_fifo_peek_empty)
    TEST("Peek returns first item",                    test_fifo_peek)
    TEST("Peek preserves FIFO count",                  test_fifo_peek_preserves_count)
    TEST("Repeated peek does not consume",             test_fifo_repeated_peek)
    TEST("Peek distinguishes stored NULL",             test_fifo_peek_null_item)

    fprintf(stderr, "\nFIFO pop\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Pop on empty FIFO fails",                    test_fifo_pop_empty)
    TEST("Pop returns pushed item",                    test_fifo_pop)
    TEST("Pop decrements FIFO count",                  test_fifo_pop_decrements_count)
    TEST("Popping last item makes FIFO empty",         test_fifo_pop_last_makes_empty)
    TEST("Pop clears full state",                      test_fifo_pop_from_full_clears_full)
    TEST("Pop returns stored NULL item",               test_fifo_pop_null_item)

    fprintf(stderr, "\nFIFO ordering\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Two items preserve FIFO ordering",           test_fifo_two_item_order)
    TEST("Many items preserve FIFO ordering",          test_fifo_many_item_order)

    fprintf(stderr, "\nFIFO capacity-one edge cases\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("Capacity-one FIFO becomes full",             test_fifo_one_push_full)
    TEST("Capacity-one FIFO rejects second push",      test_fifo_one_second_push_fails)
    TEST("Capacity-one FIFO survives repeated reuse",  test_fifo_one_reuse)

    fprintf(stderr, "\nFIFO wraparound\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("FIFO survives single wraparound",            test_fifo_single_wraparound)
    TEST("FIFO survives repeated wraparound",          test_fifo_repeated_wraparound)
    TEST("FIFO survives interleaved wraparound",       test_fifo_interleaved_wraparound)

    fprintf(stderr, "\nFIFO state transitions\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("FIFO transitions empty-full-empty",          test_fifo_empty_full_empty)
    TEST("FIFO count tracks every transition",         test_fifo_count_transitions)
    TEST("Failed empty pop preserves state",           test_fifo_failed_pop_preserves_state)

    fprintf(stderr, "\nFIFO reuse\n");
    fprintf(stderr, "----------------------------------------------------------------------\n");

    TEST("FIFO can be reinitialised",                  test_fifo_reinitialise)
    TEST("FIFO survives repeated fill/drain",          test_fifo_reuse_after_drain)

    fprintf(stderr, "\n");
    fprintf(stderr, "======================================================================\n");
    fprintf(stderr, "Tests: %-4d  Passed: %-4d  Failed: %-4d\n",
            total_tests,
            passed_tests,
            failed_tests);
    fprintf(stderr, "======================================================================\n");

    return failed_tests > 0 ? 1 : 0;
}
