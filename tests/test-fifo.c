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


/*
 * Platform-yield test support.
 *
 * Blocking primitive tests install one of these callbacks to make the
 * condition they are waiting for become true.  This lets the blocking path
 * be tested synchronously while also proving that it called
 * fifo_platform_yield().
 */
static size_t test_yield_count = 0;
static size_t test_yield_alt_count = 0;
static fifo_mutex_t *test_yield_mutex = NULL;
static fifo_semaphore_t *test_yield_semaphore = NULL;
static fifo_condition_t *test_yield_condition = NULL;
static fifo_t *test_yield_fifo = NULL;
static void *test_yield_fifo_item = NULL;

static void test_yield_counter_callback(void)
{
    test_yield_count++;
}

static void test_yield_alt_counter_callback(void)
{
    test_yield_alt_count++;
}

static void test_yield_unlock_mutex_callback(void)
{
    test_yield_count++;
    atomic_store_explicit(&test_yield_mutex->state, 0, memory_order_relaxed);
}

static void test_yield_post_semaphore_callback(void)
{
    test_yield_count++;
    atomic_fetch_add_explicit(&test_yield_semaphore->count, 1,
                              memory_order_relaxed);
}

static void test_yield_signal_condition_callback(void)
{
    test_yield_count++;
    atomic_fetch_add_explicit(&test_yield_condition->sequence, 1,
                              memory_order_relaxed);
}

static void test_yield_make_fifo_writable_callback(void)
{
    test_yield_count++;

    if (test_yield_fifo->count != 0) {
        test_yield_fifo->head =
            (test_yield_fifo->head + 1) % test_yield_fifo->capacity;
        test_yield_fifo->count--;
    }
}

static void test_yield_make_fifo_readable_callback(void)
{
    test_yield_count++;

    if (test_yield_fifo->count == 0) {
        test_yield_fifo->items[test_yield_fifo->tail] = test_yield_fifo_item;
        test_yield_fifo->tail =
            (test_yield_fifo->tail + 1) % test_yield_fifo->capacity;
        test_yield_fifo->count++;
    }
}


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
 * Platform yield callback tests
 */

static int test_platform_yield_invokes_callback(void)
{
    test_yield_count = 0;
    fifo_set_yield_callback(test_yield_counter_callback);

    fifo_platform_yield();

    ASSERT("platform yield must invoke installed callback",
           test_yield_count == 1);

    fifo_set_yield_callback(NULL);
    return 0;
}


static int test_platform_yield_repeated_callback(void)
{
    test_yield_count = 0;
    fifo_set_yield_callback(test_yield_counter_callback);

    fifo_platform_yield();
    fifo_platform_yield();
    fifo_platform_yield();

    ASSERT("platform yield must invoke callback once per call",
           test_yield_count == 3);

    fifo_set_yield_callback(NULL);
    return 0;
}


static int test_platform_yield_replaces_callback(void)
{
    test_yield_count = 0;
    test_yield_alt_count = 0;

    fifo_set_yield_callback(test_yield_counter_callback);
    fifo_platform_yield();

    fifo_set_yield_callback(test_yield_alt_counter_callback);
    fifo_platform_yield();

    ASSERT("original yield callback must run before replacement",
           test_yield_count == 1);
    ASSERT("replacement yield callback must run after replacement",
           test_yield_alt_count == 1);

    fifo_set_yield_callback(NULL);
    return 0;
}


static int test_platform_yield_clear_callback(void)
{
    test_yield_count = 0;

    fifo_set_yield_callback(test_yield_counter_callback);
    fifo_platform_yield();
    ASSERT("installed yield callback must run", test_yield_count == 1);

    fifo_set_yield_callback(NULL);
    fifo_platform_yield();

    ASSERT("cleared yield callback must not be invoked",
           test_yield_count == 1);
    return 0;
}


/*
 * ======================================================================
 * Individual function exhaustive tests
 * ======================================================================
 *
 * These tests deliberately call exactly ONE libfifo public function per
 * test. All prerequisite state is constructed directly through the public
 * structure fields and C11 atomics, and results are inspected directly.
 *
 * This is intentional: implementing one libfifo function must not make
 * another function's individual tests start passing merely because that
 * second test used the first function for setup or verification.
 *
 * The later integration/behaviour tests are deliberately left unchanged.
 */


/* fifo_spinlock_init() */


static int test_individual_spinlock_init_lockable(void)
{
    fifo_spinlock_t lock;
    atomic_flag_test_and_set_explicit(&lock.locked, memory_order_relaxed);

    fifo_spinlock_init(&lock);

    ASSERT("spinlock_init must clear a previously set flag",
           !atomic_flag_test_and_set_explicit(&lock.locked,
                                              memory_order_relaxed));
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


static int test_individual_spinlock_init_independent(void)
{
    fifo_spinlock_t a;
    fifo_spinlock_t b;

    atomic_flag_test_and_set_explicit(&a.locked, memory_order_relaxed);
    atomic_flag_test_and_set_explicit(&b.locked, memory_order_relaxed);

    fifo_spinlock_init(&a);

    ASSERT("spinlock_init must clear target flag",
           !atomic_flag_test_and_set_explicit(&a.locked,
                                              memory_order_relaxed));
    ASSERT("spinlock_init must not alter another lock",
           atomic_flag_test_and_set_explicit(&b.locked,
                                             memory_order_relaxed));
    atomic_flag_clear_explicit(&a.locked, memory_order_relaxed);
    atomic_flag_clear_explicit(&b.locked, memory_order_relaxed);
    return 0;
}


/* fifo_spinlock_trylock() */


static int test_individual_spinlock_trylock_unlocked(void)
{
    fifo_spinlock_t lock;
    bool result;
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    result = fifo_spinlock_trylock(&lock);

    ASSERT("trylock on unlocked spinlock must succeed", result);
    ASSERT("successful trylock must set lock flag",
           atomic_flag_test_and_set_explicit(&lock.locked,
                                             memory_order_relaxed));
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


static int test_individual_spinlock_trylock_held(void)
{
    fifo_spinlock_t lock;
    bool result;

    atomic_flag_test_and_set_explicit(&lock.locked, memory_order_relaxed);

    result = fifo_spinlock_trylock(&lock);

    ASSERT("trylock on held spinlock must fail", !result);
    ASSERT("failed trylock must leave lock held",
           atomic_flag_test_and_set_explicit(&lock.locked,
                                             memory_order_relaxed));

    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    result = fifo_spinlock_trylock(&lock);

    ASSERT("same trylock implementation must acquire an unlocked spinlock",
           result);
    ASSERT("successful trylock must set lock state",
           atomic_flag_test_and_set_explicit(&lock.locked,
                                             memory_order_relaxed));

    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


static int test_individual_spinlock_trylock_after_unlock(void)
{
    fifo_spinlock_t lock;
    bool result;
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    result = fifo_spinlock_trylock(&lock);

    ASSERT("trylock must acquire directly-unlocked spinlock", result);
    ASSERT("trylock must leave acquired state set",
           atomic_flag_test_and_set_explicit(&lock.locked,
                                             memory_order_relaxed));
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


/* fifo_spinlock_lock() */


static int test_individual_spinlock_lock_unlocked(void)
{
    fifo_spinlock_t lock;
    bool held;
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    fifo_spinlock_lock(&lock);

    held = atomic_flag_test_and_set_explicit(&lock.locked,
                                             memory_order_relaxed);
    ASSERT("spinlock_lock must acquire an unlocked spinlock", held);
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


static int test_individual_spinlock_lock_reusable(void)
{
    fifo_spinlock_t lock;
    bool held;
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);

    fifo_spinlock_lock(&lock);

    held = atomic_flag_test_and_set_explicit(&lock.locked,
                                             memory_order_relaxed);
    ASSERT("spinlock_lock must acquire an unlocked spinlock", held);
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


/* fifo_spinlock_unlock() */


static int test_individual_spinlock_unlock_trylocked(void)
{
    fifo_spinlock_t lock;
    bool was_locked;
    atomic_flag_test_and_set_explicit(&lock.locked, memory_order_relaxed);

    fifo_spinlock_unlock(&lock);

    was_locked = atomic_flag_test_and_set_explicit(&lock.locked,
                                                   memory_order_relaxed);
    ASSERT("spinlock_unlock must clear a held lock", !was_locked);
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


static int test_individual_spinlock_unlock_locked(void)
{
    fifo_spinlock_t lock;
    bool was_locked;
    atomic_flag_test_and_set_explicit(&lock.locked, memory_order_relaxed);

    fifo_spinlock_unlock(&lock);

    was_locked = atomic_flag_test_and_set_explicit(&lock.locked,
                                                   memory_order_relaxed);
    ASSERT("spinlock_unlock must clear a held lock", !was_locked);
    atomic_flag_clear_explicit(&lock.locked, memory_order_relaxed);
    return 0;
}


/* fifo_mutex_init() */


static int test_individual_mutex_init_lockable(void)
{
    fifo_mutex_t mutex;
    atomic_store_explicit(&mutex.state, 0x5a5aU, memory_order_relaxed);

    fifo_mutex_init(&mutex);

    ASSERT("mutex_init must reset state to unlocked",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_mutex_init_independent(void)
{
    fifo_mutex_t a;
    fifo_mutex_t b;
    atomic_store_explicit(&a.state, 0x5a5aU, memory_order_relaxed);
    atomic_store_explicit(&b.state, 0xa5a5U, memory_order_relaxed);

    fifo_mutex_init(&a);

    ASSERT("mutex_init must reset target state",
           atomic_load_explicit(&a.state, memory_order_relaxed) == 0);
    ASSERT("mutex_init must not alter another mutex",
           atomic_load_explicit(&b.state, memory_order_relaxed) == 0xa5a5U);
    return 0;
}


/* fifo_mutex_trylock() */


static int test_individual_mutex_trylock_unlocked(void)
{
    fifo_mutex_t mutex;
    bool result;
    atomic_store_explicit(&mutex.state, 0, memory_order_relaxed);

    result = fifo_mutex_trylock(&mutex);

    ASSERT("trylock on unlocked mutex must succeed", result);
    ASSERT("successful trylock must change mutex state",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}


static int test_individual_mutex_trylock_held(void)
{
    fifo_mutex_t mutex;
    bool result;

    atomic_store_explicit(&mutex.state, 1, memory_order_relaxed);

    result = fifo_mutex_trylock(&mutex);

    ASSERT("trylock on held mutex must fail", !result);
    ASSERT("failed trylock must preserve held state",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);

    atomic_store_explicit(&mutex.state, 0, memory_order_relaxed);

    result = fifo_mutex_trylock(&mutex);

    ASSERT("same trylock implementation must acquire an unlocked mutex",
           result);
    ASSERT("successful trylock must set mutex state",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}


static int test_individual_mutex_trylock_after_unlock(void)
{
    fifo_mutex_t mutex;
    bool result;
    atomic_store_explicit(&mutex.state, 0, memory_order_relaxed);

    result = fifo_mutex_trylock(&mutex);

    ASSERT("trylock must acquire directly-unlocked mutex", result);
    ASSERT("trylock must leave mutex held",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}


/* fifo_mutex_lock() */


static int test_individual_mutex_lock_unlocked(void)
{
    fifo_mutex_t mutex;
    atomic_store_explicit(&mutex.state, 0, memory_order_relaxed);

    fifo_mutex_lock(&mutex);

    ASSERT("mutex_lock must change unlocked state to locked",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}


static int test_individual_mutex_lock_reusable(void)
{
    fifo_mutex_t mutex;
    atomic_store_explicit(&mutex.state, 0, memory_order_relaxed);

    fifo_mutex_lock(&mutex);

    ASSERT("mutex_lock must change unlocked state to locked",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}



static int test_individual_mutex_lock_contended_yields(void)
{
    fifo_mutex_t mutex;

    atomic_store_explicit(&mutex.state, 1, memory_order_relaxed);
    test_yield_count = 0;
    test_yield_mutex = &mutex;
    fifo_set_yield_callback(test_yield_unlock_mutex_callback);

    fifo_mutex_lock(&mutex);

    fifo_set_yield_callback(NULL);
    test_yield_mutex = NULL;

    ASSERT("contended mutex_lock must call platform yield",
           test_yield_count > 0);
    ASSERT("mutex_lock must acquire mutex after contention clears",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}


/* fifo_mutex_unlock() */


static int test_individual_mutex_unlock_trylocked(void)
{
    fifo_mutex_t mutex;
    atomic_store_explicit(&mutex.state, 1, memory_order_relaxed);

    fifo_mutex_unlock(&mutex);

    ASSERT("mutex_unlock must reset held state to unlocked",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_mutex_unlock_locked(void)
{
    fifo_mutex_t mutex;
    atomic_store_explicit(&mutex.state, 1, memory_order_relaxed);

    fifo_mutex_unlock(&mutex);

    ASSERT("mutex_unlock must reset held state to unlocked",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) == 0);
    return 0;
}


/* fifo_semaphore_init() */


static int test_individual_semaphore_init_zero(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 999, memory_order_relaxed);

    fifo_semaphore_init(&semaphore, 0);

    ASSERT("semaphore_init must store exact initial count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_semaphore_init_one(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 999, memory_order_relaxed);

    fifo_semaphore_init(&semaphore, 1);

    ASSERT("semaphore_init must store exact initial count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 1);
    return 0;
}


static int test_individual_semaphore_init_many(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 999, memory_order_relaxed);

    fifo_semaphore_init(&semaphore, 257);

    ASSERT("semaphore_init must store exact initial count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 257);
    return 0;
}


/* fifo_semaphore_trywait() */


static int test_individual_semaphore_trywait_empty(void)
{
    fifo_semaphore_t semaphore;
    bool result;

    atomic_store_explicit(&semaphore.count, 0, memory_order_relaxed);

    result = fifo_semaphore_trywait(&semaphore);

    ASSERT("trywait on empty semaphore must fail", !result);
    ASSERT("failed trywait must preserve zero count",
           atomic_load_explicit(&semaphore.count,
                                memory_order_relaxed) == 0);

    atomic_store_explicit(&semaphore.count, 1, memory_order_relaxed);

    result = fifo_semaphore_trywait(&semaphore);

    ASSERT("same trywait implementation must consume an available unit",
           result);
    ASSERT("successful trywait must decrement count",
           atomic_load_explicit(&semaphore.count,
                                memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_semaphore_trywait_single(void)
{
    fifo_semaphore_t semaphore;
    bool result;
    atomic_store_explicit(&semaphore.count, 1, memory_order_relaxed);

    result = fifo_semaphore_trywait(&semaphore);

    ASSERT("semaphore_trywait return value must match availability",
           result == true);
    ASSERT("semaphore_trywait must leave exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_semaphore_trywait_many(void)
{
    fifo_semaphore_t semaphore;
    bool result;
    atomic_store_explicit(&semaphore.count, 4, memory_order_relaxed);

    result = fifo_semaphore_trywait(&semaphore);

    ASSERT("semaphore_trywait return value must match availability",
           result == true);
    ASSERT("semaphore_trywait must leave exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 3);
    return 0;
}


static int test_individual_semaphore_trywait_after_post(void)
{
    fifo_semaphore_t semaphore;
    bool result;
    atomic_store_explicit(&semaphore.count, 2, memory_order_relaxed);

    result = fifo_semaphore_trywait(&semaphore);

    ASSERT("semaphore_trywait return value must match availability",
           result == true);
    ASSERT("semaphore_trywait must leave exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 1);
    return 0;
}


/* fifo_semaphore_wait() */


static int test_individual_semaphore_wait_one(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 1, memory_order_relaxed);

    fifo_semaphore_wait(&semaphore);

    ASSERT("semaphore_wait must consume exactly one available unit",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_semaphore_wait_many(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 3, memory_order_relaxed);

    fifo_semaphore_wait(&semaphore);

    ASSERT("semaphore_wait must consume exactly one available unit",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 2);
    return 0;
}


static int test_individual_semaphore_wait_after_post(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 1, memory_order_relaxed);

    fifo_semaphore_wait(&semaphore);

    ASSERT("semaphore_wait must consume exactly one available unit",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 0);
    return 0;
}



static int test_individual_semaphore_wait_zero_yields(void)
{
    fifo_semaphore_t semaphore;

    atomic_store_explicit(&semaphore.count, 0, memory_order_relaxed);
    test_yield_count = 0;
    test_yield_semaphore = &semaphore;
    fifo_set_yield_callback(test_yield_post_semaphore_callback);

    fifo_semaphore_wait(&semaphore);

    fifo_set_yield_callback(NULL);
    test_yield_semaphore = NULL;

    ASSERT("semaphore_wait on zero must call platform yield",
           test_yield_count > 0);
    ASSERT("semaphore_wait must consume unit made available while yielding",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 0);
    return 0;
}


/* fifo_semaphore_post() */


static int test_individual_semaphore_post_zero(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 0, memory_order_relaxed);

    fifo_semaphore_post(&semaphore);

    ASSERT("semaphore_post must produce exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 1);
    return 0;
}


static int test_individual_semaphore_post_nonzero(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 2, memory_order_relaxed);

    fifo_semaphore_post(&semaphore);

    ASSERT("semaphore_post must produce exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 3);
    return 0;
}


static int test_individual_semaphore_post_accumulates(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 0, memory_order_relaxed);

    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);
    fifo_semaphore_post(&semaphore);

    ASSERT("semaphore_post must produce exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 100);
    return 0;
}


static int test_individual_semaphore_post_after_consumption(void)
{
    fifo_semaphore_t semaphore;
    atomic_store_explicit(&semaphore.count, 0, memory_order_relaxed);

    fifo_semaphore_post(&semaphore);

    ASSERT("semaphore_post must produce exact expected count",
           atomic_load_explicit(&semaphore.count, memory_order_relaxed) == 1);
    return 0;
}


/* fifo_condition_init(), signal(), broadcast() */


static int test_individual_condition_init_sequence(void)
{
    fifo_condition_t condition;
    atomic_store_explicit(&condition.sequence, 0x5a5a5a5aU,
                          memory_order_relaxed);

    fifo_condition_init(&condition);

    ASSERT("condition_init must reset sequence to zero",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 0);
    return 0;
}


static int test_individual_condition_signal_changes_sequence(void)
{
    fifo_condition_t condition;
    atomic_store_explicit(&condition.sequence, 17, memory_order_relaxed);

    fifo_condition_signal(&condition);

    ASSERT("condition operation must advance sequence once per call",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 18);
    return 0;
}


static int test_individual_condition_signal_repeated(void)
{
    fifo_condition_t condition;
    atomic_store_explicit(&condition.sequence, 17, memory_order_relaxed);

    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);
    fifo_condition_signal(&condition);

    ASSERT("condition operation must advance sequence once per call",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 117);
    return 0;
}


static int test_individual_condition_broadcast_changes_sequence(void)
{
    fifo_condition_t condition;
    atomic_store_explicit(&condition.sequence, 17, memory_order_relaxed);

    fifo_condition_broadcast(&condition);

    ASSERT("condition operation must advance sequence once per call",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 18);
    return 0;
}


static int test_individual_condition_signal_broadcast_sequence(void)
{
    fifo_condition_t condition;
    atomic_store_explicit(&condition.sequence, 17, memory_order_relaxed);

    fifo_condition_broadcast(&condition);
    fifo_condition_broadcast(&condition);

    ASSERT("condition operation must advance sequence once per call",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 19);
    return 0;
}



/* fifo_condition_wait() */

static int test_individual_condition_wait_yields(void)
{
    fifo_condition_t condition;
    fifo_mutex_t mutex;

    atomic_store_explicit(&condition.sequence, 7, memory_order_relaxed);
    atomic_store_explicit(&mutex.state, 1, memory_order_relaxed);
    test_yield_count = 0;
    test_yield_condition = &condition;
    fifo_set_yield_callback(test_yield_signal_condition_callback);

    fifo_condition_wait(&condition, &mutex);

    fifo_set_yield_callback(NULL);
    test_yield_condition = NULL;

    ASSERT("condition_wait must call platform yield while waiting",
           test_yield_count > 0);
    ASSERT("condition_wait must observe sequence change",
           atomic_load_explicit(&condition.sequence,
                                memory_order_relaxed) == 8);
    ASSERT("condition_wait must return with mutex reacquired",
           atomic_load_explicit(&mutex.state, memory_order_relaxed) != 0);
    return 0;
}


/* fifo_init() */


static int test_individual_fifo_init_capacity_one(void)
{
    fifo_t fifo;
    void *storage[1];
    fifo.items = NULL;
    fifo.capacity = 999;
    fifo.head = 77;
    fifo.tail = 77;
    fifo.count = 77;

    fifo_init(&fifo, storage, 1);

    ASSERT("fifo_init must install supplied storage", fifo.items == storage);
    ASSERT("fifo_init must preserve supplied capacity", fifo.capacity == 1);
    ASSERT("fifo_init must reset head", fifo.head == 0);
    ASSERT("fifo_init must reset tail", fifo.tail == 0);
    ASSERT("fifo_init must reset count", fifo.count == 0);
    return 0;
}


static int test_individual_fifo_init_typical(void)
{
    fifo_t fifo;
    void *storage[8];
    fifo.items = NULL;
    fifo.capacity = 999;
    fifo.head = 77;
    fifo.tail = 77;
    fifo.count = 77;

    fifo_init(&fifo, storage, 8);

    ASSERT("fifo_init must install supplied storage", fifo.items == storage);
    ASSERT("fifo_init must preserve supplied capacity", fifo.capacity == 8);
    ASSERT("fifo_init must reset head", fifo.head == 0);
    ASSERT("fifo_init must reset tail", fifo.tail == 0);
    ASSERT("fifo_init must reset count", fifo.count == 0);
    return 0;
}


static int test_individual_fifo_init_odd_capacity(void)
{
    fifo_t fifo;
    void *storage[17];
    fifo.items = NULL;
    fifo.capacity = 999;
    fifo.head = 77;
    fifo.tail = 77;
    fifo.count = 77;

    fifo_init(&fifo, storage, 17);

    ASSERT("fifo_init must install supplied storage", fifo.items == storage);
    ASSERT("fifo_init must preserve supplied capacity", fifo.capacity == 17);
    ASSERT("fifo_init must reset head", fifo.head == 0);
    ASSERT("fifo_init must reset tail", fifo.tail == 0);
    ASSERT("fifo_init must reset count", fifo.count == 0);
    return 0;
}


static int test_individual_fifo_init_reinitialise_nonempty(void)
{
    fifo_t fifo;
    void *storage[5];
    fifo.items = NULL;
    fifo.capacity = 999;
    fifo.head = 3;
    fifo.tail = 3;
    fifo.count = 3;

    fifo_init(&fifo, storage, 5);

    ASSERT("fifo_init must install supplied storage", fifo.items == storage);
    ASSERT("fifo_init must preserve supplied capacity", fifo.capacity == 5);
    ASSERT("fifo_init must reset head", fifo.head == 0);
    ASSERT("fifo_init must reset tail", fifo.tail == 0);
    ASSERT("fifo_init must reset count", fifo.count == 0);
    return 0;
}


/* fifo_capacity() */


static int test_individual_fifo_capacity_empty(void)
{
    void *storage[6];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 6;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_capacity must return stored capacity",
           fifo_capacity(&fifo) == 6);
    return 0;
}


static int test_individual_fifo_capacity_partial(void)
{
    void *storage[6];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 6;
    fifo.head = 0;
    fifo.tail = 3;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_capacity must return stored capacity",
           fifo_capacity(&fifo) == 6);
    return 0;
}


static int test_individual_fifo_capacity_full(void)
{
    void *storage[6];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 6;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 6;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_capacity must return stored capacity",
           fifo_capacity(&fifo) == 6);
    return 0;
}


static int test_individual_fifo_capacity_wrapped(void)
{
    void *storage[6];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 6;
    fifo.head = 5;
    fifo.tail = 2;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_capacity must return stored capacity",
           fifo_capacity(&fifo) == 6);
    return 0;
}


/* fifo_count() */


static int test_individual_fifo_count_empty(void)
{
    void *storage[8];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;

    ASSERT("fifo_count must report zero for empty FIFO",
           fifo_count(&fifo) == 0);

    fifo.count = 5;

    ASSERT("same fifo_count implementation must report nonzero count",
           fifo_count(&fifo) == 5);
    return 0;
}


static int test_individual_fifo_count_partial(void)
{
    void *storage[8];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 0;
    fifo.tail = 3;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_count must return exact stored count",
           fifo_count(&fifo) == 3);
    return 0;
}


static int test_individual_fifo_count_full(void)
{
    void *storage[8];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 8;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_count must return exact stored count",
           fifo_count(&fifo) == 8);
    return 0;
}


static int test_individual_fifo_count_after_pop(void)
{
    void *storage[8];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 1;
    fifo.tail = 3;
    fifo.count = 2;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_count must return exact stored count",
           fifo_count(&fifo) == 2);
    return 0;
}


static int test_individual_fifo_count_failed_push(void)
{
    void *storage[8];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 4;
    fifo.tail = 4;
    fifo.count = 8;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_count must return exact stored count",
           fifo_count(&fifo) == 8);
    return 0;
}


static int test_individual_fifo_count_failed_pop(void)
{
    void *storage[8];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 4;
    fifo.tail = 4;
    fifo.count = 0;

    ASSERT("fifo_count must report preserved zero count",
           fifo_count(&fifo) == 0);

    fifo.count = 3;

    ASSERT("same fifo_count implementation must report nonzero state",
           fifo_count(&fifo) == 3);
    return 0;
}


static int test_individual_fifo_count_wrapped(void)
{
    void *storage[8];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 8;
    fifo.head = 6;
    fifo.tail = 3;
    fifo.count = 5;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_count must return exact stored count",
           fifo_count(&fifo) == 5);
    return 0;
}


/* fifo_empty() */


static int test_individual_fifo_empty_initial(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_empty must reflect count state",
           fifo_empty(&fifo) == true);
    return 0;
}


static int test_individual_fifo_empty_partial(void)
{
    void *storage[4];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 2;
    fifo.count = 2;

    ASSERT("fifo_empty must reject partial FIFO",
           !fifo_empty(&fifo));

    fifo.count = 0;
    fifo.tail = fifo.head;

    ASSERT("same fifo_empty implementation must recognise empty FIFO",
           fifo_empty(&fifo));
    return 0;
}


static int test_individual_fifo_empty_full(void)
{
    void *storage[4];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 4;

    ASSERT("fifo_empty must reject full FIFO",
           !fifo_empty(&fifo));

    fifo.count = 0;

    ASSERT("same fifo_empty implementation must recognise empty FIFO",
           fifo_empty(&fifo));
    return 0;
}


static int test_individual_fifo_empty_after_drain(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 3;
    fifo.tail = 3;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_empty must reflect count state",
           fifo_empty(&fifo) == true);
    return 0;
}


static int test_individual_fifo_empty_null_item(void)
{
    void *storage[4];
    fifo_t fifo;

    storage[0] = NULL;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;

    ASSERT("FIFO containing NULL item must not be empty",
           !fifo_empty(&fifo));

    fifo.count = 0;
    fifo.tail = fifo.head;

    ASSERT("same fifo_empty implementation must recognise empty FIFO",
           fifo_empty(&fifo));
    return 0;
}


/* fifo_full() */


static int test_individual_fifo_full_initial(void)
{
    void *storage[4];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;

    ASSERT("fifo_full must reject initial empty state",
           !fifo_full(&fifo));

    fifo.count = fifo.capacity;

    ASSERT("same fifo_full implementation must recognise full FIFO",
           fifo_full(&fifo));
    return 0;
}


static int test_individual_fifo_full_partial(void)
{
    void *storage[4];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 2;
    fifo.count = 2;

    ASSERT("fifo_full must reject partial state",
           !fifo_full(&fifo));

    fifo.count = fifo.capacity;
    fifo.tail = fifo.head;

    ASSERT("same fifo_full implementation must recognise full FIFO",
           fifo_full(&fifo));
    return 0;
}


static int test_individual_fifo_full_exact(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 4;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_full must reflect count/capacity state",
           fifo_full(&fifo) == true);
    return 0;
}


static int test_individual_fifo_full_after_failed_push(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 2;
    fifo.count = 4;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_full must reflect count/capacity state",
           fifo_full(&fifo) == true);
    return 0;
}


static int test_individual_fifo_full_after_pop(void)
{
    void *storage[4];
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 1;
    fifo.tail = 0;
    fifo.count = 3;

    ASSERT("fifo_full must reject state below capacity",
           !fifo_full(&fifo));

    fifo.count = fifo.capacity;

    ASSERT("same fifo_full implementation must recognise full FIFO",
           fifo_full(&fifo));
    return 0;
}


static int test_individual_fifo_full_capacity_one(void)
{
    void *storage[1];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 1;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_full must reflect count/capacity state",
           fifo_full(&fifo) == true);
    return 0;
}


static int test_individual_fifo_full_after_wrap_refill(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 2;
    fifo.count = 4;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    ASSERT("fifo_full must reflect count/capacity state",
           fifo_full(&fifo) == true);
    return 0;
}


/* fifo_push() */


static int test_individual_fifo_push_empty(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    bool result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push return value must match available capacity",
           result == true);
    ASSERT("fifo_push must leave exact count", fifo.count == 1);
    ASSERT("fifo_push must leave exact tail", fifo.tail == 1);
    ASSERT("fifo_push must store item in previous tail slot",
           storage[0] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_partial(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 2;
    fifo.count = 2;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    bool result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push return value must match available capacity",
           result == true);
    ASSERT("fifo_push must leave exact count", fifo.count == 3);
    ASSERT("fifo_push must leave exact tail", fifo.tail == 3);
    ASSERT("fifo_push must store item in previous tail slot",
           storage[2] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_exact_capacity(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 3;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    bool result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push return value must match available capacity",
           result == true);
    ASSERT("fifo_push must leave exact count", fifo.count == 4);
    ASSERT("fifo_push must leave exact tail", fifo.tail == 0);
    ASSERT("fifo_push must store item in previous tail slot",
           storage[3] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_full_failure(void)
{
    void *storage[4];
    fifo_t fifo;
    bool result;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 1;
    fifo.tail = 1;
    fifo.count = 4;
    storage[1] = (void *)(uintptr_t)777;

    result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("push on full FIFO must fail", !result);
    ASSERT("failed push must preserve count", fifo.count == 4);
    ASSERT("failed push must preserve tail", fifo.tail == 1);
    ASSERT("failed push must not overwrite storage",
           storage[1] == (void *)(uintptr_t)777);

    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    storage[0] = (void *)(uintptr_t)777;

    result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("same fifo_push implementation must succeed with free space",
           result);
    ASSERT("successful push must increment count", fifo.count == 1);
    ASSERT("successful push must advance tail", fifo.tail == 1);
    ASSERT("successful push must store item",
           storage[0] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_null(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    bool result = fifo_push(&fifo, NULL);

    ASSERT("fifo_push return value must match available capacity",
           result == true);
    ASSERT("fifo_push must leave exact count", fifo.count == 1);
    ASSERT("fifo_push must leave exact tail", fifo.tail == 1);
    ASSERT("fifo_push must store item in previous tail slot",
           storage[0] == NULL);
    return 0;
}


static int test_individual_fifo_push_after_pop_wrap(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 1;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    bool result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push return value must match available capacity",
           result == true);
    ASSERT("fifo_push must leave exact count", fifo.count == 4);
    ASSERT("fifo_push must leave exact tail", fifo.tail == 2);
    ASSERT("fifo_push must store item in previous tail slot",
           storage[1] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_capacity_one(void)
{
    void *storage[1];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 1;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    bool result = fifo_push(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push return value must match available capacity",
           result == true);
    ASSERT("fifo_push must leave exact count", fifo.count == 1);
    ASSERT("fifo_push must leave exact tail", fifo.tail == 0);
    ASSERT("fifo_push must store item in previous tail slot",
           storage[0] == (void *)(uintptr_t)123);
    return 0;
}


/* fifo_pop() */


static int test_individual_fifo_pop_empty_failure(void)
{
    void *storage[4];
    fifo_t fifo;
    void *item;
    bool result;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 2;
    fifo.count = 0;
    item = (void *)(uintptr_t)0xdead;

    result = fifo_pop(&fifo, &item);

    ASSERT("pop on empty FIFO must fail", !result);
    ASSERT("failed pop must preserve count", fifo.count == 0);
    ASSERT("failed pop must preserve head", fifo.head == 2);
    ASSERT("failed pop must not modify output",
           item == (void *)(uintptr_t)0xdead);

    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    storage[0] = (void *)(uintptr_t)123;
    item = NULL;

    result = fifo_pop(&fifo, &item);

    ASSERT("same fifo_pop implementation must succeed when occupied",
           result);
    ASSERT("successful pop must return item",
           item == (void *)(uintptr_t)123);
    ASSERT("successful pop must decrement count", fifo.count == 0);
    ASSERT("successful pop must advance head", fifo.head == 1);
    return 0;
}


static int test_individual_fifo_pop_single(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;

    bool result = fifo_pop(&fifo, &item);

    ASSERT("fifo_pop return value must match occupancy",
           result == true);
    ASSERT("fifo_pop must leave exact count", fifo.count == 0);
    ASSERT("fifo_pop must leave exact head", fifo.head == 1);
    ASSERT("fifo_pop must return head item", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_pop_partial(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 1;
    fifo.tail = 0;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[1] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;

    bool result = fifo_pop(&fifo, &item);

    ASSERT("fifo_pop return value must match occupancy",
           result == true);
    ASSERT("fifo_pop must leave exact count", fifo.count == 2);
    ASSERT("fifo_pop must leave exact head", fifo.head == 2);
    ASSERT("fifo_pop must return head item", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_pop_full(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 4;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;

    bool result = fifo_pop(&fifo, &item);

    ASSERT("fifo_pop return value must match occupancy",
           result == true);
    ASSERT("fifo_pop must leave exact count", fifo.count == 3);
    ASSERT("fifo_pop must leave exact head", fifo.head == 1);
    ASSERT("fifo_pop must return head item", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_pop_null(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = NULL;
    void *item = (void *)(uintptr_t)0xdead;

    bool result = fifo_pop(&fifo, &item);

    ASSERT("fifo_pop return value must match occupancy",
           result == true);
    ASSERT("fifo_pop must leave exact count", fifo.count == 0);
    ASSERT("fifo_pop must leave exact head", fifo.head == 1);
    ASSERT("fifo_pop must return head item", item == NULL);
    return 0;
}


static int test_individual_fifo_pop_wrapped(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 3;
    fifo.tail = 2;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[3] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;

    bool result = fifo_pop(&fifo, &item);

    ASSERT("fifo_pop return value must match occupancy",
           result == true);
    ASSERT("fifo_pop must leave exact count", fifo.count == 2);
    ASSERT("fifo_pop must leave exact head", fifo.head == 0);
    ASSERT("fifo_pop must return head item", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_pop_capacity_one_reuse(void)
{
    void *storage[1];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 1;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;

    bool result = fifo_pop(&fifo, &item);

    ASSERT("fifo_pop return value must match occupancy",
           result == true);
    ASSERT("fifo_pop must leave exact count", fifo.count == 0);
    ASSERT("fifo_pop must leave exact head", fifo.head == 0);
    ASSERT("fifo_pop must return head item", item == (void *)(uintptr_t)123);
    return 0;
}


/* fifo_peek() */


static int test_individual_fifo_peek_empty_failure(void)
{
    void *storage[4];
    fifo_t fifo;
    void *item;
    bool result;

    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 2;
    fifo.count = 0;
    item = (void *)(uintptr_t)0xdead;

    result = fifo_peek(&fifo, &item);

    ASSERT("peek on empty FIFO must fail", !result);
    ASSERT("failed peek must preserve count", fifo.count == 0);
    ASSERT("failed peek must preserve head", fifo.head == 2);
    ASSERT("failed peek must preserve tail", fifo.tail == 2);
    ASSERT("failed peek must not modify output",
           item == (void *)(uintptr_t)0xdead);

    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    storage[0] = (void *)(uintptr_t)123;
    item = NULL;

    result = fifo_peek(&fifo, &item);

    ASSERT("same fifo_peek implementation must succeed when occupied",
           result);
    ASSERT("successful peek must return item",
           item == (void *)(uintptr_t)123);
    ASSERT("successful peek must preserve count", fifo.count == 1);
    ASSERT("successful peek must preserve head", fifo.head == 0);
    ASSERT("successful peek must preserve tail", fifo.tail == 1);
    return 0;
}


static int test_individual_fifo_peek_single(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;
    size_t old_head = fifo.head;
    size_t old_tail = fifo.tail;
    size_t old_count = fifo.count;

    bool result = fifo_peek(&fifo, &item);

    ASSERT("fifo_peek return value must match occupancy",
           result == true);
    ASSERT("fifo_peek must not change head", fifo.head == old_head);
    ASSERT("fifo_peek must not change tail", fifo.tail == old_tail);
    ASSERT("fifo_peek must not change count", fifo.count == old_count);
    ASSERT("fifo_peek must leave expected output value", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_peek_partial(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 1;
    fifo.tail = 0;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[1] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;
    size_t old_head = fifo.head;
    size_t old_tail = fifo.tail;
    size_t old_count = fifo.count;

    bool result = fifo_peek(&fifo, &item);

    ASSERT("fifo_peek return value must match occupancy",
           result == true);
    ASSERT("fifo_peek must not change head", fifo.head == old_head);
    ASSERT("fifo_peek must not change tail", fifo.tail == old_tail);
    ASSERT("fifo_peek must not change count", fifo.count == old_count);
    ASSERT("fifo_peek must leave expected output value", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_peek_full(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 4;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;
    size_t old_head = fifo.head;
    size_t old_tail = fifo.tail;
    size_t old_count = fifo.count;

    bool result = fifo_peek(&fifo, &item);

    ASSERT("fifo_peek return value must match occupancy",
           result == true);
    ASSERT("fifo_peek must not change head", fifo.head == old_head);
    ASSERT("fifo_peek must not change tail", fifo.tail == old_tail);
    ASSERT("fifo_peek must not change count", fifo.count == old_count);
    ASSERT("fifo_peek must leave expected output value", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_peek_null(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = NULL;
    void *item = (void *)(uintptr_t)0xdead;
    size_t old_head = fifo.head;
    size_t old_tail = fifo.tail;
    size_t old_count = fifo.count;

    bool result = fifo_peek(&fifo, &item);

    ASSERT("fifo_peek return value must match occupancy",
           result == true);
    ASSERT("fifo_peek must not change head", fifo.head == old_head);
    ASSERT("fifo_peek must not change tail", fifo.tail == old_tail);
    ASSERT("fifo_peek must not change count", fifo.count == old_count);
    ASSERT("fifo_peek must leave expected output value", item == NULL);
    return 0;
}


static int test_individual_fifo_peek_wrapped(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 3;
    fifo.tail = 2;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[3] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;
    size_t old_head = fifo.head;
    size_t old_tail = fifo.tail;
    size_t old_count = fifo.count;

    bool result = fifo_peek(&fifo, &item);

    ASSERT("fifo_peek return value must match occupancy",
           result == true);
    ASSERT("fifo_peek must not change head", fifo.head == old_head);
    ASSERT("fifo_peek must not change tail", fifo.tail == old_tail);
    ASSERT("fifo_peek must not change count", fifo.count == old_count);
    ASSERT("fifo_peek must leave expected output value", item == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_peek_repeated(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 0;
    fifo.count = 2;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[2] = (void *)(uintptr_t)123;
    void *item = (void *)(uintptr_t)0xdead;
    size_t old_head = fifo.head;
    size_t old_tail = fifo.tail;
    size_t old_count = fifo.count;

    bool result = fifo_peek(&fifo, &item);

    ASSERT("fifo_peek return value must match occupancy",
           result == true);
    ASSERT("fifo_peek must not change head", fifo.head == old_head);
    ASSERT("fifo_peek must not change tail", fifo.tail == old_tail);
    ASSERT("fifo_peek must not change count", fifo.count == old_count);
    ASSERT("fifo_peek must leave expected output value", item == (void *)(uintptr_t)123);
    return 0;
}


/* fifo_push_wait() */


static int test_individual_fifo_push_wait_empty(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    fifo_push_wait(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push_wait must increment count", fifo.count == 1);
    ASSERT("fifo_push_wait must advance tail correctly", fifo.tail == 1);
    ASSERT("fifo_push_wait must store item", storage[0] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_wait_partial(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 2;
    fifo.count = 2;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    fifo_push_wait(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push_wait must increment count", fifo.count == 3);
    ASSERT("fifo_push_wait must advance tail correctly", fifo.tail == 3);
    ASSERT("fifo_push_wait must store item", storage[2] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_wait_last_slot(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 3;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    fifo_push_wait(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push_wait must increment count", fifo.count == 4);
    ASSERT("fifo_push_wait must advance tail correctly", fifo.tail == 0);
    ASSERT("fifo_push_wait must store item", storage[3] == (void *)(uintptr_t)123);
    return 0;
}


static int test_individual_fifo_push_wait_null(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    fifo_push_wait(&fifo, NULL);

    ASSERT("fifo_push_wait must increment count", fifo.count == 1);
    ASSERT("fifo_push_wait must advance tail correctly", fifo.tail == 1);
    ASSERT("fifo_push_wait must store item", storage[0] == NULL);
    return 0;
}


static int test_individual_fifo_push_wait_wrapped_space(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 2;
    fifo.tail = 1;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    fifo_push_wait(&fifo, (void *)(uintptr_t)123);

    ASSERT("fifo_push_wait must increment count", fifo.count == 4);
    ASSERT("fifo_push_wait must advance tail correctly", fifo.tail == 2);
    ASSERT("fifo_push_wait must store item", storage[1] == (void *)(uintptr_t)123);
    return 0;
}


/* fifo_pop_wait() */


static int test_individual_fifo_pop_wait_single(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;

    void *item = fifo_pop_wait(&fifo);

    ASSERT("fifo_pop_wait must return head item", item == (void *)(uintptr_t)123);
    ASSERT("fifo_pop_wait must decrement count", fifo.count == 0);
    ASSERT("fifo_pop_wait must advance head correctly", fifo.head == 1);
    return 0;
}


static int test_individual_fifo_pop_wait_partial(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 1;
    fifo.tail = 0;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[1] = (void *)(uintptr_t)123;

    void *item = fifo_pop_wait(&fifo);

    ASSERT("fifo_pop_wait must return head item", item == (void *)(uintptr_t)123);
    ASSERT("fifo_pop_wait must decrement count", fifo.count == 2);
    ASSERT("fifo_pop_wait must advance head correctly", fifo.head == 2);
    return 0;
}


static int test_individual_fifo_pop_wait_full(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 4;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = (void *)(uintptr_t)123;

    void *item = fifo_pop_wait(&fifo);

    ASSERT("fifo_pop_wait must return head item", item == (void *)(uintptr_t)123);
    ASSERT("fifo_pop_wait must decrement count", fifo.count == 3);
    ASSERT("fifo_pop_wait must advance head correctly", fifo.head == 1);
    return 0;
}


static int test_individual_fifo_pop_wait_null(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 0;
    fifo.tail = 1;
    fifo.count = 1;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[0] = NULL;

    void *item = fifo_pop_wait(&fifo);

    ASSERT("fifo_pop_wait must return head item", item == NULL);
    ASSERT("fifo_pop_wait must decrement count", fifo.count == 0);
    ASSERT("fifo_pop_wait must advance head correctly", fifo.head == 1);
    return 0;
}


static int test_individual_fifo_pop_wait_wrapped(void)
{
    void *storage[4];
    fifo_t fifo;
    fifo.items = storage;
    fifo.capacity = 4;
    fifo.head = 3;
    fifo.tail = 2;
    fifo.count = 3;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);
    storage[3] = (void *)(uintptr_t)123;

    void *item = fifo_pop_wait(&fifo);

    ASSERT("fifo_pop_wait must return head item", item == (void *)(uintptr_t)123);
    ASSERT("fifo_pop_wait must decrement count", fifo.count == 2);
    ASSERT("fifo_pop_wait must advance head correctly", fifo.head == 0);
    return 0;
}



static int test_individual_fifo_push_wait_full_yields(void)
{
    void *storage[2] = {
        (void *)(uintptr_t)1,
        (void *)(uintptr_t)2
    };
    fifo_t fifo;

    fifo.items = storage;
    fifo.capacity = 2;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 2;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    test_yield_count = 0;
    test_yield_fifo = &fifo;
    fifo_set_yield_callback(test_yield_make_fifo_writable_callback);

    fifo_push_wait(&fifo, (void *)(uintptr_t)3);

    fifo_set_yield_callback(NULL);
    test_yield_fifo = NULL;

    ASSERT("fifo_push_wait on full FIFO must call platform yield",
           test_yield_count > 0);
    ASSERT("fifo_push_wait must leave FIFO full after inserting new item",
           fifo.count == 2);
    ASSERT("fifo_push_wait must store new item after space becomes available",
           storage[0] == (void *)(uintptr_t)3);
    return 0;
}


static int test_individual_fifo_pop_wait_empty_yields(void)
{
    void *storage[2] = { NULL, NULL };
    fifo_t fifo;
    void *item;

    fifo.items = storage;
    fifo.capacity = 2;
    fifo.head = 0;
    fifo.tail = 0;
    fifo.count = 0;
    atomic_store_explicit(&fifo.lock.state, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.readable.sequence, 0, memory_order_relaxed);
    atomic_store_explicit(&fifo.writable.sequence, 0, memory_order_relaxed);

    test_yield_count = 0;
    test_yield_fifo = &fifo;
    test_yield_fifo_item = (void *)(uintptr_t)123;
    fifo_set_yield_callback(test_yield_make_fifo_readable_callback);

    item = fifo_pop_wait(&fifo);

    fifo_set_yield_callback(NULL);
    test_yield_fifo = NULL;
    test_yield_fifo_item = NULL;

    ASSERT("fifo_pop_wait on empty FIFO must call platform yield",
           test_yield_count > 0);
    ASSERT("fifo_pop_wait must return item made available while yielding",
           item == (void *)(uintptr_t)123);
    ASSERT("fifo_pop_wait must consume item made available while yielding",
           fifo.count == 0);
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

    fprintf(stdout, "Platform yield and blocking-path integration\n");
    fprintf(stdout, "----------------------------------------------------------------------\n");
    TEST("platform yield invokes installed callback",          test_platform_yield_invokes_callback)
    TEST("platform yield invokes callback repeatedly",         test_platform_yield_repeated_callback)
    TEST("platform yield replaces callback",                   test_platform_yield_replaces_callback)
    TEST("platform yield clears callback",                     test_platform_yield_clear_callback)
    TEST("contended mutex_lock calls platform yield",          test_individual_mutex_lock_contended_yields)
    TEST("zero semaphore_wait calls platform yield",           test_individual_semaphore_wait_zero_yields)
    TEST("condition_wait calls platform yield",                test_individual_condition_wait_yields)
    TEST("full fifo_push_wait calls platform yield",           test_individual_fifo_push_wait_full_yields)
    TEST("empty fifo_pop_wait calls platform yield",           test_individual_fifo_pop_wait_empty_yields)

    fprintf(stdout, "\nSpinlocks\n");
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
