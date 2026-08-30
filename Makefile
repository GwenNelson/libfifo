CC       := cc
AR       := ar

CFLAGS   := -std=c11 -Wall -Wextra -Wpedantic -O2
CPPFLAGS := -Iinclude

BUILD    := build

FIFO_SRC := $(wildcard src/fifo/*.c)
FIFO_OBJ := $(patsubst src/fifo/%.c,$(BUILD)/fifo/%.o,$(FIFO_SRC))

LIBFIFO  := $(BUILD)/libfifo.a
TEST     := $(BUILD)/test-fifo
TEST_GDB := $(BUILD)/test-fifo-gdb

.PHONY: all test test-gdb clean

all: $(LIBFIFO) $(TEST)

$(LIBFIFO): $(FIFO_OBJ)
	@mkdir -p $(@D)
	$(AR) rcs $@ $^

$(BUILD)/fifo/%.o: src/fifo/%.c
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $< -o $@

$(TEST): tests/test-fifo.c $(LIBFIFO)
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) -pthread $(CFLAGS) $< $(LIBFIFO) -o $@

$(TEST_GDB): tests/test-fifo.c $(LIBFIFO)
	@mkdir -p $(@D)
	$(CC) $(CPPFLAGS) -pthread -DTEST_GDB -O0 -g3 \
		-Wall -Wextra -Wpedantic \
		$< $(LIBFIFO) -o $@

test: $(TEST)
	./$(TEST)

test-gdb: $(TEST_GDB)
	gdb ./$(TEST_GDB)

clean:
	rm -rf $(BUILD)
