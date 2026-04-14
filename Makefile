# ── OS Scheduler Makefile ─────────────────────────────────────────────────────
# Build:  make
# Run:    make run
# Clean:  make clean
# ─────────────────────────────────────────────────────────────────────────────

CC     = gcc
CFLAGS = -Wall -Wextra -g
LIBS   = -lm

BINS = process_generator.out clk.out scheduler.out process.out test_generator.out

.PHONY: build clean all run

build: $(BINS)

process_generator.out: process_generator.c headers.h shared.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

clk.out: clk.c headers.h
	$(CC) $(CFLAGS) $< -o $@

scheduler.out: scheduler.c headers.h shared.h
	$(CC) $(CFLAGS) $< -o $@ $(LIBS)

process.out: process.c headers.h
	$(CC) $(CFLAGS) $< -o $@

test_generator.out: test_generator.c
	$(CC) $(CFLAGS) $< -o $@

clean:
	rm -f $(BINS) processes.txt scheduler*.log scheduler*.perf

all: clean build
	./test_generator.out

run:
	./process_generator.out