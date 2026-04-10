#ifndef SCHEDULER_OPS_H
#define SCHEDULER_OPS_H

#include "shared.h"

typedef struct
{
    void (*init)(void *state, int argc, char *argv[]);
    void (*enqueue)(void *state, PCB proc);
    int (*dequeue)(void *state, PCB *out);
    int (*has_ready)(void *state);
    int (*should_preempt)(void *state, const PCB *running);
    void (*on_dispatch)(void *state);
    void (*on_tick)(void *state);
    ReadyQueue *(*get_ready_queue)(void *state);
} SchedulerOps;

#endif
