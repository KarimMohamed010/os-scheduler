#ifndef FCFS_H
#define FCFS_H

#include "scheduler_ops.h"

typedef struct
{
    ReadyQueue *ready_queue;
} FCFSState;

static inline void fcfs_init(FCFSState *state)
{
    state->ready_queue = queue_create();
}

static inline void fcfs_enqueue(FCFSState *state, PCB proc)
{
    queue_push_tail(state->ready_queue, proc);
}

static inline int fcfs_dequeue(FCFSState *state, PCB *out)
{
    return queue_pop(state->ready_queue, out);
}

static inline int fcfs_steal_tail(FCFSState *state, PCB *out)
{
    return queue_steal_tail(state->ready_queue, out);
}

static inline int fcfs_has_ready(const FCFSState *state)
{
    return state->ready_queue && state->ready_queue->size > 0;
}

static inline int fcfs_should_preempt(const FCFSState *state, const PCB *running)
{
    (void)state;
    (void)running;
    return 0;
}

static inline void fcfs_ops_init(void *state, int argc, char *argv[])
{
    FCFSState *fcfs_state = (FCFSState *)state;
    (void)argc;
    (void)argv;
    fcfs_init(fcfs_state);
}

static inline void fcfs_ops_enqueue(void *state, PCB proc)
{
    fcfs_enqueue((FCFSState *)state, proc);
}

static inline int fcfs_ops_dequeue(void *state, PCB *out)
{
    return fcfs_dequeue((FCFSState *)state, out);
}

static inline int fcfs_ops_has_ready(void *state)
{
    return fcfs_has_ready((FCFSState *)state);
}

static inline int fcfs_ops_should_preempt(void *state, const PCB *running)
{
    return fcfs_should_preempt((FCFSState *)state, running);
}

static inline void fcfs_ops_on_dispatch(void *state)
{
    (void)state;
}

static inline void fcfs_ops_on_tick(void *state)
{
    (void)state;
}

static inline ReadyQueue *fcfs_ops_get_ready_queue(void *state)
{
    return ((FCFSState *)state)->ready_queue;
}

static inline void fcfs_bind_ops(SchedulerOps *ops)
{
    ops->init = fcfs_ops_init;
    ops->enqueue = fcfs_ops_enqueue;
    ops->dequeue = fcfs_ops_dequeue;
    ops->has_ready = fcfs_ops_has_ready;
    ops->should_preempt = fcfs_ops_should_preempt;
    ops->on_dispatch = fcfs_ops_on_dispatch;
    ops->on_tick = fcfs_ops_on_tick;
    ops->get_ready_queue = fcfs_ops_get_ready_queue;
}

#endif /* FCFS_H */