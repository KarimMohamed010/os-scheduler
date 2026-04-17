#ifndef HPF_H
#define HPF_H

#include "scheduler_ops.h"

typedef struct
{
    ReadyQueue *ready_queue;
} HPFState;

static inline void hpf_init(HPFState *state)
{
    state->ready_queue = queue_create();
}

static inline void hpf_enqueue(HPFState *state, PCB proc)
{
    queue_insert_sorted(state->ready_queue, proc);
}

static inline int hpf_dequeue(HPFState *state, PCB *out)
{
    return queue_pop(state->ready_queue, out);
}

static inline int hpf_has_ready(const HPFState *state)
{
    return state->ready_queue && state->ready_queue->size > 0;
}

static inline int hpf_should_preempt(const HPFState *state, const PCB *running)
{
    PCB *candidate;

    if (!running || running->id == -1 || running->remaining <= 0)
    {
        return 0;
    }

    candidate = queue_peek(state->ready_queue);
    if (!candidate)
    {
        return 0;
    }

    return candidate->priority < running->priority;
}

static inline void hpf_ops_init(void *state, int argc, char *argv[])
{
    HPFState *hpf_state = (HPFState *)state;
    (void)argc;
    (void)argv;

    hpf_init(hpf_state);
}

static inline void hpf_ops_enqueue(void *state, PCB proc)
{
    hpf_enqueue((HPFState *)state, proc);
}

static inline int hpf_ops_dequeue(void *state, PCB *out)
{
    return hpf_dequeue((HPFState *)state, out);
}

static inline int hpf_ops_has_ready(void *state)
{
    return hpf_has_ready((HPFState *)state);
}

static inline int hpf_ops_should_preempt(void *state, const PCB *running)
{
    return hpf_should_preempt((HPFState *)state, running);
}

static inline void hpf_ops_on_dispatch(void *state)
{
    (void)state;
}

static inline void hpf_ops_on_tick(void *state)
{
    (void)state;
}

static inline ReadyQueue *hpf_ops_get_ready_queue(void *state)
{
    return ((HPFState *)state)->ready_queue;
}

static inline void hpf_bind_ops(SchedulerOps *ops)
{
    ops->init = hpf_ops_init;
    ops->enqueue = hpf_ops_enqueue;
    ops->dequeue = hpf_ops_dequeue;
    ops->has_ready = hpf_ops_has_ready;
    ops->should_preempt = hpf_ops_should_preempt;
    ops->on_dispatch = hpf_ops_on_dispatch;
    ops->on_tick = hpf_ops_on_tick;
    ops->get_ready_queue = hpf_ops_get_ready_queue;
}

#endif
