#ifndef RR_H
#define RR_H

#include "scheduler_ops.h"
#include <stdlib.h>

typedef struct
{
   ReadyQueue *ready_queue;
   int quantum;
   int slice_used;
} RRState;

static inline void rr_init(RRState *state, int quantum)
{
   state->ready_queue = queue_create();
   state->quantum = (quantum > 0) ? quantum : 1;
   state->slice_used = 0;
}

static inline void rr_enqueue(RRState *state, PCB proc)
{
   queue_push_tail(state->ready_queue, proc);
}

static inline int rr_dequeue(RRState *state, PCB *out)
{
   return queue_pop(state->ready_queue, out);
}

static inline int rr_has_ready(const RRState *state)
{
   return state->ready_queue && state->ready_queue->size > 0;
}

static inline void rr_on_dispatch(RRState *state)
{
   state->slice_used = 0;
}

static inline void rr_on_tick(RRState *state)
{
   state->slice_used++;
}

static inline int rr_should_preempt(RRState *state, const PCB *running)
{
   if (!running || running->id == -1 || running->remaining <= 0)
   {
      return 0;
   }

   if (state->slice_used < state->quantum)
   {
      return 0;
   }

   if (!rr_has_ready(state))
   {
      state->slice_used = 0;
      return 0;
   }

   return 1;
}

static inline void rr_ops_init(void *state, int argc, char *argv[])
{
   RRState *rr_state = (RRState *)state;
   int quantum;

   quantum = 1;
   if (argc > 2)
   {
      quantum = atoi(argv[2]);
   }
   rr_init(rr_state, quantum);
}

static inline void rr_ops_enqueue(void *state, PCB proc)
{
   rr_enqueue((RRState *)state, proc);
}

static inline int rr_ops_dequeue(void *state, PCB *out)
{
   return rr_dequeue((RRState *)state, out);
}

static inline int rr_ops_has_ready(void *state)
{
   return rr_has_ready((RRState *)state);
}

static inline int rr_ops_should_preempt(void *state, const PCB *running)
{
   return rr_should_preempt((RRState *)state, running);
}

static inline void rr_ops_on_dispatch(void *state)
{
   rr_on_dispatch((RRState *)state);
}

static inline void rr_ops_on_tick(void *state)
{
   rr_on_tick((RRState *)state);
}

static inline ReadyQueue *rr_ops_get_ready_queue(void *state)
{
   return ((RRState *)state)->ready_queue;
}

static inline void rr_bind_ops(SchedulerOps *ops)
{
   ops->init = rr_ops_init;
   ops->enqueue = rr_ops_enqueue;
   ops->dequeue = rr_ops_dequeue;
   ops->has_ready = rr_ops_has_ready;
   ops->should_preempt = rr_ops_should_preempt;
   ops->on_dispatch = rr_ops_on_dispatch;
   ops->on_tick = rr_ops_on_tick;
   ops->get_ready_queue = rr_ops_get_ready_queue;
}

#endif
