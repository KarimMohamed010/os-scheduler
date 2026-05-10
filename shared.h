#ifndef SHARED_H
#define SHARED_H

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>

#define MSG_KEY          1234
#define SHM_KEY_CLK      4321
#define TICK_SYNC_SEM_KEY 1235

#define FCFS2_CTRL_SHM_KEY       2234
#define FCFS2_CONSUME_SEM_KEY    2235
#define FCFS2_ACK_SEM_KEY        2236
#define FCFS2_STEAL_SEM_KEY      2237
#define FCFS2_CTRL_MUTEX_SEM_KEY 2238
#define FCFS2_TICK_DONE_SEM_KEY  2239
#define FCFS2_START_SEM_KEY      2240
#define FCFS2_CHILD_TICK_SEM_KEY 2241
#define FCFS2_CHILD_TICK_ACK_SEM_KEY 2242
#define FCFS2_STEAL_OVERHEAD_SEC 3

#define PROC_SYNC_SEM_KEY 5433
#define PROC_REQ_MQ_KEY   5434
#define PROC_ACK_MQ_KEY   5435

#define ALGO_HPF    1
#define ALGO_RR     2
#define ALGO_FCFS_2 3

#define MAX_PROCESSES 100
#define MAX_REQUESTS  64   /* max memory requests per process (Phase 2) */

#define PROC_READY   0
#define PROC_RUNNING 1
#define PROC_BLOCKED 2  /* blocked waiting for disk I/O */

typedef struct
{
    int time;
    int va;
    char va_str[11];
    int is_write;
} MemRequest;

#define MSG_COMPUTE 1
#define MSG_MEM_REQ 2

typedef struct {
    long mtype;       // process id
    int msg_type;     // MSG_COMPUTE or MSG_MEM_REQ
    int va;
    int is_write;
    char va_str[16];
} ProcReqMsg;

typedef struct {
    long mtype;       // process id
    int fault;        // 1 if page fault occurred, 0 otherwise
} ProcAckMsg;
typedef struct
{
    int id;
    int arrival;
    int runtime;
    int priority;

    int remaining;
    int waiting;
    int start_time;
    int finish_time;

    pid_t pid;

    int started;
    int finished;

    int base;
    int limit;
    int page_table_frame;
    int cpu_ticks_consumed;
    int state;
    int phase2_enabled;
} PCB;

typedef struct
{
    int next_req_idx;
    MemRequest requests[MAX_REQUESTS];
    int num_requests;
} ProcessRequests;

typedef struct
{
    long mtype;
    PCB  proc;
} Message;

typedef struct QNode
{
    PCB         pcb;
    struct QNode *prev;
    struct QNode *next;
} QNode;

typedef struct
{
    QNode *head;
    QNode *tail;
    int    size;
} ReadyQueue;

typedef struct
{
    int queue_size[2];
    int ready_remaining[2];
    int running_remaining[2];

    int consume_turn;
    int all_received;
    int shutdown;
    int child_ready[2];
    int child_done[2];
    int penalty_until;
    int current_tick;

    int steal_pending;
    int steal_from;
    int steal_to;
    int has_stolen;
    PCB stolen_proc;
} FCFS2Control;


static inline ReadyQueue *queue_create(void)
{
    ReadyQueue *q = (ReadyQueue *)malloc(sizeof(ReadyQueue));
    if (!q) { perror("malloc queue"); exit(1); }
    q->head = q->tail = NULL;
    q->size = 0;
    return q;
}

static inline void queue_push_tail(ReadyQueue *q, PCB pcb)
{
    QNode *n = (QNode *)malloc(sizeof(QNode));
    if (!n) { perror("malloc QNode"); exit(1); }
    n->pcb  = pcb;
    n->prev = q->tail;
    n->next = NULL;
    if (q->tail) q->tail->next = n; else q->head = n;
    q->tail = n;
    q->size++;
}

static inline void queue_insert_sorted(ReadyQueue *q, PCB pcb)
{
    QNode *n = (QNode *)malloc(sizeof(QNode));
    if (!n) { perror("malloc QNode"); exit(1); }
    n->pcb = pcb;

    QNode *cur = q->head;
    while (cur)
    {
        if (pcb.priority < cur->pcb.priority) break;
        if (pcb.priority == cur->pcb.priority && pcb.arrival < cur->pcb.arrival) break;
        if (pcb.priority == cur->pcb.priority && pcb.arrival == cur->pcb.arrival
            && pcb.id < cur->pcb.id) break;
        cur = cur->next;
    }

    if (!cur)
    {
        n->prev = q->tail; n->next = NULL;
        if (q->tail) q->tail->next = n; else q->head = n;
        q->tail = n;
    }
    else
    {
        n->next = cur; n->prev = cur->prev;
        if (cur->prev) cur->prev->next = n; else q->head = n;
        cur->prev = n;
    }
    q->size++;
}

static inline int queue_pop(ReadyQueue *q, PCB *out)
{
    if (!q->head) return 0;
    QNode *n = q->head;
    *out   = n->pcb;
    q->head = n->next;
    if (q->head) q->head->prev = NULL; else q->tail = NULL;
    free(n);
    q->size--;
    return 1;
}

static inline int queue_steal_tail(ReadyQueue *q, PCB *out)
{
    if (!q->tail) return 0;
    QNode *n = q->tail;
    *out   = n->pcb;
    q->tail = n->prev;
    if (q->tail) q->tail->next = NULL; else q->head = NULL;
    free(n);
    q->size--;
    return 1;
}

static inline PCB *queue_peek(ReadyQueue *q)
{
    return q->head ? &q->head->pcb : NULL;
}

static inline int queue_total_remaining(ReadyQueue *q)
{
    int total = 0;
    QNode *cur = q->head;
    while (cur) { total += cur->pcb.remaining; cur = cur->next; }
    return total;
}

typedef short bool;
#define true  1
#define false 0

#define SHKEY 300

#endif /* SHARED_H */
