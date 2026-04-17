#ifndef SHARED_H
#define SHARED_H

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>

#define MSG_KEY 1234     /* generator  →  scheduler message queue  */
#define SHM_KEY_CLK 4321 /* clock shared memory  (used by clk.c)   */
#define TICK_SYNC_SEM_KEY 1235

/* FCFS-2 master/child coordination IPC keys */
#define FCFS2_CTRL_SHM_KEY 2234
#define FCFS2_CONSUME_SEM_KEY 2235
#define FCFS2_ACK_SEM_KEY 2236
#define FCFS2_STEAL_SEM_KEY 2237
#define FCFS2_CTRL_MUTEX_SEM_KEY 2238
#define FCFS2_TICK_DONE_SEM_KEY 2239
#define FCFS2_START_SEM_KEY 2240
#define FCFS2_CHILD_TICK_SEM_KEY 2241
#define FCFS2_CHILD_TICK_ACK_SEM_KEY 2242

#define FCFS2_STEAL_OVERHEAD_SEC 3

#define ALGO_HPF 1    /* Preemptive Highest Priority First */
#define ALGO_RR 2     /* Round Robin                        */
#define ALGO_FCFS_2 3 /* 2-CPU FCFS with work stealing      */

#define MAX_PROCESSES 100 /* Maximum number of processes, added by Youssef */

/* =========================================================
 *  Process Control Block  (PCB)
 *  Carried through message queues and kept alive in the
 *  scheduler's ready queue for the whole process lifetime.
 * ========================================================= */
typedef struct
{
    int id;       /* unique process id from input file  */
    int arrival;  /* arrival time (clock ticks)         */
    int runtime;  /* original burst time                */
    int priority; /* 0 = highest, 10 = lowest           */

    /* fields filled and maintained by the scheduler */
    int remaining;   /* remaining burst time               */
    int waiting;     /* total time spent in ready queue    */
    int start_time;  /* first time the process ran         */
    int finish_time; /* clock tick when process finished   */

    pid_t pid; /* OS pid of the forked process.c     */

    /* state flags */
    int started;  /* 1 after first dispatch             */
    int finished; /* 1 after process sends finish sig   */
} PCB;

/* =========================================================
 *  Message sent from Process Generator → Scheduler
 *  via System V message queue (msgget / msgsnd / msgrcv).
 *
 *  mtype = 1  →  new process arrived
 *  mtype = 2  →  end-of-input sentinel (no more processes)
 * ========================================================= */
typedef struct
{
    long mtype; /* MUST be the first field for System V IPC */
    PCB proc;
} Message;

/* =========================================================
 *  Ready-Queue Node  (doubly-linked list)
 *
 *  Using a doubly-linked list lets every scheduling
 *  algorithm share the same node type:
 *
 *  • HPF  → insert in sorted order by priority (O(n) insert,
 *            O(1) dequeue from head).  Ties broken by arrival
 *            time (earlier arrival wins), then by id.
 *
 *  • RR   → always append to tail, dequeue from head (O(1)
 *            both ends).
 *
 *  • FCFS → same as RR (arrival order = FIFO).
 *
 *  A min-heap would give O(log n) insert for HPF, but at the
 *  scale of this assignment a sorted list is simpler to debug,
 *  easier to steal from (just take the tail node), and fast
 *  enough for the expected process counts.
 * ========================================================= */
typedef struct QNode
{
    PCB pcb;
    struct QNode *prev;
    struct QNode *next;
} QNode;

typedef struct
{
    QNode *head; /* dequeue / peek from here */
    QNode *tail; /* append / steal from here */
    int size;
} ReadyQueue;

typedef struct
{
    int queue_size[2];
    int ready_remaining[2];
    int running_remaining[2];

    int consume_turn; /* 1 or 2 */
    int all_received;
    int shutdown;
    int child_ready[2];
    int child_done[2];
    int penalty_until;
    int current_tick;

    int steal_pending;
    int steal_from; /* 1 or 2 */
    int steal_to;   /* 1 or 2 */
    int has_stolen;
    PCB stolen_proc;
} FCFS2Control;

// Added by Youssef
typedef struct {
    PCB ready_queue[MAX_PROCESSES];
    int queue_size;
    int running_count;
    bool scheduler_ready;
    bool all_processes_received;  /* Add this line */
    bool all_processes_completed;
} SharedMemory;

/* ----  Queue helpers (implemented in queue.c or inlined)  ---- */

/* Create an empty queue on the heap */
static inline ReadyQueue *queue_create(void)
{
    ReadyQueue *q = (ReadyQueue *)malloc(sizeof(ReadyQueue));
    if (!q)
    {
        perror("malloc queue");
        exit(1);
    }
    q->head = q->tail = NULL;
    q->size = 0;
    return q;
}

/* Append to tail – used by RR and FCFS */
static inline void queue_push_tail(ReadyQueue *q, PCB pcb)
{
    QNode *n = (QNode *)malloc(sizeof(QNode));
    if (!n)
    {
        perror("malloc QNode");
        exit(1);
    }
    n->pcb = pcb;
    n->prev = q->tail;
    n->next = NULL;
    if (q->tail)
        q->tail->next = n;
    else
        q->head = n;
    q->tail = n;
    q->size++;
}

/*
 * Sorted insert – used by HPF.
 * Ordering: lower priority value wins; ties broken by arrival
 * time (earlier first), then by id (lower first).
 */
static inline void queue_insert_sorted(ReadyQueue *q, PCB pcb)
{
    QNode *n = (QNode *)malloc(sizeof(QNode));
    if (!n)
    {
        perror("malloc QNode");
        exit(1);
    }
    n->pcb = pcb;

    /* Find the first node that the new node should come before */
    QNode *cur = q->head;
    while (cur)
    {
        if (pcb.priority < cur->pcb.priority)
            break;
        if (pcb.priority == cur->pcb.priority &&
            pcb.arrival < cur->pcb.arrival)
            break;
        if (pcb.priority == cur->pcb.priority &&
            pcb.arrival == cur->pcb.arrival &&
            pcb.id < cur->pcb.id)
            break;
        cur = cur->next;
    }

    if (!cur)
    {
        /* Insert at tail */
        n->prev = q->tail;
        n->next = NULL;
        if (q->tail)
            q->tail->next = n;
        else
            q->head = n;
        q->tail = n;
    }
    else
    {
        /* Insert before cur */
        n->next = cur;
        n->prev = cur->prev;
        if (cur->prev)
            cur->prev->next = n;
        else
            q->head = n;
        cur->prev = n;
    }
    q->size++;
}

/* Dequeue from head; caller owns the returned PCB value */
static inline int queue_pop(ReadyQueue *q, PCB *out)
{
    if (!q->head)
        return 0;
    QNode *n = q->head;
    *out = n->pcb;
    q->head = n->next;
    if (q->head)
        q->head->prev = NULL;
    else
        q->tail = NULL;
    free(n);
    q->size--;
    return 1;
}

/* Steal from tail (used by 2-CPU work stealing) */
static inline int queue_steal_tail(ReadyQueue *q, PCB *out)
{
    if (!q->tail)
        return 0;
    QNode *n = q->tail;
    *out = n->pcb;
    q->tail = n->prev;
    if (q->tail)
        q->tail->next = NULL;
    else
        q->head = NULL;
    free(n);
    q->size--;
    return 1;
}

/* Peek at head without removing */
static inline PCB *queue_peek(ReadyQueue *q)
{
    return q->head ? &q->head->pcb : NULL;
}

/* Sum of remaining times for all processes in the queue */
static inline int queue_total_remaining(ReadyQueue *q)
{
    int total = 0;
    QNode *cur = q->head;
    while (cur)
    {
        total += cur->pcb.remaining;
        cur = cur->next;
    }
    return total;
}

#endif