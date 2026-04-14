/*
 * scheduler.c — OS Scheduler (CMP N303)
 *
 * Algorithms (argv[1]):
 *   1 = HPF  Preemptive Highest Priority First
 *   2 = RR   Round Robin  (Q = argv[2])
 *   3 = FCFS_2  2-CPU FCFS with Work Stealing  (N = argv[2], M = argv[3])
 *
 * ── Canonical timing ─────────────────────────────────────────────────────────
 *   All event times use the logical spec time, correcting for MQ polling latency:
 *   • Arrival log    : PCB.arrival
 *   • Preemption     : max(slice_start, head->arrival)
 *   • Quantum expiry : quantum_end (set at dispatch time)
 *   • Finish time    : slice_start + remaining
 *   • Dispatch time  : max(cpu_free_at, front->arrival)
 *   • Steal log time : (detected_tick / N) * N  (nearest passed multiple of N)
 *
 * ── Preemption (HPF & RR) ────────────────────────────────────────────────────
 *   SIGKILL child → fork NEW process.out with updated remaining.
 *
 * ── Waiting time ─────────────────────────────────────────────────────────────
 *   waiting += (dispatch_time - last_enqueue_time)
 *   last_enqueue_time stored in PCB.finish_time until final completion.
 *   First arrival: finish_time = arrival.
 *   Re-queue (stop / quantum / steal-overhead): finish_time = stop_time.
 *
 * ── 2-CPU FCFS Work Stealing ─────────────────────────────────────────────────
 *   • Two FIFO queues, non-preemptive FCFS per CPU.
 *   • Arrival: shorter queue (by process count); tie → CPU1 (q[0]).
 *   • Steal check every N ticks (when now >= next_check):
 *       total_remaining = running.remaining + sum(queue.remaining)
 *       while |diff| > M: steal tail from longer → shorter
 *       log: "At time <canonical_N_multiple> process Y was stolen"
 *   • Steal overhead: kill both running processes (save remaining),
 *     block 3 ticks, re-fork with saved remaining. waiting += 3 per process.
 *   • Output: scheduler_1.log/perf + scheduler_2.log/perf (separate per CPU).
 *
 * ── WTA guard ────────────────────────────────────────────────────────────────
 *   runtime==0 → WTA = 0.00
 *
 * ── CPU utilisation ──────────────────────────────────────────────────────────
 *   Σ(runtimes) / last_finish × 100  (per CPU for 2-CPU mode)
 */

#include "headers.h"
#include "shared.h"
#include <string.h>
#include <math.h>

/* ═══ Shared globals ══════════════════════════════════════════════════════════ */
static int msgqid;
static FILE *log_fp;
static int all_arrived;
static int num_completed, total_runtime, total_waiting, last_finish;
static double total_wta;
static double wta_arr[1024];

/* ═══ Logging ═════════════════════════════════════════════════════════════════ */
static void log_arrived_to(FILE *f, const PCB *p)
{
    if (!f)
        return;
    fprintf(f, "At time %d process %d arrived arr %d total %d remain %d wait %d\n",
            p->arrival, p->id, p->arrival, p->runtime, p->runtime, 0);
    fflush(f);
}

static void log_event_to(FILE *f, int t, const PCB *p, const char *state)
{
    if (!f)
        return;
    if (strcmp(state, "finished") == 0)
    {
        int ta = p->finish_time - p->arrival;
        double wta = (p->runtime > 0) ? (double)ta / p->runtime : 0.0;
        fprintf(f, "At time %d process %d finished arr %d total %d remain %d wait %d TA %d WTA %.2f\n",
                t, p->id, p->arrival, p->runtime, 0, p->waiting, ta, round(wta*100)/100);
    }
    else
    {
        fprintf(f, "At time %d process %d %s arr %d total %d remain %d wait %d\n",
                t, p->id, state, p->arrival, p->runtime, p->remaining, p->waiting);
    }
    fflush(f);
}

static void log_arrived(const PCB *p) { log_arrived_to(log_fp, p); }
static void log_event(int t, const PCB *p, const char *s) { log_event_to(log_fp, t, p, s); }

/* ═══ Message queue ══════════════════════════════════════════════════════════ */
static void receive_1cpu(ReadyQueue *q, void (*fn)(ReadyQueue *, PCB), int now)
{
    Message msg;
    while (msgrcv(msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) > 0)
    {
        if (msg.mtype == 1)
        {
            PCB p = msg.proc;
            p.remaining = p.runtime;
            p.waiting = 0;
            p.started = 0;
            p.finished = 0;
            p.finish_time = p.arrival;
            fn(q, p);
            log_arrived(&p);
            printf("[Sched]t=%d Q P%d(arr=%d run=%d pri=%d)\n",
                   now, p.id, p.arrival, p.runtime, p.priority);
        }
        else if (msg.mtype == 2)
        {
            all_arrived = 1;
            printf("[Sched]Sentinel.\n");
        }
    }
}

/* ═══ Process utilities ═══════════════════════════════════════════════════════ */
static pid_t reap_child(pid_t pid, int ms)
{
    int st, w = 0;
    pid_t r = 0;
    while (w < ms)
    {
        r = waitpid(pid, &st, WNOHANG);
        if (r != 0)
            break;
        usleep(10000);
        w += 10;
    }
    return r;
}

static pid_t fork_proc(int rem)
{
    pid_t p = fork();
    if (p == 0)
    {
        char r[16];
        snprintf(r, 16, "%d", rem);
        execl("./process.out", "process.out", r, NULL);
        perror("execl");
        exit(1);
    }
    if (p < 0)
    {
        perror("fork");
        exit(1);
    }
    return p;
}

/* ═══ Completion (single-CPU) ════════════════════════════════════════════════ */
static void complete(PCB *p, int fin)
{
    p->finish_time = fin;
    p->remaining = 0;
    p->finished = 1;
    log_event(fin, p, "finished");
    int ta = fin - p->arrival;
    double wta = (p->runtime > 0) ? (double)ta / p->runtime : 0.0;
    total_wta += wta;
    wta_arr[num_completed] = wta;
    total_runtime += p->runtime;
    total_waiting += p->waiting;
    num_completed++;
    if (fin > last_finish)
        last_finish = fin;
    printf("[Sched]P%d done TA=%d WTA=%.2f wait=%d\n", p->id, ta, wta, p->waiting);
}

/* ═══ Performance file ════════════════════════════════════════════════════════ */
static void write_perf_to(const char *fn, int nc, int rt, int wt,
                          double wta, double *ws, int lf)
{
    FILE *f = fopen(fn, "w");
    if (!f)
    {
        perror(fn);
        return;
    }
    double u = lf > 0 ? (double)rt / lf * 100 : 0.0;
    double aw = nc > 0 ? wta / nc : 0.0;
    double awt = nc > 0 ? (double)wt / nc : 0.0;
    double v = 0;
    for (int i = 0; i < nc; i++)
    {
        double d = ws[i] - aw;
        v += d * d;
    }
    double s = nc > 0 ? sqrt(v / nc) : 0.0;
    fprintf(f, "CPU utilization = %.2f%%\n", u);
    fprintf(f, "Avg WTA = %.2f\n", aw);
    fprintf(f, "Avg Waiting = %.2f\n", awt);
    fprintf(f, "Std WTA = %.2f\n", s);
    fclose(f);
    printf("[%s] util=%.2f%% avgWTA=%.2f avgWait=%.2f stdWTA=%.2f\n",
           fn, u, aw, awt, s);
}

static void write_perf(void)
{
    write_perf_to("scheduler.perf", num_completed, total_runtime,
                  total_waiting, total_wta, wta_arr, last_finish);
    if (num_completed == 0)
        return;
    double aw = total_wta / num_completed;
    double awt = (double)total_waiting / num_completed;
    double v = 0;
    for (int i = 0; i < num_completed; i++)
    {
        double d = wta_arr[i] - aw;
        v += d * d;
    }
    printf("\n=== Performance Summary ===\n");
    printf("CPU utilization = %.2f%%\n", (double)total_runtime / last_finish * 100);
    printf("Avg WTA         = %.2f\n", aw);
    printf("Avg Waiting     = %.2f\n", awt);
    printf("Std WTA         = %.2f\n", sqrt(v / num_completed));
}

/* ═══ Init ════════════════════════════════════════════════════════════════════ */
static void init_sched(void)
{
    msgqid = msgget(MSG_KEY, 0644);
    while (msgqid == -1)
    {
        usleep(50000);
        msgqid = msgget(MSG_KEY, 0644);
    }
    printf("[Sched]MQ id=%d\n", msgqid);
    initClk();
    printf("[Sched]Clock ready.\n");
    log_fp = fopen("scheduler.log", "w");
    if (!log_fp)
    {
        perror("scheduler.log");
        exit(1);
    }
    fprintf(log_fp, "#At time x process y state arr w total z remain y wait k\n");
    fflush(log_fp);
    all_arrived = num_completed = total_runtime = total_waiting = last_finish = 0;
    total_wta = 0.0;
}

/* ═══════════════════════════════════════════════════════════════════════════════
 *  FCFS — Single-CPU, non-preemptive
 * ═══════════════════════════════════════════════════════════════════════════════ */
static void run_fcfs(void)
{
    ReadyQueue *q = queue_create();
    PCB cur;
    cur.id = -1;
    int cf = 0, prev = -1, now;
    for (;;)
    {
        now = getClk();
        if (now == prev)
        {
            usleep(10000);
            continue;
        }
        prev = now;
        receive_1cpu(q, queue_push_tail, now);
        /* completion */
        if (cur.id != -1)
        {
            int e = cur.start_time + cur.runtime;
            if (now >= e)
            {
                if (reap_child(cur.pid, 1200) > 0)
                {
                    complete(&cur, e);
                    cf = e + 1;
                    cur.id = -1;
                }
                else
                    fprintf(stderr, "[FCFS]P%d not exited\n", cur.id);
            }
        }
        /* dispatch */
        if (cur.id == -1)
        {
            PCB *f = queue_peek(q);
            if (f)
            {
                int s = (cf > f->arrival) ? cf : f->arrival;
                if (now >= s)
                {
                    PCB nx;
                    queue_pop(q, &nx);
                    nx.start_time = s;
                    nx.started = 1;
                    nx.waiting = s - nx.arrival;
                    nx.pid = fork_proc(nx.remaining);
                    cur = nx;
                    log_event(s, &cur, "started");
                    printf("[FCFS]t=%d P%d run=%d wait=%d\n", s, cur.id, cur.runtime, cur.waiting);
                    if (cur.runtime == 0)
                    {
                        if (reap_child(cur.pid, 1200) > 0)
                        {
                            complete(&cur, s);
                            cf = s + 1;
                            cur.id = -1;
                        }
                    }
                }
            }
        }
        if (all_arrived && q->size == 0 && cur.id == -1)
        {
            printf("[FCFS]done t=%d\n", now);
            break;
        }
    }
    free(q);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 *  HPF — Preemptive Highest Priority First
 * ═══════════════════════════════════════════════════════════════════════════════ */
static void run_hpf(void)
{
    ReadyQueue *q = queue_create();
    PCB cur;
    cur.id = -1;
    int cf = 0, ss = 0, ics = 0, prev = -1, now;
    for (;;)
    {
        now = getClk();
        if (now == prev)
        {
            usleep(10000);
            continue;
        }
        prev = now;
        receive_1cpu(q, queue_insert_sorted, now);
        if (cur.id != -1)
        {
            PCB *h = queue_peek(q);
            int pre = 0;
            if (h && !ics)
            {
                if (h->priority < cur.priority)
                    pre = 1;
                else if (h->priority == cur.priority && h->arrival < cur.arrival)
                    pre = 1;
                else if (h->priority == cur.priority && h->arrival == cur.arrival && h->id < cur.id)
                    pre = 1;
            }
            int cs = pre ? (h->arrival > ss ? h->arrival : ss) : 0;
            int rn = cur.remaining - (now - ss);
            int rc = pre ? cur.remaining - (cs - ss) : 0;
            if (rn <= 0)
            {
                int fin = ss + cur.remaining;
                if (reap_child(cur.pid, 1200) > 0)
                {
                    cur.remaining = 0;
                    complete(&cur, fin);
                    cf = fin + 1;
                    ics = 1;
                    cur.id = -1;
                }
                else
                    fprintf(stderr, "[HPF]P%d not exited\n", cur.id);
            }
            else if (pre && rc > 0)
            {
                cur.remaining = rc;
                cur.finish_time = cs;
                kill(cur.pid, SIGKILL);
                reap_child(cur.pid, 500);
                log_event(cs, &cur, "stopped");
                printf("[HPF]t=%d stop P%d rem=%d\n", cs, cur.id, cur.remaining);
                queue_insert_sorted(q, cur);
                cf = cs + 1;
                ics = 1;
                cur.id = -1;
            }
        }
        if (ics && now >= cf)
            ics = 0;
        if (cur.id == -1 && !ics)
        {
            PCB *f = queue_peek(q);
            if (f)
            {
                int s = (cf > f->arrival) ? cf : f->arrival;
                if (now >= s)
                {
                    PCB nx;
                    queue_pop(q, &nx);
                    nx.waiting += (s - nx.finish_time);
                    nx.pid = fork_proc(nx.remaining);
                    nx.start_time = nx.started ? nx.start_time : s;
                    ss = s;
                    const char *ev = nx.started ? "resumed" : "started";
                    nx.started = 1;
                    log_event(s, &nx, ev);
                    printf("[HPF]t=%d %s P%d rem=%d wait=%d\n", s, ev, nx.id, nx.remaining, nx.waiting);
                    cur = nx;
                    if (cur.remaining == 0)
                    {
                        if (reap_child(cur.pid, 1200) > 0)
                        {
                            complete(&cur, s);
                            cf = s + 1;
                            ics = 1;
                            cur.id = -1;
                        }
                    }
                }
            }
        }
        if (all_arrived && q->size == 0 && cur.id == -1 && !ics)
        {
            printf("[HPF]done t=%d\n", now);
            break;
        }
    }
    free(q);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 *  RR — Round Robin (quantum Q)
 * ═══════════════════════════════════════════════════════════════════════════════ */
static void run_rr(int Q)
{
    printf("[RR]Q=%d\n", Q);
    ReadyQueue *q = queue_create();
    PCB cur;
    cur.id = -1;
    int cf = 0, ss = 0, qe = 0, ics = 0, prev = -1, now;
    for (;;)
    {
        now = getClk();
        if (now == prev)
        {
            usleep(10000);
            continue;
        }
        prev = now;
        receive_1cpu(q, queue_push_tail, now);
        if (cur.id != -1)
        {
            int rn = cur.remaining - (now - ss);
            int rq = cur.remaining - (qe - ss);
            if (rn <= 0)
            {
                int fin = ss + cur.remaining;
                if (reap_child(cur.pid, 1200) > 0)
                {
                    cur.remaining = 0;
                    complete(&cur, fin);
                    cf = fin + 1;
                    ics = 1;
                    cur.id = -1;
                }
                else
                    fprintf(stderr, "[RR]P%d not exited\n", cur.id);
            }
            else if (now >= qe && rq > 0)
            {
                cur.remaining = rq;
                cur.finish_time = qe;
                kill(cur.pid, SIGKILL);
                reap_child(cur.pid, 500);
                log_event(qe, &cur, "stopped");
                printf("[RR]t=%d qexp P%d rem=%d\n", qe, cur.id, cur.remaining);
                queue_push_tail(q, cur);
                cf = qe + 1;
                ics = 1;
                cur.id = -1;
            }
        }
        if (ics && now >= cf)
            ics = 0;
        if (cur.id == -1 && !ics)
        {
            PCB *f = queue_peek(q);
            if (f)
            {
                int s = (cf > f->arrival) ? cf : f->arrival;
                if (now >= s)
                {
                    PCB nx;
                    queue_pop(q, &nx);
                    nx.waiting += (s - nx.finish_time);
                    nx.pid = fork_proc(nx.remaining);
                    nx.start_time = nx.started ? nx.start_time : s;
                    ss = s;
                    qe = s + Q;
                    const char *ev = nx.started ? "resumed" : "started";
                    nx.started = 1;
                    log_event(s, &nx, ev);
                    printf("[RR]t=%d %s P%d rem=%d wait=%d qe=%d\n", s, ev, nx.id, nx.remaining, nx.waiting, qe);
                    cur = nx;
                    if (cur.remaining == 0)
                    {
                        if (reap_child(cur.pid, 1200) > 0)
                        {
                            complete(&cur, s);
                            cf = s + 1;
                            ics = 1;
                            cur.id = -1;
                        }
                    }
                }
            }
        }
        if (all_arrived && q->size == 0 && cur.id == -1 && !ics)
        {
            printf("[RR]done t=%d\n", now);
            break;
        }
    }
    free(q);
}

/* ═══════════════════════════════════════════════════════════════════════════════
 *  2-CPU FCFS with Work Stealing
 * ═══════════════════════════════════════════════════════════════════════════════ */

typedef struct
{
    ReadyQueue *q;
    PCB cur; /* id==-1 when idle */
    int cf;  /* cpu_free_at */
    int ss;  /* slice_start of current process */
    FILE *log;
    int num; /* 1 or 2 */
    /* per-CPU stats */
    int nc, rt, wt, lf;
    double wta;
    double wtas[512];
} Cpu2;

/* Complete a process on one CPU */
static void cpu2_done(Cpu2 *c, PCB *p, int fin)
{
    p->finish_time = fin;
    p->remaining = 0;
    p->finished = 1;
    log_event_to(c->log, fin, p, "finished");
    int ta = fin - p->arrival;
    double wta = (p->runtime > 0) ? (double)ta / p->runtime : 0.0;
    c->wta += wta;
    c->wtas[c->nc] = wta;
    c->rt += p->runtime;
    c->wt += p->waiting;
    c->nc++;
    if (fin > c->lf)
        c->lf = fin;
    /* accumulate into global counters (for combined scheduler.perf) */
    total_wta += wta;
    wta_arr[num_completed] = wta;
    total_runtime += p->runtime;
    total_waiting += p->waiting;
    num_completed++;
    if (fin > last_finish)
        last_finish = fin;
    printf("[CPU%d]P%d done TA=%d WTA=%.2f wait=%d\n", c->num, p->id, ta, wta, p->waiting);
}

/* Dispatch one process on a CPU */
static void cpu2_dispatch(Cpu2 *c, int now)
{
    if (c->cur.id != -1)
        return;
    PCB *f = queue_peek(c->q);
    if (!f)
        return;
    int s = (c->cf > f->arrival) ? c->cf : f->arrival;
    if (now < s)
        return;
    PCB nx;
    queue_pop(c->q, &nx);
    nx.waiting += (s - nx.finish_time);
    nx.start_time = nx.started ? nx.start_time : s;
    nx.started = 1;
    nx.pid = fork_proc(nx.remaining);
    c->cur = nx;
    c->ss = s;
    log_event_to(c->log, s, &c->cur, "started");
    printf("[CPU%d]t=%d start P%d run=%d wait=%d\n", c->num, s, c->cur.id, c->cur.runtime, c->cur.waiting);
    if (c->cur.runtime == 0)
    {
        if (reap_child(c->cur.pid, 1200) > 0)
        {
            cpu2_done(c, &c->cur, s);
            c->cf = s + 1;
            c->cur.id = -1;
        }
    }
}

/* Receive messages and assign to shorter queue */
static void recv_2cpu(Cpu2 *cpu, int now)
{
    Message msg;
    while (msgrcv(msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) > 0)
    {
        if (msg.mtype == 1)
        {
            PCB p = msg.proc;
            p.remaining = p.runtime;
            p.waiting = 0;
            p.started = 0;
            p.finished = 0;
            p.finish_time = p.arrival;
            /* assign to shorter queue; tie → CPU1 (index 0) */
            int c = (cpu[1].q->size < cpu[0].q->size) ? 1 : 0;
            queue_push_tail(cpu[c].q, p);
            // log_arrived_to(cpu[c].log,&p);
            printf("[FCFS2]t=%d P%d→CPU%d(arr=%d run=%d)\n", now, p.id, c + 1, p.arrival, p.runtime);
        }
        else if (msg.mtype == 2)
        {
            all_arrived = 1;
            printf("[Sched]Sentinel.\n");
        }
    }
}

static void run_fcfs_2cpu(int N, int M)
{
    printf("[FCFS2]N=%d M=%d\n", N, M);

    Cpu2 cpu[2];
    for (int c = 0; c < 2; c++)
    {
        char fn[32];
        snprintf(fn, 32, "scheduler_%d.log", c + 1);
        cpu[c].q = queue_create();
        cpu[c].cur.id = -1;
        cpu[c].cf = 0;
        cpu[c].ss = 0;
        cpu[c].nc = cpu[c].rt = cpu[c].wt = cpu[c].lf = 0;
        cpu[c].wta = 0.0;
        cpu[c].num = c + 1;
        cpu[c].log = fopen(fn, "w");
        if (!cpu[c].log)
        {
            perror(fn);
            exit(1);
        }
        fprintf(cpu[c].log, "#At time x process y state arr w total z remain y wait k\n");
        fflush(cpu[c].log);
    }

    /*
     * steal_state: 0=none, 1=overhead in progress
     * steal_resume_at: tick when overhead ends
     * saved[c]: the killed process to re-fork at resume time
     * was_running[c]: 1 if CPU c had a running process that was killed
     */
    int steal_state = 0, steal_resume_at = -1, next_check = N;
    PCB saved[2];
    int was_running[2] = {0, 0};

    int prev = -1, now;
    for (;;)
    {
        now = getClk();
        if (now == prev)
        {
            usleep(10000);
            continue;
        }
        prev = now;

        /* 1. Receive new arrivals */
        recv_2cpu(cpu, now);

        /* 2. End of steal overhead → re-fork saved processes */
        if (steal_state == 1 && now >= steal_resume_at)
        {
            steal_state = 0;
            for (int c = 0; c < 2; c++)
            {
                if (!was_running[c])
                    continue;
                PCB *p = &saved[c];
                p->waiting += 3; /* 3-tick overhead = waiting */
                p->pid = fork_proc(p->remaining);
                /* finish_time preserved from steal (= canonical_steal_time) */
                cpu[c].cur = *p;
                cpu[c].ss = now;
                cpu[c].cf = now;
                // log_event_to(cpu[c].log,now,&cpu[c].cur,"resumed");
                printf("[CPU%d]t=%d resumed P%d rem=%d wait=%d\n",
                       c + 1, now, cpu[c].cur.id, cpu[c].cur.remaining, cpu[c].cur.waiting);
                was_running[c] = 0;
            }
        }

        int in_overhead = (steal_state == 1);

        /* 3. Check completions (skip during overhead — processes are re-forked) */
        if (!in_overhead)
        {
            for (int c = 0; c < 2; c++)
            {
                Cpu2 *cp = &cpu[c];
                if (cp->cur.id == -1)
                    continue;
                int exp = cp->ss + cp->cur.remaining;
                if (now >= exp)
                {
                    if (reap_child(cp->cur.pid, 1200) > 0)
                    {
                        cpu2_done(cp, &cp->cur, exp);
                        cp->cf = exp + 1;
                        cp->cur.id = -1;
                    }
                    else
                        fprintf(stderr, "[CPU%d]P%d not exited\n", cp->num, cp->cur.id);
                }
            }
        }

        /* 4. Work stealing check (use >= so we never miss a check tick) */
        if (!in_overhead && now >= next_check)
        {
            /*
             * Canonical steal time = (now/N)*N  (last passed N-multiple).
             * This corrects for the MQ polling race: if we detect the imbalance
             * one tick late, we still log the steal at the canonical tick.
             */
            int canonical_steal_time = (now / N) * N;
            next_check = canonical_steal_time + N; /* next check N ticks later */

            /* Compute total remaining per CPU (running + queued) */
            int rem[2];
            for (int c = 0; c < 2; c++)
            {
                rem[c] = queue_total_remaining(cpu[c].q);
                if (cpu[c].cur.id != -1)
                {
                    int ran = now - cpu[c].ss;
                    int r = cpu[c].cur.remaining - ran;
                    rem[c] += (r > 0 ? r : 0);
                }
            }

            int diff = rem[0] - rem[1];
            int did_steal = 0;

            while (abs(diff) > M)
            {
                int from = (diff > 0) ? 0 : 1;
                int to = (diff > 0) ? 1 : 0;
                PCB stolen;
                if (!queue_steal_tail(cpu[from].q, &stolen))
                    break;
                /* Update enqueue_time to canonical steal time */
                /* Do NOT update finish_time - it stays as arrival (last enqueue time for waiting calc) */
                /* Log the steal at canonical time */
                fprintf(cpu[from].log, "At time %d process %d was stolen\n",
                        canonical_steal_time, stolen.id);
                fflush(cpu[from].log);
                printf("[FCFS2]t=%d(%d) P%d stolen CPU%d→CPU%d\n",
                       now, canonical_steal_time, stolen.id, from + 1, to + 1);
                queue_push_tail(cpu[to].q, stolen);
                did_steal = 1;
                /* Recompute remaining */
                for (int c = 0; c < 2; c++)
                {
                    rem[c] = queue_total_remaining(cpu[c].q);
                    if (cpu[c].cur.id != -1)
                    {
                        int r = cpu[c].cur.remaining - (now - cpu[c].ss);
                        rem[c] += (r > 0 ? r : 0);
                    }
                }
                diff = rem[0] - rem[1];
            }

            if (did_steal)
            {
                /*
                 * Kill both running processes and start 3-tick overhead.
                 * Overhead starts at canonical_steal_time, ends at canonical+3.
                 * The re-fork happens at canonical_steal_time+3.
                 */
                int overhead_start = canonical_steal_time;
                steal_resume_at = overhead_start + 3;
                steal_state = 1;

                for (int c = 0; c < 2; c++)
                {
                    if (cpu[c].cur.id != -1)
                    {
                        /* Remaining = what was left at overhead_start */
                        int ran = overhead_start - cpu[c].ss;
                        if (ran < 0)
                            ran = 0;
                        cpu[c].cur.remaining -= ran;
                        if (cpu[c].cur.remaining < 0)
                            cpu[c].cur.remaining = 0;
                        cpu[c].cur.finish_time = overhead_start;
                        kill(cpu[c].cur.pid, SIGKILL);
                        reap_child(cpu[c].cur.pid, 500);
                        // log_event_to(cpu[c].log,overhead_start,&cpu[c].cur,"stopped");
                        printf("[CPU%d]t=%d P%d stopped for steal overhead rem=%d\n",
                               c + 1, overhead_start, cpu[c].cur.id, cpu[c].cur.remaining);
                        saved[c] = cpu[c].cur;
                        was_running[c] = 1;
                        cpu[c].cur.id = -1;
                    }
                    else
                    {
                        was_running[c] = 0;
                    }
                }
                printf("[FCFS2]overhead t=%d→%d\n", overhead_start, steal_resume_at);
                /* Block dispatch on ALL CPUs during overhead, even idle ones */
                for (int c = 0; c < 2; c++)
                    if (cpu[c].cf < steal_resume_at)
                        cpu[c].cf = steal_resume_at;
            }
        }

        /* 5. Dispatch on both CPUs — re-check in_overhead after steal step */
        in_overhead = (steal_state == 1);
        if (!in_overhead)
        {
            for (int c = 0; c < 2; c++)
                cpu2_dispatch(&cpu[c], now);
        }

        /* 6. Termination */
        if (all_arrived &&
            cpu[0].q->size == 0 && cpu[1].q->size == 0 &&
            cpu[0].cur.id == -1 && cpu[1].cur.id == -1 &&
            steal_state == 0)
        {
            printf("[FCFS2]All done t=%d.\n", now);
            break;
        }
    }

    /* Write per-CPU perf files */
    for (int c = 0; c < 2; c++)
    {
        char fn[32];
        snprintf(fn, 32, "scheduler_%d.perf", c + 1);
        write_perf_to(fn, cpu[c].nc, cpu[c].rt, cpu[c].wt,
                      cpu[c].wta, cpu[c].wtas, cpu[c].lf);
        if (cpu[c].log)
            fclose(cpu[c].log);
        free(cpu[c].q);
    }
}

/* ═══════════════════════════════════════════════════════════════════════════════
 *  main
 * ═══════════════════════════════════════════════════════════════════════════════ */
int main(int argc, char *argv[])
{
    int algo = (argc > 1) ? atoi(argv[1]) : 3;
    int p2 = (argc > 2) ? atoi(argv[2]) : 1;
    int p3 = (argc > 3) ? atoi(argv[3]) : 1;
    printf("[Sched]algo=%d p2=%d p3=%d\n", algo, p2, p3);
    init_sched();
    switch (algo)
    {
    case ALGO_HPF:
        run_hpf();
        break;
    case ALGO_RR:
        run_rr(p2);
        break;
    case ALGO_FCFS_2:
    {
        int N = (argc > 3) ? atoi(argv[3]) : 1;
        int M_ = (argc > 4) ? atoi(argv[4]) : 1;
        run_fcfs_2cpu(N, M_);
    };
    break;
    default:
        fprintf(stderr, "[Sched]Unknown algo %d, using FCFS.\n", algo);
        run_fcfs();
    }
    write_perf();
    if (log_fp)
        fclose(log_fp);
    destroyClk(true);
    return 0;
}