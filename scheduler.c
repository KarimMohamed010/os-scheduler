/*
 * scheduler.c — OS Scheduler (CMP N303)
 *
 * Algorithms (argv[1]):
 *   1 = HPF     Preemptive Highest Priority First
 *   2 = RR      Round Robin  (Q = argv[2])
 *   3 = FCFS_2  2-CPU FCFS with Work Stealing (N = argv[3], M = argv[4])
 *
 * ── Spec-compliant log format ────────────────────────────────────────────────
 *   Allowed states: started, resumed, stopped, finished.
 *   NO "arrived" state in the log (spec §Part V-A).
 *   "was stolen" only appears in 2-CPU log (spec §Part V-B).
 *   TA and WTA printed only on "finished" line.
 *
 * ── Key design decisions ─────────────────────────────────────────────────────
 *
 *  Preemption (HPF):
 *    Detection tick T = now (when scheduler sees the higher-priority process).
 *    stop_time = T.  rc = cur.remaining - (T - ss).
 *    Using T (not canonical arrival) avoids incorrect remaining due to MQ lag.
 *    In practice T ≈ arrival (10ms poll << 1 sec tick), so timing is accurate.
 *
 *  Quantum expiry (RR):
 *    stop_time = quantum_end (set at dispatch).  rc = remaining - (qend-ss).
 *    quantum_end is computed at dispatch and never drifts.
 *
 *  Dispatch time:
 *    start = max(cpu_free_at, process.arrival).
 *    Corrects for scheduler startup latency (first process starts at arrival, not now).
 *
 *  Waiting time:
 *    waiting += (dispatch_time - last_enqueue_time).
 *    last_enqueue_time stored in PCB.finish_time (reused temp field until done).
 *    Set to arrival on first queue entry; set to stop_time on re-queue.
 *
 *  WTA guard: runtime==0 → WTA = 0.00 (no division by zero).
 *
 *  CPU utilisation = Σ(runtimes) / last_finish × 100.
 *
 * ── 2-CPU FCFS Work Stealing ─────────────────────────────────────────────────
 *  • Two FIFO queues (non-preemptive FCFS per CPU).
 *  • Arrival assignment: shorter queue (count); tie → CPU1.
 *  • total_remaining = running.remaining + sum(queue.remaining).
 *  • Steal check every N ticks: while |diff| > M steal tail from longer → shorter.
 *    Log: "At time <canonical_N_multiple> process Y was stolen" (in FROM-CPU log).
 *  • Steal overhead: SIGKILL running processes, save remaining, re-fork after 3 ticks.
 *    Stopped processes: waiting += 3.  cpu_free_at = steal_resume for all CPUs.
 *  • Output: scheduler_1.log/perf + scheduler_2.log/perf.
 */

#include "headers.h"
#include "shared.h"
#include <string.h>
#include <math.h>

/* ═══ Shared globals ══════════════════════════════════════════════════════════ */
static int    msgqid;
static FILE  *log_fp;
static int    all_arrived;
static int    num_completed, total_runtime, total_waiting, last_finish;
static double total_wta;
static double wta_arr[1024];

/* ═══ Logging ═════════════════════════════════════════════════════════════════
 *
 * Spec log line format:
 *   started/resumed/stopped:
 *     At time X process Y <state> arr A total T remain R wait W
 *   finished:
 *     At time X process Y finished arr A total T remain 0 wait W TA t WTA w
 *
 * NOTE: "arrived" is NOT a valid log state per spec §Part V-A.
 * ========================================================================== */
static void log_event_to(FILE *f, int t, const PCB *p, const char *state)
{
    if (!f) return;
    if (strcmp(state, "finished") == 0) {
        int    ta  = p->finish_time - p->arrival;
        double wta = (p->runtime > 0) ? (double)ta / p->runtime : 0.0;
        fprintf(f, "At time %d process %d finished arr %d total %d remain %d "
                   "wait %d TA %d WTA %.2f\n",
                t, p->id, p->arrival, p->runtime, 0, p->waiting, ta, wta);
    } else {
        fprintf(f, "At time %d process %d %s arr %d total %d remain %d wait %d\n",
                t, p->id, state, p->arrival, p->runtime, p->remaining, p->waiting);
    }
    fflush(f);
}

static void log_event(int t, const PCB *p, const char *s)
{
    log_event_to(log_fp, t, p, s);
}

/* ═══ Message queue ══════════════════════════════════════════════════════════ */

/*
 * Drain MQ and enqueue each new PCB using enqueue_fn.
 * PCB.finish_time is initialised to arrival (used as last_enqueue_time).
 * NOTE: No "arrived" log line emitted here (not in spec).
 */
/*
 * receive_1cpu — drain the message queue into the ready queue.
 *
 * After the clock ticks, there is a brief race: the generator and scheduler
 * both detect the new tick and the generator sends process messages.
 * To ensure we capture ALL arrivals for this tick (not just those already
 * in the MQ when we first poll), we poll in a tight loop for up to 300 ms.
 * A clock tick is 1000 ms, so 300 ms is a safe window that still leaves
 * plenty of time for scheduling before the next tick.
 */
static void receive_1cpu(ReadyQueue *q, void (*enqueue_fn)(ReadyQueue *, PCB), int now)
{
    int waited = 0;
    int got_any = 1;

    /* Keep polling until MQ is empty AND we have waited at least 300ms,
     * OR until the clock has advanced (next tick started). */
    while (waited < 50 || got_any) {
        got_any = 0;
        Message msg;
        while (msgrcv(msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) > 0) {
            got_any = 1;
            if (msg.mtype == 1) {
                PCB p = msg.proc;
                p.remaining  = p.runtime;
                p.waiting    = 0;
                p.started    = 0;
                p.finished   = 0;
                p.finish_time = p.arrival;   /* enqueue_time = arrival */
                enqueue_fn(q, p);
                printf("[Sched] t=%d  queued P%d (arr=%d run=%d pri=%d)\n",
                       now, p.id, p.arrival, p.runtime, p.priority);
            } else if (msg.mtype == 2) {
                all_arrived = 1;
                printf("[Sched] Sentinel received.\n");
            }
        }
        if (waited < 50) {
            usleep(10000);   /* 10 ms */
            waited += 10;
            /* Stop early if clock already moved to next tick */
            if (getClk() != now) break;
        } else {
            break;
        }
    }
}

/* ═══ Process utilities ═══════════════════════════════════════════════════════ */

static pid_t reap_child(pid_t pid, int max_ms)
{
    int status, waited = 0;
    pid_t r = 0;
    while (waited < max_ms) {
        r = waitpid(pid, &status, WNOHANG);
        if (r != 0) break;
        usleep(10000);
        waited += 10;
    }
    return r;
}

static pid_t fork_proc(int remaining)
{
    pid_t p = fork();
    if (p == 0) {
        char r[16];
        snprintf(r, sizeof(r), "%d", remaining);
        execl("./process.out", "process.out", r, NULL);
        perror("execl process.out");
        exit(1);
    }
    if (p < 0) { perror("fork"); exit(1); }
    return p;
}

/* ═══ Completion (single-CPU) ════════════════════════════════════════════════ */

static void complete(PCB *p, int fin)
{
    p->finish_time = fin;
    p->remaining   = 0;
    p->finished    = 1;
    log_event(fin, p, "finished");

    int    ta  = fin - p->arrival;
    double wta = (p->runtime > 0) ? (double)ta / p->runtime : 0.0;

    total_wta              += wta;
    wta_arr[num_completed]  = wta;
    total_runtime          += p->runtime;
    total_waiting          += p->waiting;
    num_completed++;
    if (fin > last_finish) last_finish = fin;

    printf("[Sched] P%d finished  TA=%d WTA=%.2f wait=%d\n",
           p->id, ta, wta, p->waiting);
}

/* ═══ Performance file ════════════════════════════════════════════════════════ */

static void write_perf_to(const char *fn, int nc, int rt, int wt,
                           double wta, double *ws, int lf)
{
    FILE *f = fopen(fn, "w");
    if (!f) { perror(fn); return; }

    double util = (lf > 0) ? (double)rt / lf * 100.0 : 0.0;
    double aw   = (nc > 0) ? wta / nc : 0.0;
    double awt  = (nc > 0) ? (double)wt / nc : 0.0;
    double var  = 0.0;
    for (int i = 0; i < nc; i++) { double d = ws[i] - aw; var += d * d; }
    double std  = (nc > 0) ? sqrt(var / nc) : 0.0;

    fprintf(f, "CPU utilization = %.2f%%\n", util);
    fprintf(f, "Avg WTA = %.2f\n",           aw);
    fprintf(f, "Avg Waiting = %.2f\n",        awt);
    fprintf(f, "Std WTA = %.2f\n",            std);
    fclose(f);

    printf("[%s] util=%.2f%% avgWTA=%.2f avgWait=%.2f stdWTA=%.2f\n",
           fn, util, aw, awt, std);
}

static void write_perf(void)
{
    write_perf_to("scheduler.perf", num_completed, total_runtime,
                  total_waiting, total_wta, wta_arr, last_finish);

    if (num_completed == 0) return;
    double aw  = total_wta / num_completed;
    double awt = (double)total_waiting / num_completed;
    double var = 0.0;
    for (int i = 0; i < num_completed; i++) {
        double d = wta_arr[i] - aw; var += d * d;
    }
    printf("\n=== Performance Summary ===\n");
    printf("CPU utilization = %.2f%%\n",
           (double)total_runtime / last_finish * 100.0);
    printf("Avg WTA         = %.2f\n", aw);
    printf("Avg Waiting     = %.2f\n", awt);
    printf("Std WTA         = %.2f\n", sqrt(var / num_completed));
}

/* ═══ Init ════════════════════════════════════════════════════════════════════ */

static void init_sched(void)
{
    msgqid = msgget(MSG_KEY, 0644);
    while (msgqid == -1) { usleep(50000); msgqid = msgget(MSG_KEY, 0644); }
    printf("[Sched] MQ attached (id=%d)\n", msgqid);

    initClk();
    printf("[Sched] Clock ready.\n");

    log_fp = fopen("scheduler.log", "w");
    if (!log_fp) { perror("scheduler.log"); exit(1); }
    fprintf(log_fp, "#At time x process y state arr w total z remain y wait k\n");
    fflush(log_fp);

    all_arrived = num_completed = total_runtime = total_waiting = last_finish = 0;
    total_wta   = 0.0;
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  FCFS — single-CPU, non-preemptive
 * ══════════════════════════════════════════════════════════════════════════════ */

static void run_fcfs(void)
{
    ReadyQueue *q = queue_create();
    PCB cur;  cur.id = -1;
    int cf = 0, prev = -1, now;

    for (;;) {
        now = getClk();
        if (now == prev) { usleep(10000); continue; }
        prev = now;

        receive_1cpu(q, queue_push_tail, now);

        /* Check if running process finished */
        if (cur.id != -1) {
            int exp = cur.start_time + cur.runtime;
            if (now >= exp) {
                if (reap_child(cur.pid, 1200) > 0) {
                    complete(&cur, exp);
                    cf = exp + 1;
                    cur.id = -1;
                } else {
                    fprintf(stderr, "[FCFS] WARNING: P%d not exited\n", cur.id);
                }
            }
        }

        /* Dispatch next process */
        if (cur.id == -1) {
            PCB *front = queue_peek(q);
            if (front) {
                int s = (cf > front->arrival) ? cf : front->arrival;
                if (now >= s) {
                    PCB nx; queue_pop(q, &nx);
                    nx.start_time = s;
                    nx.started    = 1;
                    nx.waiting    = s - nx.arrival;
                    nx.pid        = fork_proc(nx.remaining);
                    cur = nx;
                    log_event(s, &cur, "started");
                    printf("[FCFS] t=%d  started P%d (run=%d wait=%d)\n",
                           s, cur.id, cur.runtime, cur.waiting);

                    /* Handle runtime==0: reap immediately */
                    if (cur.runtime == 0) {
                        if (reap_child(cur.pid, 1200) > 0) {
                            complete(&cur, s);
                            cf = s + 1;
                            cur.id = -1;
                        }
                    }
                }
            }
        }

        if (all_arrived && q->size == 0 && cur.id == -1) {
            printf("[FCFS] All done at t=%d.\n", now);
            break;
        }
    }
    free(q);
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  HPF — Preemptive Highest Priority First
 *
 *  Priority: 0 = highest, 10 = lowest.
 *  Tie-breaking: earlier arrival wins; equal arrival → lower id wins.
 *
 *  Preemption:
 *    When a higher-priority process is detected in the queue at tick T:
 *      stop_time = T  (the actual tick when we detect and act)
 *      remaining_saved = cur.remaining - (T - ss)  (real remaining at T)
 *    We SIGKILL the child and fork a new process.out with remaining_saved.
 *    This is always correct regardless of any MQ polling delay.
 *    The 1-tick context-switch overhead applies: cf = T + 1.
 *
 *  Waiting time: accumulated via PCB.finish_time as last_enqueue_time.
 * ══════════════════════════════════════════════════════════════════════════════ */

static void run_hpf(void)
{
    ReadyQueue *q = queue_create();
    PCB cur;  cur.id = -1;
    int cf = 0, ss = 0, in_ctx = 0;
    int prev = -1, now;

    for (;;) {
        now = getClk();
        if (now == prev) { usleep(10000); continue; }
        prev = now;

        receive_1cpu(q, queue_insert_sorted, now);

        /* ── Check running process ── */
        if (cur.id != -1) {
            int rem_now = cur.remaining - (now - ss);

            /* Natural completion */
            if (rem_now <= 0) {
                int fin = ss + cur.remaining;
                if (reap_child(cur.pid, 1200) > 0) {
                    cur.remaining = 0;
                    complete(&cur, fin);
                    cf     = fin + 1;   /* 1-tick context-switch overhead */
                    in_ctx = 1;
                    cur.id = -1;
                } else {
                    fprintf(stderr, "[HPF] WARNING: P%d not exited\n", cur.id);
                }
            } else {
                /* Check for preemption */
                PCB *head = queue_peek(q);
                int preempt = 0;
                if (head && !in_ctx) {
                    if      (head->priority < cur.priority) preempt = 1;
                    else if (head->priority == cur.priority
                             && head->arrival < cur.arrival) preempt = 1;
                    else if (head->priority == cur.priority
                             && head->arrival == cur.arrival
                             && head->id < cur.id) preempt = 1;
                }
                if (preempt) {
                    /*
                     * Stop at `now` — the tick we detect the higher-priority process.
                     * Real remaining = what the process has LEFT right now.
                     * This is correct even if we detected 1 tick late (MQ lag).
                     */
                    int stop_time = now;
                    cur.remaining = rem_now;          /* real remaining at stop */
                    cur.finish_time = stop_time;      /* enqueue_time for re-queue */

                    kill(cur.pid, SIGKILL);
                    reap_child(cur.pid, 500);

                    log_event(stop_time, &cur, "stopped");
                    printf("[HPF] t=%d  stopped P%d (rem=%d) for P%d (pri=%d)\n",
                           stop_time, cur.id, cur.remaining, head->id, head->priority);

                    queue_insert_sorted(q, cur);
                    cf     = stop_time + 1;   /* 1-tick context-switch */
                    in_ctx = 1;
                    cur.id = -1;
                }
            }
        }

        /* Clear context-switch flag when time is up */
        if (in_ctx && now >= cf) in_ctx = 0;

        /* ── Dispatch next process ── */
        if (cur.id == -1 && !in_ctx) {
            PCB *front = queue_peek(q);
            if (front) {
                int s = (cf > front->arrival) ? cf : front->arrival;
                if (now >= s) {
                    PCB nx; queue_pop(q, &nx);

                    nx.waiting    += (s - nx.finish_time);    /* accumulated wait */
                    nx.pid         = fork_proc(nx.remaining);
                    nx.start_time  = nx.started ? nx.start_time : s;
                    ss = s;

                    const char *ev = nx.started ? "resumed" : "started";
                    nx.started = 1;

                    log_event(s, &nx, ev);
                    printf("[HPF] t=%d  %s P%d (rem=%d wait=%d)\n",
                           s, ev, nx.id, nx.remaining, nx.waiting);
                    cur = nx;

                    /* Handle runtime==0 */
                    if (cur.remaining == 0) {
                        if (reap_child(cur.pid, 1200) > 0) {
                            complete(&cur, s);
                            cf = s + 1;
                            in_ctx = 1;
                            cur.id = -1;
                        }
                    }
                }
            }
        }

        if (all_arrived && q->size == 0 && cur.id == -1 && !in_ctx) {
            printf("[HPF] All done at t=%d.\n", now);
            break;
        }
    }
    free(q);
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  RR — Round Robin (quantum Q clock ticks)
 *
 *  quantum_end is computed at dispatch: quantum_end = start + Q.
 *  When now >= quantum_end and process not done: stop and re-queue at tail.
 *  stop_time = quantum_end (canonical, not now) to avoid 1-tick drift.
 *  Remaining saved = cur.remaining - (quantum_end - ss).
 * ══════════════════════════════════════════════════════════════════════════════ */

static void run_rr(int Q)
{
    printf("[RR] Quantum = %d ticks\n", Q);
    ReadyQueue *q = queue_create();
    PCB cur;  cur.id = -1;
    int cf = 0, ss = 0, qend = 0, in_ctx = 0;
    int prev = -1, now;

    for (;;) {
        now = getClk();
        if (now == prev) { usleep(10000); continue; }
        prev = now;

        receive_1cpu(q, queue_push_tail, now);

        if (cur.id != -1) {
            int rem_now  = cur.remaining - (now  - ss);
            int rem_qend = cur.remaining - (qend - ss);

            if (rem_now <= 0) {
                /* Natural finish */
                int fin = ss + cur.remaining;
                if (reap_child(cur.pid, 1200) > 0) {
                    cur.remaining = 0;
                    complete(&cur, fin);
                    cf = fin + 1;
                    in_ctx = 1;
                    cur.id = -1;
                } else {
                    fprintf(stderr, "[RR] WARNING: P%d not exited\n", cur.id);
                }
            } else if (now >= qend && rem_qend > 0) {
                /* Quantum expired — canonical stop at quantum_end */
                cur.remaining  = rem_qend;
                cur.finish_time = qend;   /* enqueue_time for re-queue */

                kill(cur.pid, SIGKILL);
                reap_child(cur.pid, 500);

                log_event(qend, &cur, "stopped");
                printf("[RR] t=%d  quantum expired P%d (rem=%d)\n",
                       qend, cur.id, cur.remaining);

                queue_push_tail(q, cur);
                cf = qend + 1;
                in_ctx = 1;
                cur.id = -1;
            }
        }

        if (in_ctx && now >= cf) in_ctx = 0;

        if (cur.id == -1 && !in_ctx) {
            PCB *front = queue_peek(q);
            if (front) {
                int s = (cf > front->arrival) ? cf : front->arrival;
                if (now >= s) {
                    PCB nx; queue_pop(q, &nx);
                    nx.waiting    += (s - nx.finish_time);
                    nx.pid         = fork_proc(nx.remaining);
                    nx.start_time  = nx.started ? nx.start_time : s;
                    ss   = s;
                    qend = s + Q;

                    const char *ev = nx.started ? "resumed" : "started";
                    nx.started = 1;

                    log_event(s, &nx, ev);
                    printf("[RR] t=%d  %s P%d (rem=%d wait=%d q_end=%d)\n",
                           s, ev, nx.id, nx.remaining, nx.waiting, qend);
                    cur = nx;

                    if (cur.remaining == 0) {
                        if (reap_child(cur.pid, 1200) > 0) {
                            complete(&cur, s);
                            cf = s + 1;
                            in_ctx = 1;
                            cur.id = -1;
                        }
                    }
                }
            }
        }

        if (all_arrived && q->size == 0 && cur.id == -1 && !in_ctx) {
            printf("[RR] All done at t=%d.\n", now);
            break;
        }
    }
    free(q);
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  2-CPU FCFS with Work Stealing
 *
 *  Each CPU has its own FIFO queue (non-preemptive FCFS).
 *  One scheduler process manages both CPUs.
 *
 *  Assignment: new arrival → shorter queue (count); tie → CPU1 (index 0).
 *
 *  Steal check every N ticks (when now >= next_check):
 *    total_remaining = running.remaining + queue.remaining  per CPU.
 *    while |diff| > M: steal tail from longer queue → shorter queue.
 *    Log "At time <canonical> process Y was stolen" in FROM-CPU log.
 *
 *  Steal overhead = 3 ticks:
 *    SIGKILL both running processes (save remaining).
 *    Re-fork both at steal_resume = canonical + 3.
 *    Stopped processes: waiting += 3.
 *    All CPUs blocked (cf = steal_resume) so nothing dispatches during overhead.
 *
 *  Output:  scheduler_1.log / scheduler_1.perf
 *           scheduler_2.log / scheduler_2.perf
 *  (scheduler.log / scheduler.perf are also written but contain combined stats)
 * ══════════════════════════════════════════════════════════════════════════════ */

typedef struct {
    ReadyQueue *q;
    PCB         cur;         /* id == -1 when idle */
    int         cf;          /* cpu_free_at */
    int         ss;          /* slice_start of currently running process */
    FILE       *log;
    int         num;         /* 1 or 2 (for printf) */
    /* Per-CPU performance counters */
    int    nc, rt, wt, lf;
    double wta;
    double wtas[512];
} Cpu2;

static void cpu2_complete(Cpu2 *c, PCB *p, int fin)
{
    p->finish_time = fin;
    p->remaining   = 0;
    p->finished    = 1;
    log_event_to(c->log, fin, p, "finished");

    int    ta  = fin - p->arrival;
    double wta = (p->runtime > 0) ? (double)ta / p->runtime : 0.0;

    c->wta += wta;  c->wtas[c->nc] = wta;
    c->rt  += p->runtime;
    c->wt  += p->waiting;
    c->nc++;
    if (fin > c->lf) c->lf = fin;

    /* Global accumulators */
    total_wta             += wta;
    wta_arr[num_completed] = wta;
    total_runtime         += p->runtime;
    total_waiting         += p->waiting;
    num_completed++;
    if (fin > last_finish) last_finish = fin;

    printf("[CPU%d] P%d finished  TA=%d WTA=%.2f wait=%d\n",
           c->num, p->id, ta, wta, p->waiting);
}

static void cpu2_dispatch(Cpu2 *c, int now)
{
    if (c->cur.id != -1) return;
    PCB *front = queue_peek(c->q);
    if (!front) return;

    int s = (c->cf > front->arrival) ? c->cf : front->arrival;
    if (now < s) return;

    PCB nx; queue_pop(c->q, &nx);
    nx.waiting    += (s - nx.finish_time);
    nx.start_time  = nx.started ? nx.start_time : s;
    nx.started     = 1;
    nx.pid         = fork_proc(nx.remaining);
    c->cur = nx;
    c->ss  = s;

    log_event_to(c->log, s, &c->cur, "started");
    printf("[CPU%d] t=%d  started P%d (run=%d wait=%d)\n",
           c->num, s, c->cur.id, c->cur.runtime, c->cur.waiting);

    /* Handle runtime==0 */
    if (c->cur.runtime == 0) {
        if (reap_child(c->cur.pid, 1200) > 0) {
            cpu2_complete(c, &c->cur, s);
            c->cf     = s + 1;
            c->cur.id = -1;
        }
    }
}

/*
 * recv_2cpu — drain MQ and assign each new process to the shorter queue.
 *
 * Uses the same 300ms drain window as receive_1cpu to eliminate the
 * MQ polling race between the generator and scheduler at each tick.
 */
static void recv_2cpu(Cpu2 *cpu, int now)
{
    int waited  = 0;
    int got_any = 1;

    while (waited < 50 || got_any) {
        got_any = 0;
        Message msg;
        while (msgrcv(msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) > 0) {
            got_any = 1;
            if (msg.mtype == 1) {
                PCB p = msg.proc;
                p.remaining  = p.runtime;
                p.waiting    = 0;
                p.started    = 0;
                p.finished   = 0;
                p.finish_time = p.arrival;

                /* Assign to shorter queue; tie → CPU1 (index 0) */
                int c = (cpu[1].q->size < cpu[0].q->size) ? 1 : 0;
                queue_push_tail(cpu[c].q, p);

                /* NOTE: No "arrived" line — not in spec log format */
                printf("[FCFS2] t=%d  P%d → CPU%d (arr=%d run=%d)\n",
                       now, p.id, c + 1, p.arrival, p.runtime);
            } else if (msg.mtype == 2) {
                all_arrived = 1;
                printf("[Sched] Sentinel received.\n");
            }
        }
        if (waited < 50) {
            usleep(10000);
            waited += 10;
            if (getClk() != now) break;
        } else {
            break;
        }
    }
}

static void run_fcfs_2cpu(int N, int M)
{
    printf("[FCFS2] N=%d M=%d\n", N, M);

    Cpu2 cpu[2];
    for (int c = 0; c < 2; c++) {
        char fn[32];
        snprintf(fn, sizeof(fn), "scheduler_%d.log", c + 1);
        cpu[c].q   = queue_create();
        cpu[c].cur.id = -1;
        cpu[c].cf  = 0;
        cpu[c].ss  = 0;
        cpu[c].nc  = cpu[c].rt = cpu[c].wt = cpu[c].lf = 0;
        cpu[c].wta = 0.0;
        cpu[c].num = c + 1;
        cpu[c].log = fopen(fn, "w");
        if (!cpu[c].log) { perror(fn); exit(1); }
        fprintf(cpu[c].log, "#At time x process y state arr w total z remain y wait k\n");
        fflush(cpu[c].log);
    }

    int steal_state     = 0;   /* 0 = none, 1 = overhead running */
    int steal_resume_at = -1;  /* tick when overhead ends         */
    int next_check      = N;   /* next tick to check for stealing */
    PCB saved[2];
    int was_running[2] = {0, 0};

    int prev = -1, now;

    for (;;) {
        now = getClk();
        if (now == prev) { usleep(10000); continue; }
        prev = now;

        /* ── 1. Receive new arrivals ── */
        recv_2cpu(cpu, now);

        /* ── 2. End of steal overhead → re-fork saved processes ── */
        if (steal_state == 1 && now >= steal_resume_at) {
            steal_state = 0;
            for (int c = 0; c < 2; c++) {
                if (!was_running[c]) continue;
                PCB *p  = &saved[c];
                p->waiting += 3;             /* 3 ticks of overhead = waiting */
                p->pid      = fork_proc(p->remaining);
                cpu[c].cur  = *p;
                cpu[c].ss   = now;
                cpu[c].cf   = now;
                /* Spec does NOT log "resumed" for steal overhead on running processes */
                printf("[CPU%d] t=%d  P%d resumed from steal overhead (rem=%d wait=%d)\n",
                       c + 1, now, cpu[c].cur.id, cpu[c].cur.remaining, cpu[c].cur.waiting);
                was_running[c] = 0;
            }
        }

        int in_overhead = (steal_state == 1);

        /* ── 3. Check completions ── */
        if (!in_overhead) {
            for (int c = 0; c < 2; c++) {
                Cpu2 *cp = &cpu[c];
                if (cp->cur.id == -1) continue;
                int exp = cp->ss + cp->cur.remaining;
                if (now >= exp) {
                    if (reap_child(cp->cur.pid, 1200) > 0) {
                        cpu2_complete(cp, &cp->cur, exp);
                        cp->cf     = exp + 1;
                        cp->cur.id = -1;
                    } else {
                        fprintf(stderr, "[CPU%d] WARNING: P%d not exited\n",
                                cp->num, cp->cur.id);
                    }
                }
            }
        }

        /* ── 4. Work stealing check ── */
        if (!in_overhead && now >= next_check) {
            /*
             * Canonical steal time = the scheduled check tick (next_check before advancing).
             * This is the N-multiple at which the check was supposed to fire.
             * With the 300ms MQ drain, `now` should always equal next_check exactly.
             */
            int canonical = next_check;
            next_check = canonical + N;

            /* Compute total remaining per CPU (running + queued) */
            int rem[2];
            for (int c = 0; c < 2; c++) {
                rem[c] = queue_total_remaining(cpu[c].q);
                if (cpu[c].cur.id != -1) {
                    int r = cpu[c].cur.remaining - (now - cpu[c].ss);
                    rem[c] += (r > 0) ? r : 0;
                }
            }

            int diff     = rem[0] - rem[1];
            int did_steal = 0;

            while (abs(diff) > M) {
                int from = (diff > 0) ? 0 : 1;
                int to   = (diff > 0) ? 1 : 0;
                PCB stolen;
                if (!queue_steal_tail(cpu[from].q, &stolen)) break;

                /*
                 * finish_time stays as-is (= arrival for a queued process).
                 * This ensures waiting = dispatch_time - arrival when dispatched.
                 * Log the steal in the FROM-CPU log (where it was taken from).
                 */
                fprintf(cpu[from].log, "At time %d process %d was stolen\n",
                        canonical, stolen.id);
                fflush(cpu[from].log);
                printf("[FCFS2] t=%d  P%d stolen CPU%d → CPU%d\n",
                       canonical, stolen.id, from + 1, to + 1);

                queue_push_tail(cpu[to].q, stolen);
                did_steal = 1;

                /* Recompute diff */
                for (int c = 0; c < 2; c++) {
                    rem[c] = queue_total_remaining(cpu[c].q);
                    if (cpu[c].cur.id != -1) {
                        int r = cpu[c].cur.remaining - (now - cpu[c].ss);
                        rem[c] += (r > 0) ? r : 0;
                    }
                }
                diff = rem[0] - rem[1];
            }

            if (did_steal) {
                /*
                 * Start 3-tick overhead.
                 * SIGKILL both running processes (they will be re-forked at canonical+3).
                 * Block all dispatch until steal_resume_at.
                 */
                steal_resume_at = canonical + 3;
                steal_state     = 1;
                in_overhead     = 1;

                for (int c = 0; c < 2; c++) {
                    if (cpu[c].cur.id != -1) {
                        /* Remaining at overhead start = remaining at canonical */
                        int ran = canonical - cpu[c].ss;
                        if (ran < 0) ran = 0;
                        cpu[c].cur.remaining -= ran;
                        if (cpu[c].cur.remaining < 0) cpu[c].cur.remaining = 0;
                        cpu[c].cur.finish_time = canonical;

                        kill(cpu[c].cur.pid, SIGKILL);
                        reap_child(cpu[c].cur.pid, 500);

                        /* Spec does NOT log "stopped" for steal overhead on running processes */
                        printf("[CPU%d] t=%d  P%d paused for steal overhead (rem=%d)\n",
                               c + 1, canonical, cpu[c].cur.id, cpu[c].cur.remaining);

                        saved[c]      = cpu[c].cur;
                        was_running[c] = 1;
                        cpu[c].cur.id = -1;
                    } else {
                        was_running[c] = 0;
                    }
                    /* Block dispatch on ALL CPUs during overhead */
                    if (cpu[c].cf < steal_resume_at)
                        cpu[c].cf = steal_resume_at;
                }
                printf("[FCFS2] overhead t=%d → t=%d\n", canonical, steal_resume_at);
            }
        }

        /* ── 5. Dispatch on both CPUs (re-evaluate in_overhead after steal step) ── */
        in_overhead = (steal_state == 1);
        if (!in_overhead) {
            for (int c = 0; c < 2; c++)
                cpu2_dispatch(&cpu[c], now);
        }

        /* ── 6. Termination ── */
        if (all_arrived &&
            cpu[0].q->size == 0 && cpu[1].q->size == 0 &&
            cpu[0].cur.id  == -1 && cpu[1].cur.id  == -1 &&
            steal_state == 0)
        {
            printf("[FCFS2] All done at t=%d.\n", now);
            break;
        }
    }

    /* Write per-CPU perf files */
    for (int c = 0; c < 2; c++) {
        char fn[32];
        snprintf(fn, sizeof(fn), "scheduler_%d.perf", c + 1);
        write_perf_to(fn, cpu[c].nc, cpu[c].rt, cpu[c].wt,
                      cpu[c].wta, cpu[c].wtas, cpu[c].lf);
        if (cpu[c].log) fclose(cpu[c].log);
        free(cpu[c].q);
    }
}

/* ══════════════════════════════════════════════════════════════════════════════
 *  main
 * ══════════════════════════════════════════════════════════════════════════════ */

int main(int argc, char *argv[])
{
    int algo = (argc > 1) ? atoi(argv[1]) : 3;
    int p2   = (argc > 2) ? atoi(argv[2]) : 1;   /* RR: quantum; FCFS_2: unused */
    int p3   = (argc > 3) ? atoi(argv[3]) : 1;   /* FCFS_2: N                   */
    int p4   = (argc > 4) ? atoi(argv[4]) : 1;   /* FCFS_2: M                   */

    printf("[Sched] algo=%d  p2=%d  p3=%d  p4=%d\n", algo, p2, p3, p4);
    init_sched();

    switch (algo) {
        case ALGO_HPF:
            run_hpf();
            break;
        case ALGO_RR:
            run_rr(p2);
            break;
        case ALGO_FCFS_2:
            run_fcfs_2cpu(p3, p4);
            break;
        default:
            fprintf(stderr, "[Sched] Unknown algo %d, defaulting to single-CPU FCFS.\n", algo);
            run_fcfs();
    }

    write_perf();
    if (log_fp) fclose(log_fp);
    destroyClk(true);
    return 0;
}