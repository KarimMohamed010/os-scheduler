/*
 * scheduler.c  —  First-Come First-Served (FCFS) Scheduler
 *
 * OS Scheduler project  –  CMP N303
 *
 * Single-CPU FCFS implementation (non-preemptive).
 *
 * ─── Bug fixes applied ───────────────────────────────────────────────────────
 *
 * BUG 1 (runtime=0, WTA=inf):
 *   When a process has runtime=0, dividing TA/runtime produces +Inf, which
 *   poisons total_wta -> Avg WTA = inf, Std WTA = -nan.
 *   Fix: if runtime==0, set WTA=0.0 (process consumed no CPU, so its
 *   weighted turnaround is defined as zero).  It is still counted in
 *   waiting-time and completion statistics.
 *
 * BUG 2 (runtime=0, dispatch logic):
 *   A process with runtime=0 has expected_finish = start_time + 0 = start_time.
 *   The child (process.c) exits instantly (its while loop never runs).
 *   We handle it by doing a blocking waitpid immediately after fork, then
 *   logging "finished" in the same tick as "started".
 *
 * BUG 3 (finish detection uses now instead of expected_finish):
 *   The finish event must be logged at start_time+runtime (canonical tick),
 *   not at the tick the scheduler happens to poll waitpid successfully.
 *   Already fixed via expected_finish = start_time + runtime.
 *
 * ─── Invariants ──────────────────────────────────────────────────────────────
 *
 *   start_time    = max(cpu_free_at, process.arrival)
 *   finish_time   = start_time + runtime          (canonical, integer ticks)
 *   waiting_time  = start_time - arrival_time
 *   cpu_free_at   = finish_time + 1               (1-tick context-switch overhead)
 *   WTA           = TA / runtime  if runtime > 0, else 0.0
 *   CPU util      = Σ(runtimes) / last_finish_time × 100
 *
 * ─────────────────────────────────────────────────────────────────────────────
 */

#include "headers.h"
#include "shared.h"
#include <string.h>
#include <math.h>

/* ═══════════════════════════════════════════════════════════════════════════
 *  Scheduler state
 * ═══════════════════════════════════════════════════════════════════════════ */

static ReadyQueue *ready_queue;
static PCB         current;       /* current.id == -1  →  CPU idle */
static int         all_arrived;   /* 1 after mtype-2 sentinel received */
static int         cpu_free_at;   /* earliest tick a new process may start */
static int         msgqid;
static FILE       *log_fp;

/* ═══════════════════════════════════════════════════════════════════════════
 *  Performance counters
 * ═══════════════════════════════════════════════════════════════════════════ */

static int    num_completed;
static int    total_runtime;    /* Σ runtimes (numerator of CPU utilisation) */
static int    total_waiting;    /* Σ waiting times                            */
static double total_wta;        /* Σ WTA values (0.0 when runtime=0)          */
static double wta_arr[1024];    /* per-process WTA for std-dev                */
static int    last_finish;      /* finish tick of the last completed process  */

/* ═══════════════════════════════════════════════════════════════════════════
 *  Logging
 * ═══════════════════════════════════════════════════════════════════════════ */

/* Arrival line: uses process.arrival (canonical), NOT the poll tick. */
static void log_arrived(const PCB *p)
{
    if (!log_fp) return;
    fprintf(log_fp,
        "At time %d process %d arrived arr %d total %d remain %d wait %d\n",
        p->arrival, p->id,
        p->arrival, p->runtime, p->runtime, 0);
    fflush(log_fp);
}

/*
 * Event line for started / resumed / stopped / finished.
 *
 * Spec format:
 *   started|resumed|stopped:
 *     At time X process Y <state> arr A total T remain R wait W
 *   finished:
 *     At time X process Y finished arr A total T remain 0 wait W TA t WTA w
 *
 * WTA guard: if runtime==0 we report WTA=0.00 (not inf/nan).
 */
static void log_event(int time, const PCB *p, const char *state)
{
    if (!log_fp) return;

    if (strcmp(state, "finished") == 0)
    {
        int    ta  = p->finish_time - p->arrival;
        /* FIX BUG 1: guard against division by zero */
        double wta = (p->runtime > 0)
                     ? (double)ta / (double)p->runtime
                     : 0.0;
        fprintf(log_fp,
            "At time %d process %d finished arr %d total %d remain %d "
            "wait %d TA %d WTA %.2f\n",
            time, p->id,
            p->arrival, p->runtime, 0, p->waiting, ta, wta);
    }
    else
    {
        fprintf(log_fp,
            "At time %d process %d %s arr %d total %d remain %d wait %d\n",
            time, p->id, state,
            p->arrival, p->runtime, p->remaining, p->waiting);
    }
    fflush(log_fp);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Message queue
 * ═══════════════════════════════════════════════════════════════════════════ */

static void receive_new_processes(int now)
{
    Message msg;
    while (msgrcv(msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) > 0)
    {
        if (msg.mtype == 1)
        {
            PCB p       = msg.proc;
            p.remaining = p.runtime;
            p.waiting   = 0;
            p.started   = 0;
            p.finished  = 0;

            queue_push_tail(ready_queue, p);
            log_arrived(&p);

            printf("[Scheduler] t=%d  Queued P%d  (arr=%d run=%d pri=%d)\n",
                   now, p.id, p.arrival, p.runtime, p.priority);
        }
        else if (msg.mtype == 2)
        {
            all_arrived = 1;
            printf("[Scheduler] Sentinel received — no more processes.\n");
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Dispatch
 * ═══════════════════════════════════════════════════════════════════════════ */

/*
 * Fork ./process.out <remaining_time> and update PCB fields.
 * For runtime==0 the child exits before we even return, which is fine:
 * waitpid in schedule_tick will reap it immediately.
 */
static void dispatch_process(PCB *p, int start_time)
{
    p->start_time = start_time;
    p->started    = 1;
    p->waiting    = start_time - p->arrival;

    pid_t pid = fork();
    if (pid == 0)
    {
        char rem[16];
        snprintf(rem, sizeof(rem), "%d", p->remaining);
        execl("./process.out", "process.out", rem, NULL);
        perror("[Process] execl failed");
        exit(EXIT_FAILURE);
    }
    else if (pid < 0)
    {
        perror("[Scheduler] fork");
        exit(EXIT_FAILURE);
    }
    p->pid = pid;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Complete a process: record stats and free the CPU
 * ═══════════════════════════════════════════════════════════════════════════ */

static void complete_process(PCB *p, int finish_time)
{
    p->finish_time = finish_time;
    p->remaining   = 0;
    p->finished    = 1;

    log_event(finish_time, p, "finished");

    int    ta  = finish_time - p->arrival;
    /* FIX BUG 1: WTA = 0.0 when runtime == 0 (no division by zero) */
    double wta = (p->runtime > 0)
                 ? (double)ta / (double)p->runtime
                 : 0.0;

    total_wta              += wta;
    wta_arr[num_completed]  = wta;
    total_runtime          += p->runtime;
    total_waiting          += p->waiting;
    num_completed++;
    last_finish = finish_time;

    printf("[Scheduler] t=%d  P%d finished  (TA=%d WTA=%.2f)\n",
           finish_time, p->id, ta, wta);

    /* 1-tick context-switch overhead */
    cpu_free_at = finish_time + 1;
    current.id  = -1;
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Per-tick scheduling step
 * ═══════════════════════════════════════════════════════════════════════════ */

static void schedule_tick(int now)
{
    /* ── A. Check if the running process has finished ── */
    if (current.id != -1)
    {
        int expected_finish = current.start_time + current.runtime;

        if (now >= expected_finish)
        {
            /*
             * Wait for child to exit.
             * For runtime > 0: process.c counts exactly `runtime` ticks,
             *   exits at tick start+runtime.  We spin up to 1200 ms.
             * For runtime == 0: child exits instantly; first poll succeeds.
             */
            int   status = 0;
            pid_t res    = 0;
            int   waited = 0;

            while (waited < 1200)
            {
                res = waitpid(current.pid, &status, WNOHANG);
                if (res > 0) break;
                usleep(10000);   /* 10 ms */
                waited += 10;
            }

            if (res > 0)
            {
                complete_process(&current, expected_finish);
            }
            else
            {
                /*
                 * Child hasn't exited within 1200 ms.
                 * This is abnormal — warn and retry next tick.
                 * The canonical finish_time is still expected_finish,
                 * so the log will be correct when we reap next tick.
                 */
                fprintf(stderr,
                    "[Scheduler] WARNING: t=%d  P%d expected finish=%d "
                    "but still running (pid=%d). Retrying.\n",
                    now, current.id, expected_finish, current.pid);
            }
        }
    }

    /* ── B. Dispatch next process if CPU is free ── */
    if (current.id == -1)
    {
        PCB *front = queue_peek(ready_queue);
        if (front != NULL)
        {
            /*
             * Canonical start time = max(cpu_free_at, arrival).
             *
             * If the CPU was idle when this process arrived (cpu_free_at <=
             * arrival), it should start at its arrival tick.  This corrects
             * for any scheduler startup latency that caused us to see the
             * process one tick late.
             *
             * We only dispatch if now >= start_time (never jump ahead of clock).
             */
            int start_time = (cpu_free_at > front->arrival)
                             ? cpu_free_at
                             : front->arrival;

            if (now >= start_time)
            {
                PCB next;
                queue_pop(ready_queue, &next);
                dispatch_process(&next, start_time);
                current = next;

                log_event(start_time, &current, "started");

                printf("[Scheduler] t=%d  Started P%d  "
                       "(pid=%d run=%d wait=%d)\n",
                       start_time, current.id,
                       current.pid, current.runtime, current.waiting);

                /*
                 * FIX BUG 2: Handle runtime==0 immediately.
                 * The child exits before we can poll it next tick,
                 * and expected_finish == start_time, so the completion
                 * check above won't trigger until 'now >= start_time'
                 * which is already true right now.  Call it here directly.
                 */
                if (current.runtime == 0)
                {
                    /* Reap the child — it exited instantly */
                    int   status = 0;
                    pid_t res    = 0;
                    int   waited = 0;
                    while (waited < 1200)
                    {
                        res = waitpid(current.pid, &status, WNOHANG);
                        if (res > 0) break;
                        usleep(10000);
                        waited += 10;
                    }
                    if (res > 0)
                    {
                        complete_process(&current, start_time);
                    }
                    /* If it still hasn't exited, next tick's poll will catch it */
                }
            }
        }
    }
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  Performance report
 * ═══════════════════════════════════════════════════════════════════════════ */

static void write_perf(void)
{
    FILE *fp = fopen("scheduler.perf", "w");
    if (!fp) { perror("scheduler.perf"); return; }

    /*
     * CPU utilisation = Σ(burst times) / last_finish_time × 100
     * (A process with runtime=0 contributes 0 to the numerator.)
     */
    double util     = (last_finish > 0)
                      ? (double)total_runtime / last_finish * 100.0
                      : 0.0;
    double avg_wta  = (num_completed > 0)
                      ? total_wta / num_completed
                      : 0.0;
    double avg_wait = (num_completed > 0)
                      ? (double)total_waiting / num_completed
                      : 0.0;

    /* Population std-dev of WTA */
    double var = 0.0;
    for (int i = 0; i < num_completed; i++)
    {
        double d = wta_arr[i] - avg_wta;
        var += d * d;
    }
    double std_wta = (num_completed > 0) ? sqrt(var / num_completed) : 0.0;

    fprintf(fp, "CPU utilization = %.2f%%\n", util);
    fprintf(fp, "Avg WTA = %.2f\n",           avg_wta);
    fprintf(fp, "Avg Waiting = %.2f\n",        avg_wait);
    fprintf(fp, "Std WTA = %.2f\n",            std_wta);
    fclose(fp);

    printf("\n=== Performance Summary ===\n");
    printf("CPU utilization = %.2f%%\n", util);
    printf("Avg WTA         = %.2f\n",   avg_wta);
    printf("Avg Waiting     = %.2f\n",   avg_wait);
    printf("Std WTA         = %.2f\n",   std_wta);
}

/* ═══════════════════════════════════════════════════════════════════════════
 *  main
 * ═══════════════════════════════════════════════════════════════════════════ */

int main(int argc, char *argv[])
{
    int algo   = (argc > 1) ? atoi(argv[1]) : 0;
    int param2 = (argc > 2) ? atoi(argv[2]) : 0;
    int param3 = (argc > 3) ? atoi(argv[3]) : 0;
    (void)param2; (void)param3;

    printf("[Scheduler] Starting  algo=%d param2=%d param3=%d\n",
           algo, param2, param3);

    /* Attach to message queue */
    msgqid = msgget(MSG_KEY, 0644);
    while (msgqid == -1)
    {
        usleep(50000);
        msgqid = msgget(MSG_KEY, 0644);
    }
    printf("[Scheduler] MQ attached (id=%d key=%d)\n", msgqid, MSG_KEY);

    /* Connect to clock */
    initClk();
    printf("[Scheduler] Clock ready.\n");

    /* Open log */
    log_fp = fopen("scheduler.log", "w");
    if (!log_fp) { perror("scheduler.log"); exit(EXIT_FAILURE); }
    fprintf(log_fp, "#At time x process y state arr w total z remain y wait k\n");
    fflush(log_fp);

    /* Initialise state */
    ready_queue   = queue_create();
    current.id    = -1;
    all_arrived   = 0;
    cpu_free_at   = 0;
    num_completed = 0;
    total_runtime = 0;
    total_waiting = 0;
    total_wta     = 0.0;
    last_finish   = 0;

    /* ── Main loop ── */
    int prev_tick = -1, now;
    for (;;)
    {
        now = getClk();
        if (now != prev_tick)
        {
            prev_tick = now;
            receive_new_processes(now);
            schedule_tick(now);

            if (all_arrived && ready_queue->size == 0 && current.id == -1)
            {
                printf("[Scheduler] All processes finished at t=%d.\n", now);
                break;
            }
        }
        usleep(10000);
    }

    write_perf();
    if (log_fp) fclose(log_fp);
    free(ready_queue);
    destroyClk(true);
    return 0;
}