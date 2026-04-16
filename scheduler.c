#include "headers.h"
#include "scheduler_ops.h"
#include "RR.h"
#include "HPF.h"
#include "fcfs.h"
#include <errno.h>
#include <string.h>

typedef struct
{
    int msgqid;
    int tick_semid;
    int algo;
    int all_received;
    int has_running;
    int finish_pending;
    int last_clk;
    int next_dispatch_time;

    PCB running;
    SchedulerOps ops;
    void *algo_state;

    RRState rr_state;
    HPFState hpf_state;
    FCFSState fcfs_state;

    FILE *log_file;

    int total_processes;
    int finished_processes;
    int busy_ticks;
    long long total_waiting;
    double total_wta;
    double total_wta_sq;
} SchedulerContext;

static int scheduler_done(const SchedulerContext *ctx)
{
    /* Stop only after end-of-input and all work (running + ready queues) is drained. */
    return ctx->all_received && !ctx->has_running && !ctx->ops.has_ready(ctx->algo_state);
}

static void log_header(SchedulerContext *ctx)
{
    fprintf(ctx->log_file, "#At time x process y state arr w total z remain y wait k\n");
    fflush(ctx->log_file);
}

static void log_event(SchedulerContext *ctx, int time, const char *state, const PCB *proc)
{
    int ta;
    double wta;

    if (strcmp(state, "finished") == 0)
    {
        ta = proc->finish_time - proc->arrival;
        wta = (double)ta / (double)((proc->runtime > 0) ? proc->runtime : 1);
        fprintf(ctx->log_file,
                "At time %d process %d finished arr %d total %d remain %d wait %d TA %d WTA %.2f\n",
                time,
                proc->id,
                proc->arrival,
                proc->runtime,
                proc->remaining,
                proc->waiting,
                ta,
                wta);
    }
    else
    {
        fprintf(ctx->log_file,
                "At time %d process %d %s arr %d total %d remain %d wait %d\n",
                time,
                proc->id,
                state,
                proc->arrival,
                proc->runtime,
                proc->remaining,
                proc->waiting);
    }

    fflush(ctx->log_file);
}

static void refresh_waiting(PCB *proc, int now)
{
    int executed;
    int waiting;

    executed = proc->runtime - proc->remaining;
    waiting = now - proc->arrival - executed;
    if (waiting < 0)
    {
        waiting = 0;
    }
    proc->waiting = waiting;
}

static int receive_current_processes(SchedulerContext *ctx, int now)
{
    Message msg;
    int arrivals = 0;

    /* Drain all currently available IPC messages without blocking this tick. */
    while (msgrcv(ctx->msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) != -1)
    {
        if (msg.mtype == 1)
        {
            PCB proc = msg.proc;
            proc.remaining = proc.runtime;
            proc.waiting = 0;
            proc.start_time = -1;
            proc.finish_time = -1;
            proc.started = 0;
            proc.finished = 0;
            proc.pid = -1;

            ctx->ops.enqueue(ctx->algo_state, proc);
            ctx->total_processes++;
            arrivals++;
        }
        else if (msg.mtype == 2)
        {
            ctx->all_received = 1;
        }
    }

    if (errno != ENOMSG && errno != EINTR)
    {
        perror("msgrcv");
    }

    return arrivals;
}

static int wait_for_generator_tick(SchedulerContext *ctx)
{
    struct sembuf op;

    op.sem_num = 0;
    op.sem_op = -1;
    op.sem_flg = 0;

    while (semop(ctx->tick_semid, &op, 1) == -1)
    {
        if (errno == EINTR)
        {
            continue;
        }
        perror("semop down");
        return 0;
    }

    return 1;
}

static void dispatch_next(SchedulerContext *ctx, int now)
{
    PCB next;

    if (now < ctx->next_dispatch_time)
    {
        return;
    }

    if (!ctx->ops.dequeue(ctx->algo_state, &next))
    {
        return;
    }

    if (!next.started)
    {
        /* First dispatch creates the child process once; later dispatches resume it. */
        pid_t pid;
        char runtime_str[32];

        pid = fork();
        if (pid == -1)
        {
            perror("fork process");
            return;
        }

        if (pid == 0)
        {
            snprintf(runtime_str, sizeof(runtime_str), "%d", next.runtime);
            execl("./process.out", "process.out", runtime_str, NULL);
            perror("execl process.out");
            exit(1);
        }

        next.pid = pid;
        next.started = 1;
        next.start_time = now;
        refresh_waiting(&next, now);
        log_event(ctx, now, "started", &next);
    }
    else
    {
        kill(next.pid, SIGCONT);
        refresh_waiting(&next, now);
        log_event(ctx, now, "resumed", &next);
    }

    ctx->running = next;
    ctx->has_running = 1;
    ctx->finish_pending = 0;
    ctx->ops.on_dispatch(ctx->algo_state);
}

static void finish_running(SchedulerContext *ctx, int now)
{
    int turnaround;
    int uncounted_ticks;
    double wta;

    /*
     * Child execution is clock-driven and can reach zero just before the scheduler
     * handles SIGCHLD at a boundary tick. If that happens, `remaining` may still be
     * positive here and the final consumed tick(s) were not reflected in busy_ticks.
     */
    uncounted_ticks = ctx->running.remaining;
    if (uncounted_ticks > 0)
    {
        ctx->busy_ticks += uncounted_ticks;
    }

    ctx->running.remaining = 0;
    ctx->running.finished = 1;
    ctx->running.finish_time = now;

    turnaround = ctx->running.finish_time - ctx->running.arrival;
    ctx->running.waiting = turnaround - ctx->running.runtime;
    if (ctx->running.waiting < 0)
    {
        ctx->running.waiting = 0;
    }
    wta = (double)turnaround / (double)((ctx->running.runtime > 0) ? ctx->running.runtime : 1);

    ctx->finished_processes++;
    ctx->total_waiting += ctx->running.waiting;
    ctx->total_wta += wta;
    ctx->total_wta_sq += (wta * wta);

    log_event(ctx, now, "finished", &ctx->running);

    ctx->has_running = 0;
    ctx->finish_pending = 0;
    ctx->running.id = -1;
    ctx->next_dispatch_time = now + 1;
}

static int settle_pending_finish(SchedulerContext *ctx, int now)
{
    int status;
    pid_t done;

    if (!ctx->has_running || !ctx->finish_pending)
    {
        return 0;
    }

    done = waitpid(ctx->running.pid, &status, WNOHANG);
    if (done == 0)
    {
        kill(ctx->running.pid, SIGKILL);

        do
        {
            done = waitpid(ctx->running.pid, &status, 0);
        } while (done == -1 && errno == EINTR);
    }

    if (done == ctx->running.pid || (done == -1 && errno == ECHILD))
    {
        finish_running(ctx, now);
        return 1;
    }

    if (done == -1 && errno == EINTR)
    {
        return 0;
    }

    /* Even if child reaping is delayed unexpectedly, keep scheduler model consistent. */
    finish_running(ctx, now);
    return 1;
}

static int check_running_finished(SchedulerContext *ctx, int now)
{
    int status;
    pid_t done;

    if (!ctx->has_running)
    {
        return 0;
    }

    /* Poll non-blocking each boundary tick to avoid SIGCHLD ordering races. */
    done = waitpid(ctx->running.pid, &status, WNOHANG);
    if (done == ctx->running.pid)
    {
        finish_running(ctx, now);
        return 1;
    }

    if (done == 0)
    {
        return 0;
    }

    if (done == -1 && errno == ECHILD)
    {
        /* Child already reaped elsewhere; treat it as finished to keep state consistent. */
        finish_running(ctx, now);
        return 1;
    }

    if (done == -1 && errno == EINTR)
    {
        return 0;
    }

    return 0;
}

static void preempt_running(SchedulerContext *ctx, int now)
{
    kill(ctx->running.pid, SIGSTOP);
    refresh_waiting(&ctx->running, now);
    log_event(ctx, now, "stopped", &ctx->running);
    ctx->ops.enqueue(ctx->algo_state, ctx->running);
    ctx->has_running = 0;
    ctx->running.id = -1;
    ctx->next_dispatch_time = now + 1;
}

static void scheduler_tick(SchedulerContext *ctx, int now)
{
    int arrivals;

    /* Wait until generator finishes sending this tick before consuming arrivals. */
    if (!ctx->all_received && !wait_for_generator_tick(ctx))
    {
        return;
    }

    /* Tick boundary order: ingest arrivals, settle finish/preemption, dispatch, then execute this tick. */
    arrivals = receive_current_processes(ctx, now);
    (void)arrivals;

    if (ctx->has_running)
    {
        if (settle_pending_finish(ctx, now))
        {
            if (!ctx->has_running)
            {
                dispatch_next(ctx, now);
            }
        }

        if (check_running_finished(ctx, now))
        {
            /* If a process just finished, scheduler may dispatch another process at this same boundary. */
            if (!ctx->has_running)
            {
                dispatch_next(ctx, now);
            }
        }

        if (ctx->has_running && ctx->ops.should_preempt(ctx->algo_state, &ctx->running))
        {
            preempt_running(ctx, now);
        }
    }

    if (!ctx->has_running)
    {
        dispatch_next(ctx, now);
    }

    if (ctx->has_running)
    {
        if (ctx->running.remaining > 0)
        {
            ctx->running.remaining--;
            ctx->busy_ticks++;
            ctx->ops.on_tick(ctx->algo_state);

            if (ctx->running.remaining == 0)
            {
                /* Commit finish on the next boundary tick for deterministic log timing. */
                ctx->finish_pending = 1;
            }
        }
    }
}

static double fast_sqrt(double x)
{
    double guess;
    int i;

    if (x <= 0.0)
    {
        return 0.0;
    }

    guess = x;
    for (i = 0; i < 20; i++)
    {
        guess = 0.5 * (guess + x / guess);
    }
    return guess;
}

static void write_perf_file(const SchedulerContext *ctx, int total_time)
{
    FILE *perf;
    double cpu_util;
    double avg_wta;
    double avg_wait;
    double variance;
    double std_wta;

    perf = fopen("scheduler.perf", "w");
    if (!perf)
    {
        perror("fopen scheduler.perf");
        return;
    }

    if (total_time <= 0)
    {
        total_time = 1;
    }

    cpu_util = (double)ctx->busy_ticks * 100.0 / (double)total_time;

    if (ctx->finished_processes > 0)
    {
        avg_wta = ctx->total_wta / (double)ctx->finished_processes;
        avg_wait = (double)ctx->total_waiting / (double)ctx->finished_processes;
        variance = (ctx->total_wta_sq / (double)ctx->finished_processes) - (avg_wta * avg_wta);
        if (variance < 0.0)
        {
            variance = 0.0;
        }
        std_wta = fast_sqrt(variance);
    }
    else
    {
        avg_wta = 0.0;
        avg_wait = 0.0;
        std_wta = 0.0;
    }

    fprintf(perf, "CPU utilization = %.2f%%\n", cpu_util);
    fprintf(perf, "Avg WTA = %.2f\n", avg_wta);
    fprintf(perf, "Avg Waiting = %.2f\n", avg_wait);
    fprintf(perf, "Std WTA = %.2f\n", std_wta);

    fclose(perf);
}

static int setup_ops(SchedulerContext *ctx, int argc, char *argv[])
{
    memset(&ctx->ops, 0, sizeof(ctx->ops));

    if (ctx->algo == ALGO_HPF)
    {
        hpf_bind_ops(&ctx->ops);
        ctx->algo_state = &ctx->hpf_state;
    }
    else if (ctx->algo == ALGO_RR)
    {
        rr_bind_ops(&ctx->ops);
        ctx->algo_state = &ctx->rr_state;
    }
    else if (ctx->algo == ALGO_FCFS_2)
    {
        fcfs_bind_ops(&ctx->ops);
        ctx->algo_state = &ctx->fcfs_state;
        printf("[Scheduler] FCFS-2 selected: running temporary single-queue FCFS mode.\n");
    }
    else
    {
        fprintf(stderr, "Invalid scheduling algorithm: %d\n", ctx->algo);
        return 0;
    }

    ctx->ops.init(ctx->algo_state, argc, argv);
    return 1;
}

static void cleanup_algo_state(SchedulerContext *ctx)
{
    ReadyQueue *q;
    PCB tmp;

    q = ctx->ops.get_ready_queue(ctx->algo_state);
    while (queue_pop(q, &tmp))
    {
    }
    free(q);

    ctx->algo_state = NULL;
}

int main(int argc, char *argv[])
{
    SchedulerContext ctx;
    int now;

    memset(&ctx, 0, sizeof(ctx));
    ctx.running.id = -1;

    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <algo> [quantum] [N] [M]\n", argv[0]);
        return 1;
    }

    ctx.algo = atoi(argv[1]);
    ctx.msgqid = msgget(MSG_KEY, 0666 | IPC_CREAT);
    if (ctx.msgqid == -1)
    {
        perror("msgget");
        return 1;
    }

    ctx.tick_semid = semget(TICK_SYNC_SEM_KEY, 1, 0666 | IPC_CREAT);
    if (ctx.tick_semid == -1)
    {
        perror("semget");
        return 1;
    }

    if (!setup_ops(&ctx, argc, argv))
    {
        return 1;
    }

    ctx.log_file = fopen("scheduler.log", "w");
    if (!ctx.log_file)
    {
        perror("fopen scheduler.log");
        cleanup_algo_state(&ctx);
        return 1;
    }
    log_header(&ctx);

    initClk();

    ctx.last_clk = getClk();
    if (!wait_for_generator_tick(&ctx))
    {
        fclose(ctx.log_file);
        cleanup_algo_state(&ctx);
        return 1;
    }
    receive_current_processes(&ctx, ctx.last_clk);
    dispatch_next(&ctx, ctx.last_clk);

    while (1)
    {
        now = getClk();

        while (ctx.last_clk < now)
        {
            ctx.last_clk++;
            scheduler_tick(&ctx, ctx.last_clk);
        }

        if (scheduler_done(&ctx))
        {
            break;
        }
    }

    write_perf_file(&ctx, ctx.last_clk);
    fclose(ctx.log_file);
    cleanup_algo_state(&ctx);

    destroyClk(true);
    return 0;
}
