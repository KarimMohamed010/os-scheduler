#include "headers.h"
#include "scheduler_ops.h"
#include "RR.h"
#include "HPF.h"
#include "fcfs.h"
#include <errno.h>
#include <string.h>

typedef struct
{
    int cpu_id;
    int cpu_index;

    int msgqid;
    int ctrl_shmid;
    FCFS2Control *ctrl;
    int consume_semid;
    int ack_semid;
    int steal_semid;
    int child_tick_semid;
    int child_tick_ack_semid;
    int ctrl_mutex_semid;

    int has_running;
    int finish_pending;
    int penalty_paused;
    int done_reported;
    int done_tick;
    int last_clk;
    int next_dispatch_time;
    PCB running;

    FCFSState fcfs_state;
    FILE *log_file;

    int finished_processes;
    int busy_ticks;
    long long total_waiting;
    double total_wta;
    double total_wta_sq;
} FCFS2ChildContext;

static int sem_up_idx(int semid, unsigned short sem_num)
{
    struct sembuf op;

    op.sem_num = sem_num;
    op.sem_op = 1;
    op.sem_flg = 0;

    while (semop(semid, &op, 1) == -1)
    {
        if (errno == EINTR)
        {
            continue;
        }
        perror("semop up");
        return 0;
    }

    return 1;
}

static int sem_try_down_idx(int semid, unsigned short sem_num)
{
    struct sembuf op;

    op.sem_num = sem_num;
    op.sem_op = -1;
    op.sem_flg = IPC_NOWAIT;

    while (semop(semid, &op, 1) == -1)
    {
        if (errno == EINTR)
        {
            continue;
        }
        if (errno == EAGAIN)
        {
            return 0;
        }
        perror("semop try down");
        return -1;
    }

    return 1;
}

static int sem_down_idx(int semid, unsigned short sem_num)
{
    struct sembuf op;

    op.sem_num = sem_num;
    op.sem_op = -1;
    op.sem_flg = 0;

    while (semop(semid, &op, 1) == -1)
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

static double fast_sqrt(double x);
static double round_2dp_half_up(double value);

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
        wta = round_2dp_half_up(wta);
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

static double round_2dp_half_up(double value)
{
    if (value >= 0.0)
    {
        return (double)((long long)(value * 100.0 + 0.5)) / 100.0;
    }
    return (double)((long long)(value * 100.0 - 0.5)) / 100.0;
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

    /* A preemption/finish at time t should not let another process consume the same tick twice. */
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

    /* `finish_pending` means the model reached zero during the previous tick and must be committed now. */
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

    /* Wait until generator finishes sending this tick before consuming arrivals. */
    if (!ctx->all_received && !wait_for_generator_tick(ctx))
    {
        return;
    }

    /* Tick boundary order: ingest arrivals, settle finish/preemption, dispatch, then execute this tick. */
    if(ctx->algo == ALGO_HPF)
    {
         /* HPF must see same-tick arrivals before the preemption decision. */
         receive_current_processes(ctx, now);
    }

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
    if(ctx->algo == ALGO_RR)
    {
        /* RR defers new arrivals until after the current slice/preemption checks for this boundary. */
        receive_current_processes(ctx, now);
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

static void child_log_header(FCFS2ChildContext *ctx)
{
    fprintf(ctx->log_file, "#At time x process y state arr w total z remain y wait k\n");
    fflush(ctx->log_file);
}

static int child_ctrl_lock(FCFS2ChildContext *ctx)
{
    return sem_down_idx(ctx->ctrl_mutex_semid, 0);
}

static int child_ctrl_unlock(FCFS2ChildContext *ctx)
{
    return sem_up_idx(ctx->ctrl_mutex_semid, 0);
}

static void child_log_event(FCFS2ChildContext *ctx, int time, const char *state, const PCB *proc)
{
    int ta;
    double wta;

    if (strcmp(state, "finished") == 0)
    {
        ta = proc->finish_time - proc->arrival;
        wta = (double)ta / (double)((proc->runtime > 0) ? proc->runtime : 1);
        wta = round_2dp_half_up(wta);
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

static void child_log_stolen(FCFS2ChildContext *ctx, int time, int proc_id)
{
    fprintf(ctx->log_file, "At time %d process %d was stolen\n", time, proc_id);
    fflush(ctx->log_file);
}

static void child_update_running_metric(FCFS2ChildContext *ctx, int value)
{
    if (!child_ctrl_lock(ctx))
    {
        return;
    }
    ctx->ctrl->running_remaining[ctx->cpu_index] = value;
    child_ctrl_unlock(ctx);
}

static void child_account_enqueue(FCFS2ChildContext *ctx, const PCB *proc)
{
    if (!child_ctrl_lock(ctx))
    {
        return;
    }
    ctx->ctrl->queue_size[ctx->cpu_index]++;
    ctx->ctrl->ready_remaining[ctx->cpu_index] += proc->remaining;
    child_ctrl_unlock(ctx);
}

static void child_account_dequeue(FCFS2ChildContext *ctx, const PCB *proc)
{
    if (!child_ctrl_lock(ctx))
    {
        return;
    }

    ctx->ctrl->queue_size[ctx->cpu_index]--;
    if (ctx->ctrl->queue_size[ctx->cpu_index] < 0)
    {
        ctx->ctrl->queue_size[ctx->cpu_index] = 0;
    }

    ctx->ctrl->ready_remaining[ctx->cpu_index] -= proc->remaining;
    if (ctx->ctrl->ready_remaining[ctx->cpu_index] < 0)
    {
        ctx->ctrl->ready_remaining[ctx->cpu_index] = 0;
    }

    child_ctrl_unlock(ctx);
}

static void child_dispatch_next(FCFS2ChildContext *ctx, int now)
{
    PCB next;

    if (now < ctx->next_dispatch_time)
    {
        return;
    }

    if (!fcfs_dequeue(&ctx->fcfs_state, &next))
    {
        return;
    }

    child_account_dequeue(ctx, &next);

    if (!next.started)
    {
        pid_t pid;
        char runtime_str[32];

        pid = fork();
        if (pid == -1)
        {
            perror("fork process");
            fcfs_enqueue(&ctx->fcfs_state, next);
            child_account_enqueue(ctx, &next);
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
        child_log_event(ctx, now, "started", &next);
    }
    else
    {
        kill(next.pid, SIGCONT);
        refresh_waiting(&next, now);
        child_log_event(ctx, now, "resumed", &next);
    }

    ctx->running = next;
    ctx->has_running = 1;
    ctx->finish_pending = 0;
    child_update_running_metric(ctx, ctx->running.remaining);
}

static void child_finish_running(FCFS2ChildContext *ctx, int now)
{
    int turnaround;
    int uncounted_ticks;
    double wta;

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

    child_log_event(ctx, now, "finished", &ctx->running);

    ctx->has_running = 0;
    ctx->finish_pending = 0;
    ctx->penalty_paused = 0;
    ctx->running.id = -1;
    ctx->next_dispatch_time = now + 1;
    child_update_running_metric(ctx, 0);
}

static int child_settle_pending_finish(FCFS2ChildContext *ctx, int now)
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
        child_finish_running(ctx, now);
        return 1;
    }

    if (done == -1 && errno == EINTR)
    {
        return 0;
    }

    child_finish_running(ctx, now);
    return 1;
}

static int child_check_running_finished(FCFS2ChildContext *ctx, int now)
{
    int status;
    pid_t done;

    if (!ctx->has_running)
    {
        return 0;
    }

    done = waitpid(ctx->running.pid, &status, WNOHANG);
    if (done == ctx->running.pid)
    {
        child_finish_running(ctx, now);
        return 1;
    }

    if (done == 0)
    {
        return 0;
    }

    if (done == -1 && errno == ECHILD)
    {
        child_finish_running(ctx, now);
        return 1;
    }

    return 0;
}

static int child_apply_penalty(FCFS2ChildContext *ctx, int now)
{
    int penalty_until;

    if (!child_ctrl_lock(ctx))
    {
        return 0;
    }
    penalty_until = ctx->ctrl->penalty_until;
    if (!child_ctrl_unlock(ctx))
    {
        return 0;
    }

    if (penalty_until > now)
    {
        /* Work stealing charges a global pause; both CPUs must effectively lose these ticks. */
        if (ctx->has_running && !ctx->penalty_paused)
        {
            kill(ctx->running.pid, SIGSTOP);
            ctx->penalty_paused = 1;
        }

        if (ctx->next_dispatch_time > now)
        {
            ctx->next_dispatch_time++;
        }

        return 1;
    }

    if (ctx->has_running && ctx->penalty_paused)
    {
        kill(ctx->running.pid, SIGCONT);
        ctx->penalty_paused = 0;
    }

    return 0;
}

static void child_tick(FCFS2ChildContext *ctx, int now)
{
    /* Process completions BEFORE penalty to ensure correct timing and dispatch calculations */
    if (ctx->has_running)
    {
        int finished = 0;
        if (ctx->finish_pending)
        {
            if (child_settle_pending_finish(ctx, now))
            {
                finished = 1;
            }
        }
        else if (child_check_running_finished(ctx, now))
        {
            finished = 1;
        }
    }

    if (child_apply_penalty(ctx, now))
    {
        return;
    }

    if (!ctx->has_running)
    {
        child_dispatch_next(ctx, now);
    }

    if (ctx->has_running && ctx->running.remaining > 0)
    {
        ctx->running.remaining--;
        ctx->busy_ticks++;
        child_update_running_metric(ctx, ctx->running.remaining);

        if (ctx->running.remaining == 0)
        {
            ctx->finish_pending = 1;
        }
    }
}

static void child_consume_one_message(FCFS2ChildContext *ctx)
{
    Message msg;

    if (msgrcv(ctx->msgqid, &msg, sizeof(PCB), 0, IPC_NOWAIT) == -1)
    {
        if (errno != ENOMSG && errno != EINTR)
        {
            perror("msgrcv child");
        }
        return;
    }

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

        fcfs_enqueue(&ctx->fcfs_state, proc);
        child_account_enqueue(ctx, &proc);
    }
    else if (msg.mtype == 2)
    {
        if (!child_ctrl_lock(ctx))
        {
            return;
        }
        ctx->ctrl->all_received = 1;
        child_ctrl_unlock(ctx);
    }
}

static int child_handle_consume_grants(FCFS2ChildContext *ctx)
{
    while (1)
    {
        int sem_state = sem_try_down_idx(ctx->consume_semid, (unsigned short)ctx->cpu_index);
        if (sem_state < 0)
        {
            return 0;
        }
        if (sem_state == 0)
        {
            break;
        }

        {
            int shutdown = 0;
            if (!child_ctrl_lock(ctx))
            {
                return 0;
            }
            shutdown = ctx->ctrl->shutdown;
            if (!child_ctrl_unlock(ctx))
            {
                return 0;
            }

            if (!shutdown)
            {
                /* The master decides which child may consume exactly one pending generator message. */
                child_consume_one_message(ctx);
            }
        }

        if (!sem_up_idx(ctx->ack_semid, 0))
        {
            return 0;
        }
    }

    return 1;
}

static int child_handle_steal_commands(FCFS2ChildContext *ctx)
{
    while (1)
    {
        int sem_state = sem_try_down_idx(ctx->steal_semid, (unsigned short)ctx->cpu_index);
        if (sem_state < 0)
        {
            return 0;
        }
        if (sem_state == 0)
        {
            break;
        }

        {
            int steal_pending;
            int steal_from;
            int steal_to;
            int has_stolen;
            int current_tick;

            if (!child_ctrl_lock(ctx))
            {
                return 0;
            }
            steal_pending = ctx->ctrl->steal_pending;
            steal_from = ctx->ctrl->steal_from;
            steal_to = ctx->ctrl->steal_to;
            has_stolen = ctx->ctrl->has_stolen;
            current_tick = ctx->ctrl->current_tick;
            if (!child_ctrl_unlock(ctx))
            {
                return 0;
            }

            if (steal_pending)
            {
                if (steal_from == ctx->cpu_id)
                {
                    PCB stolen;

                    /* Steal from the tail so the donor keeps its oldest FCFS work in arrival order. */
                    if (fcfs_steal_tail(&ctx->fcfs_state, &stolen))
                    {
                        child_account_dequeue(ctx, &stolen);
                        child_log_stolen(ctx, current_tick, stolen.id);

                        if (!child_ctrl_lock(ctx))
                        {
                            return 0;
                        }
                        ctx->ctrl->stolen_proc = stolen;
                        ctx->ctrl->has_stolen = 1;
                        if (!child_ctrl_unlock(ctx))
                        {
                            return 0;
                        }
                    }
                    else
                    {
                        if (!child_ctrl_lock(ctx))
                        {
                            return 0;
                        }
                        ctx->ctrl->has_stolen = 0;
                        if (!child_ctrl_unlock(ctx))
                        {
                            return 0;
                        }
                    }
                }
                else if (steal_to == ctx->cpu_id && has_stolen)
                {
                    PCB stolen;

                    if (!child_ctrl_lock(ctx))
                    {
                        return 0;
                    }
                    stolen = ctx->ctrl->stolen_proc;
                    ctx->ctrl->has_stolen = 0;
                    if (!child_ctrl_unlock(ctx))
                    {
                        return 0;
                    }

                    fcfs_enqueue(&ctx->fcfs_state, stolen);
                    child_account_enqueue(ctx, &stolen);
                }
            }
        }

        if (!sem_up_idx(ctx->ack_semid, 0))
        {
            return 0;
        }
    }

    return 1;
}

static void child_write_perf_file(const FCFS2ChildContext *ctx, int total_time)
{
    char perf_name[32];
    FILE *perf;
    double cpu_util;
    double avg_wta;
    double avg_wait;
    double variance;
    double std_wta;

    snprintf(perf_name, sizeof(perf_name), "scheduler_%d.perf", ctx->cpu_id);
    perf = fopen(perf_name, "w");
    if (!perf)
    {
        perror("fopen child perf");
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

static int child_handle_tick_grants(FCFS2ChildContext *ctx)
{
    while (1)
    {
        int tick;
        int sem_state = sem_try_down_idx(ctx->child_tick_semid, (unsigned short)ctx->cpu_index);
        if (sem_state < 0)
        {
            return 0;
        }
        if (sem_state == 0)
        {
            break;
        }

        if (!child_ctrl_lock(ctx))
        {
            return 0;
        }
        tick = ctx->ctrl->current_tick;
        if (!child_ctrl_unlock(ctx))
        {
            return 0;
        }

        /* Children advance only when the master publishes the authoritative clock tick. */
        ctx->last_clk = tick;
        child_tick(ctx, tick);
        if (!sem_up_idx(ctx->child_tick_ack_semid, 0))
        {
            return 0;
        }
    }

    return 1;
}

static int run_fcfs2_child(int argc, char *argv[])
{
    FCFS2ChildContext ctx;
    char log_name[32];

    memset(&ctx, 0, sizeof(ctx));
    ctx.ctrl_shmid = -1;
    ctx.running.id = -1;
    ctx.next_dispatch_time = 0;

    if (argc < 5)
    {
        fprintf(stderr, "FCFS-2 child usage: %s 3 <N> <M> <cpu_id>\n", argv[0]);
        return 1;
    }

    ctx.cpu_id = atoi(argv[4]);
    if (ctx.cpu_id != 1 && ctx.cpu_id != 2)
    {
        fprintf(stderr, "Invalid cpu_id for FCFS-2 child: %d\n", ctx.cpu_id);
        return 1;
    }
    ctx.cpu_index = ctx.cpu_id - 1;

    ctx.msgqid = msgget(MSG_KEY, 0666 | IPC_CREAT);
    if (ctx.msgqid == -1)
    {
        perror("msgget child");
        return 1;
    }

    ctx.ctrl_shmid = shmget(FCFS2_CTRL_SHM_KEY, sizeof(FCFS2Control), 0666 | IPC_CREAT);
    if (ctx.ctrl_shmid == -1)
    {
        perror("shmget child");
        return 1;
    }

    ctx.ctrl = (FCFS2Control *)shmat(ctx.ctrl_shmid, NULL, 0);
    if (ctx.ctrl == (void *)-1)
    {
        perror("shmat child");
        return 1;
    }

    ctx.consume_semid = semget(FCFS2_CONSUME_SEM_KEY, 2, 0666 | IPC_CREAT);
    ctx.ack_semid = semget(FCFS2_ACK_SEM_KEY, 1, 0666 | IPC_CREAT);
    ctx.steal_semid = semget(FCFS2_STEAL_SEM_KEY, 2, 0666 | IPC_CREAT);
    ctx.child_tick_semid = semget(FCFS2_CHILD_TICK_SEM_KEY, 2, 0666 | IPC_CREAT);
    ctx.child_tick_ack_semid = semget(FCFS2_CHILD_TICK_ACK_SEM_KEY, 1, 0666 | IPC_CREAT);
    ctx.ctrl_mutex_semid = semget(FCFS2_CTRL_MUTEX_SEM_KEY, 1, 0666 | IPC_CREAT);
    if (ctx.consume_semid == -1 || ctx.ack_semid == -1 || ctx.steal_semid == -1 ||
        ctx.child_tick_semid == -1 || ctx.child_tick_ack_semid == -1 || ctx.ctrl_mutex_semid == -1)
    {
        perror("semget child");
        shmdt(ctx.ctrl);
        return 1;
    }

    fcfs_init(&ctx.fcfs_state);

    snprintf(log_name, sizeof(log_name), "scheduler_%d.log", ctx.cpu_id);
    ctx.log_file = fopen(log_name, "w");
    if (!ctx.log_file)
    {
        perror("fopen child log");
        shmdt(ctx.ctrl);
        return 1;
    }
    child_log_header(&ctx);

    initClk();
    ctx.last_clk = getClk();

    if (!child_ctrl_lock(&ctx))
    {
        fclose(ctx.log_file);
        shmdt(ctx.ctrl);
        destroyClk(false);
        return 1;
    }
    ctx.ctrl->child_ready[ctx.cpu_index] = 1;
    if (!child_ctrl_unlock(&ctx))
    {
        fclose(ctx.log_file);
        shmdt(ctx.ctrl);
        destroyClk(false);
        return 1;
    }

    while (1)
    {
        if (!child_handle_consume_grants(&ctx) || !child_handle_steal_commands(&ctx) ||
            !child_handle_tick_grants(&ctx))
        {
            break;
        }

        {
            int shutdown = 0;
            int all_received = 0;

            if (!child_ctrl_lock(&ctx))
            {
                break;
            }
            shutdown = ctx.ctrl->shutdown;
            all_received = ctx.ctrl->all_received;
            if (!child_ctrl_unlock(&ctx))
            {
                break;
            }

            if (shutdown)
            {
                break;
            }

            if (all_received && !ctx.has_running && !fcfs_has_ready(&ctx.fcfs_state))
            {
                /* Report completion once, after both the local queue and current process are drained. */
                if (!ctx.done_reported)
                {
                    if (!child_ctrl_lock(&ctx))
                    {
                        break;
                    }
                    ctx.ctrl->child_done[ctx.cpu_index] = 1;
                    if (!child_ctrl_unlock(&ctx))
                    {
                        break;
                    }
                    ctx.done_reported = 1;
                    ctx.done_tick = ctx.last_clk;
                }
            }
        }

        usleep(1000);
    }

    child_write_perf_file(&ctx, (ctx.done_tick > 0) ? ctx.done_tick : ctx.last_clk);
    fclose(ctx.log_file);

    while (queue_pop(ctx.fcfs_state.ready_queue, &ctx.running))
    {
    }
    free(ctx.fcfs_state.ready_queue);

    shmdt(ctx.ctrl);
    destroyClk(false);
    return 0;
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

    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <algo> [quantum] [N] [M]\n", argv[0]);
        return 1;
    }

    if (atoi(argv[1]) == ALGO_FCFS_2 && argc >= 5)
    {
        return run_fcfs2_child(argc, argv);
    }

    memset(&ctx, 0, sizeof(ctx));
    ctx.running.id = -1;

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