#include "headers.h"
#include "shared.h"
#include <errno.h>
#include <string.h>

typedef struct
{
    int msgqid;
    int tick_semid;
    int ctrl_shmid;
    FCFS2Control *ctrl;

    int consume_semid;
    int ack_semid;
    int steal_semid;
    int child_tick_semid;
    int child_tick_ack_semid;
    int ctrl_mutex_semid;
    int tick_done_semid;
    int start_semid;

    int N;
    int M;
    int last_clk;

    /*
     * Steal-check scheduling (Q38/Q43/Q46):
     *   next_N_check : next regular N-multiple at which to check balance.
     *   recheck_at   : if > 0, a re-check is scheduled at this tick
     *                  (set after every steal; cleared after a clean re-check).
     * Regular N-checks are suppressed while recheck_at > 0 so that the
     * re-check chain (steal → +3 → re-check → steal → +3 → …) runs to
     * completion before the normal N-cadence resumes.
     */
    int next_N_check;
    int recheck_at;
    int recheck_heavy;  /* CPU id (1 or 2) that was the heavy side of the last steal.
                         * During a re-check we only steal if that SAME CPU is still
                         * heavier; if the direction has flipped we stop the chain. */

    pid_t child_pids[2];
} MasterContext;

static MasterContext *g_master_ctx = NULL;

static void cleanup_master_ipc(MasterContext *ctx);

static void handle_sigint(int signum)
{
    int i;

    (void)signum;

    if (!g_master_ctx)
    {
        _exit(0);
    }

    if (g_master_ctx->ctrl)
    {
        g_master_ctx->ctrl->shutdown = 1;
    }

    for (i = 0; i < 2; i++)
    {
        if (g_master_ctx->child_pids[i] > 0)
        {
            kill(g_master_ctx->child_pids[i], SIGINT);
        }
    }

    cleanup_master_ipc(g_master_ctx);
    _exit(0);
}

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

static int choose_consumer(const FCFS2Control *ctrl)
{
    /* Tie-break toward CPU 1 to keep routing deterministic when both queues look equally loaded. */
    if (ctrl->queue_size[0] <= ctrl->queue_size[1])
    {
        return 1;
    }
    return 2;
}

static int ctrl_lock(MasterContext *ctx)
{
    return sem_down_idx(ctx->ctrl_mutex_semid, 0);
}

static int ctrl_unlock(MasterContext *ctx)
{
    return sem_up_idx(ctx->ctrl_mutex_semid, 0);
}

static int choose_consumer_locked(MasterContext *ctx)
{
    int cpu;

    if (!ctrl_lock(ctx))
    {
        return 0;
    }

    cpu = choose_consumer(ctx->ctrl);

    if (!ctrl_unlock(ctx))
    {
        return 0;
    }

    return cpu;
}

static int read_done_state(MasterContext *ctx, int *all_received, int *done1, int *done2)
{
    if (!ctrl_lock(ctx))
    {
        return 0;
    }

    *all_received = ctx->ctrl->all_received;
    *done1 = ctx->ctrl->child_done[0];
    *done2 = ctx->ctrl->child_done[1];

    if (!ctrl_unlock(ctx))
    {
        return 0;
    }

    return 1;
}

static int wait_for_children_ready(MasterContext *ctx)
{
    while (1)
    {
        int ready1;
        int ready2;

        if (!ctrl_lock(ctx))
        {
            return 0;
        }

        ready1 = ctx->ctrl->child_ready[0];
        ready2 = ctx->ctrl->child_ready[1];

        if (!ctrl_unlock(ctx))
        {
            return 0;
        }

        if (ready1 && ready2)
        {
            return 1;
        }

        usleep(2000);
    }
}

static int route_pending_messages(MasterContext *ctx)
{
    struct msqid_ds ds;
    unsigned long pending;
    unsigned long i;

    if (msgctl(ctx->msgqid, IPC_STAT, &ds) == -1)
    {
        perror("msgctl IPC_STAT");
        return 0;
    }

    pending = ds.msg_qnum;
    for (i = 0; i < pending; i++)
    {
        int cpu = choose_consumer_locked(ctx);
        if (cpu == 0)
        {
            return 0;
        }

        if (!ctrl_lock(ctx))
        {
            return 0;
        }
        /* Hand off one message at a time so queue accounting stays synchronized with routing decisions. */
        ctx->ctrl->consume_turn = cpu;
        if (!ctrl_unlock(ctx))
        {
            return 0;
        }
        if (!sem_up_idx(ctx->consume_semid, (unsigned short)(cpu - 1)))
        {
            return 0;
        }

        if (!sem_down_idx(ctx->ack_semid, 0))
        {
            return 0;
        }
    }

    return 1;
}

static void maybe_rebalance(MasterContext *ctx, int now)
{
    int is_regular_check;
    int is_recheck;
    int load1, load2, diff;
    int heavy, light;
    int has_stolen;
    int penalty_until;

    if (ctx->N <= 0)
    {
        return;
    }

    is_regular_check = (ctx->recheck_at == 0) && (now == ctx->next_N_check);
    is_recheck       = (ctx->recheck_at > 0)  && (now == ctx->recheck_at);

    if (!is_regular_check && !is_recheck)
    {
        return;
    }

    /* Advance the regular N-check pointer (may be overwritten if we steal). */
    if (is_regular_check)
    {
        ctx->next_N_check = now + ctx->N;
    }

    /* Do not steal while a penalty from a *previous* tick is still in effect. */
    if (!ctrl_lock(ctx)) { return; }
    load1        = ctx->ctrl->ready_remaining[0] + ctx->ctrl->running_remaining[0];
    load2        = ctx->ctrl->ready_remaining[1] + ctx->ctrl->running_remaining[1];
    penalty_until = ctx->ctrl->penalty_until;
    if (!ctrl_unlock(ctx)) { return; }

    if (penalty_until > now)
    {
        return;
    }

    diff = load1 - load2;
    if (diff < 0) { diff = -diff; }

    if (diff <= ctx->M)
    {
        if (is_recheck)
        {
            ctx->next_N_check = ((now / ctx->N) + 1) * ctx->N;
        }
        ctx->recheck_at    = 0;
        ctx->recheck_heavy = 0;
        return;
    }

    /* ── Imbalanced: perform one steal ── */
    if (is_recheck)
    {
        if (ctx->recheck_heavy == 1 && load1 > load2)
        {
            heavy = 1; light = 2;   /* CPU1 still heavy: steal again */
        }
        else if (ctx->recheck_heavy == 2 && load2 > load1)
        {
            heavy = 2; light = 1;   /* CPU2 still heavy: steal again */
        }
        else
        {
            /* Direction flipped or balanced — stop the re-check chain. */
            ctx->next_N_check = ((now / ctx->N) + 1) * ctx->N;
            ctx->recheck_at   = 0;
            ctx->recheck_heavy = 0;
            return;
        }
    }
    else
    {
        /* Regular check: steal from whichever side is heavier. */
        if (load1 > load2) { heavy = 1; light = 2; }
        else               { heavy = 2; light = 1; }
    }

    if (!ctrl_lock(ctx)) { return; }
    ctx->ctrl->steal_pending = 1;
    ctx->ctrl->steal_from    = heavy;
    ctx->ctrl->steal_to      = light;
    ctx->ctrl->has_stolen    = 0;
    if (!ctrl_unlock(ctx)) { return; }

    /* Ask the heavy CPU to hand over its tail process. */
    if (!sem_up_idx(ctx->steal_semid,  (unsigned short)(heavy - 1))) { return; }
    if (!sem_down_idx(ctx->ack_semid, 0)) { return; }

    if (!ctrl_lock(ctx)) { return; }
    has_stolen = ctx->ctrl->has_stolen;
    if (!ctrl_unlock(ctx)) { return; }

    if (!has_stolen)
    {
        /* Nothing to steal from heavy CPU — stop the loop. */
        if (!ctrl_lock(ctx)) { return; }
        ctx->ctrl->steal_pending = 0;
        ctrl_unlock(ctx);
        ctx->recheck_at    = 0;
        ctx->recheck_heavy = 0;
        return;
    }

    /* Deliver the stolen process to the light CPU. */
    if (!sem_up_idx(ctx->steal_semid,  (unsigned short)(light - 1))) { return; }
    if (!sem_down_idx(ctx->ack_semid, 0)) { return; }

    /* Apply 3-tick overhead and schedule re-check. */
    if (!ctrl_lock(ctx)) { return; }
    ctx->ctrl->penalty_until = now + FCFS2_STEAL_OVERHEAD_SEC;
    ctx->ctrl->steal_pending = 0;
    if (!ctrl_unlock(ctx)) { return; }

    ctx->recheck_at    = now + FCFS2_STEAL_OVERHEAD_SEC;
    ctx->recheck_heavy = heavy;   /* remember direction for the re-check */
}

static int process_tick_boundary(MasterContext *ctx, int tick)
{
    int all_received;
    int i;

    if (!ctrl_lock(ctx))
    {
        return 0;
    }
    all_received = ctx->ctrl->all_received;
    if (!ctrl_unlock(ctx))
    {
        return 0;
    }

    if (!all_received && !sem_down_idx(ctx->tick_semid, 0))
    {
        return 0;
    }

    if (!ctrl_lock(ctx))
    {
        return 0;
    }
    /* Publish the tick once so both children use the same boundary for logs and accounting. */
    ctx->ctrl->current_tick = tick;
    if (!ctrl_unlock(ctx))
    {
        return 0;
    }

    if (!route_pending_messages(ctx))
    {
        return 0;
    }

    maybe_rebalance(ctx, tick);

    for (i = 0; i < 2; i++)
    {
        if (!sem_up_idx(ctx->child_tick_semid, (unsigned short)i))
        {
            return 0;
        }
    }

    /* Wait for both children to finish their local tick before releasing the generator side. */
    if (!sem_down_idx(ctx->child_tick_ack_semid, 0) || !sem_down_idx(ctx->child_tick_ack_semid, 0))
    {
        return 0;
    }

    if (!sem_up_idx(ctx->tick_done_semid, 0))
    {
        return 0;
    }

    return 1;
}

static int init_sem_set_zero(int semid, int nsems)
{
    int i;
    unsigned short values[2] = {0, 0};

    if (nsems > 2)
    {
        return 0;
    }

    for (i = 0; i < nsems; i++)
    {
        values[i] = 0;
    }

    if (semctl(semid, 0, SETALL, values) == -1)
    {
        perror("semctl SETALL");
        return 0;
    }

    return 1;
}

static int spawn_children(MasterContext *ctx)
{
    char s_algo[16];
    char s_n[16];
    char s_m[16];
    char s_cpu[16];
    int cpu;

    snprintf(s_algo, sizeof(s_algo), "%d", ALGO_FCFS_2);
    snprintf(s_n, sizeof(s_n), "%d", ctx->N);
    snprintf(s_m, sizeof(s_m), "%d", ctx->M);

    for (cpu = 1; cpu <= 2; cpu++)
    {
        pid_t pid;

        snprintf(s_cpu, sizeof(s_cpu), "%d", cpu);
        pid = fork();
        if (pid == -1)
        {
            perror("fork scheduler child");
            return 0;
        }

        if (pid == 0)
        {
            execl("./scheduler.out", "scheduler.out", s_algo, s_n, s_m, s_cpu, NULL);
            perror("execl scheduler.out");
            exit(1);
        }

        ctx->child_pids[cpu - 1] = pid;
    }

    return 1;
}

static void shutdown_children(MasterContext *ctx)
{
    int i;

    if (!ctx->ctrl)
    {
        return;
    }

    if (ctrl_lock(ctx))
    {
        ctx->ctrl->shutdown = 1;
        ctrl_unlock(ctx);
    }

    for (i = 0; i < 2; i++)
    {
        sem_up_idx(ctx->consume_semid, (unsigned short)i);
        sem_up_idx(ctx->steal_semid, (unsigned short)i);
    }
}

static void cleanup_master_ipc(MasterContext *ctx)
{
    if (ctx->ctrl)
    {
        shmdt(ctx->ctrl);
        ctx->ctrl = NULL;
    }

    if (ctx->ctrl_shmid != -1)
    {
        if (shmctl(ctx->ctrl_shmid, IPC_RMID, NULL) == -1)
        {
            perror("shmctl IPC_RMID");
        }
        ctx->ctrl_shmid = -1;
    }

    if (ctx->consume_semid != -1)
    {
        if (semctl(ctx->consume_semid, 0, IPC_RMID) == -1)
        {
            perror("semctl consume IPC_RMID");
        }
        ctx->consume_semid = -1;
    }

    if (ctx->ack_semid != -1)
    {
        if (semctl(ctx->ack_semid, 0, IPC_RMID) == -1)
        {
            perror("semctl ack IPC_RMID");
        }
        ctx->ack_semid = -1;
    }

    if (ctx->steal_semid != -1)
    {
        if (semctl(ctx->steal_semid, 0, IPC_RMID) == -1)
        {
            perror("semctl steal IPC_RMID");
        }
        ctx->steal_semid = -1;
    }

    if (ctx->child_tick_semid != -1)
    {
        if (semctl(ctx->child_tick_semid, 0, IPC_RMID) == -1)
        {
            perror("semctl child tick IPC_RMID");
        }
        ctx->child_tick_semid = -1;
    }

    if (ctx->child_tick_ack_semid != -1)
    {
        if (semctl(ctx->child_tick_ack_semid, 0, IPC_RMID) == -1)
        {
            perror("semctl child tick ack IPC_RMID");
        }
        ctx->child_tick_ack_semid = -1;
    }

    if (ctx->ctrl_mutex_semid != -1)
    {
        if (semctl(ctx->ctrl_mutex_semid, 0, IPC_RMID) == -1)
        {
            perror("semctl ctrl mutex IPC_RMID");
        }
        ctx->ctrl_mutex_semid = -1;
    }

    ctx->tick_done_semid = -1;
    ctx->start_semid = -1;
}

int main(int argc, char *argv[])
{
    MasterContext ctx;
    int now;

    memset(&ctx, 0, sizeof(ctx));
    ctx.ctrl_shmid = -1;
    ctx.consume_semid = -1;
    ctx.ack_semid = -1;
    ctx.steal_semid = -1;
    ctx.child_tick_semid = -1;
    ctx.child_tick_ack_semid = -1;
    ctx.ctrl_mutex_semid = -1;
    ctx.tick_done_semid = -1;
    ctx.start_semid = -1;

    g_master_ctx = &ctx;
    signal(SIGINT, handle_sigint);

    if (argc < 4)
    {
        fprintf(stderr, "Usage: %s <algo> <N> <M>\n", argv[0]);
        return 1;
    }

    if (atoi(argv[1]) != ALGO_FCFS_2)
    {
        fprintf(stderr, "master scheduler supports FCFS-2 only\n");
        return 1;
    }

    ctx.N = atoi(argv[2]);
    ctx.M = atoi(argv[3]);
    ctx.next_N_check  = ctx.N;  /* first regular check at t=N */
    ctx.recheck_at    = 0;      /* no re-check pending        */
    ctx.recheck_heavy = 0;      /* 0 = not set                */

    ctx.msgqid = msgget(MSG_KEY, 0666 | IPC_CREAT);
    if (ctx.msgqid == -1)
    {
        perror("msgget");
        return 1;
    }

    ctx.tick_semid = semget(TICK_SYNC_SEM_KEY, 1, 0666 | IPC_CREAT);
    if (ctx.tick_semid == -1)
    {
        perror("semget tick");
        return 1;
    }

    ctx.ctrl_shmid = shmget(FCFS2_CTRL_SHM_KEY, sizeof(FCFS2Control), IPC_CREAT | 0666);
    if (ctx.ctrl_shmid == -1)
    {
        perror("shmget");
        return 1;
    }

    ctx.ctrl = (FCFS2Control *)shmat(ctx.ctrl_shmid, NULL, 0);
    if (ctx.ctrl == (void *)-1)
    {
        perror("shmat");
        return 1;
    }
    memset(ctx.ctrl, 0, sizeof(FCFS2Control));

    ctx.consume_semid = semget(FCFS2_CONSUME_SEM_KEY, 2, IPC_CREAT | 0666);
    if (ctx.consume_semid == -1)
    {
        perror("semget consume");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.ack_semid = semget(FCFS2_ACK_SEM_KEY, 1, IPC_CREAT | 0666);
    if (ctx.ack_semid == -1)
    {
        perror("semget ack");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.steal_semid = semget(FCFS2_STEAL_SEM_KEY, 2, IPC_CREAT | 0666);
    if (ctx.steal_semid == -1)
    {
        perror("semget steal");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.child_tick_semid = semget(FCFS2_CHILD_TICK_SEM_KEY, 2, IPC_CREAT | 0666);
    if (ctx.child_tick_semid == -1)
    {
        perror("semget child tick");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.child_tick_ack_semid = semget(FCFS2_CHILD_TICK_ACK_SEM_KEY, 1, IPC_CREAT | 0666);
    if (ctx.child_tick_ack_semid == -1)
    {
        perror("semget child tick ack");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.ctrl_mutex_semid = semget(FCFS2_CTRL_MUTEX_SEM_KEY, 1, IPC_CREAT | 0666);
    if (ctx.ctrl_mutex_semid == -1)
    {
        perror("semget ctrl mutex");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    if (semctl(ctx.ctrl_mutex_semid, 0, SETVAL, 1) == -1)
    {
        perror("semctl ctrl mutex SETVAL");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.tick_done_semid = semget(FCFS2_TICK_DONE_SEM_KEY, 1, IPC_CREAT | 0666);
    if (ctx.tick_done_semid == -1)
    {
        perror("semget tick done");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    if (semctl(ctx.tick_done_semid, 0, SETVAL, 0) == -1)
    {
        perror("semctl tick done SETVAL");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    ctx.start_semid = semget(FCFS2_START_SEM_KEY, 1, IPC_CREAT | 0666);
    if (ctx.start_semid == -1)
    {
        perror("semget start");
        cleanup_master_ipc(&ctx);
        return 1;
    }

    if (!init_sem_set_zero(ctx.consume_semid, 2) ||
        !init_sem_set_zero(ctx.ack_semid, 1) ||
        !init_sem_set_zero(ctx.steal_semid, 2) ||
        !init_sem_set_zero(ctx.child_tick_semid, 2) ||
        !init_sem_set_zero(ctx.child_tick_ack_semid, 1))
    {
        cleanup_master_ipc(&ctx);
        return 1;
    }

    if (!spawn_children(&ctx))
    {
        shutdown_children(&ctx);
        cleanup_master_ipc(&ctx);
        return 1;
    }

    initClk();
    ctx.last_clk = getClk();

    if (!wait_for_children_ready(&ctx) || !sem_up_idx(ctx.start_semid, 0))
    {
        shutdown_children(&ctx);
        cleanup_master_ipc(&ctx);
        destroyClk(true);
        return 1;
    }

    if (!process_tick_boundary(&ctx, ctx.last_clk))
    {
        shutdown_children(&ctx);
        cleanup_master_ipc(&ctx);
        destroyClk(true);
        return 1;
    }

    while (1)
    {
        int all_received;
        int done1;
        int done2;

        now = getClk();
        if (now <= ctx.last_clk)
        {
            if (!read_done_state(&ctx, &all_received, &done1, &done2))
            {
                shutdown_children(&ctx);
                cleanup_master_ipc(&ctx);
                destroyClk(true);
                return 1;
            }

            if (all_received && done1 && done2)
            {
                break;
            }
            usleep(2000);
            continue;
        }

        while (ctx.last_clk < now)
        {
            ctx.last_clk++;
            if (!process_tick_boundary(&ctx, ctx.last_clk))
            {
                shutdown_children(&ctx);
                cleanup_master_ipc(&ctx);
                destroyClk(true);
                return 1;
            }
        }

        {
            int all_received;
            int done1;
            int done2;

            if (!read_done_state(&ctx, &all_received, &done1, &done2))
            {
                shutdown_children(&ctx);
                cleanup_master_ipc(&ctx);
                destroyClk(true);
                return 1;
            }

            if (all_received && done1 && done2)
            {
                break;
            }
        }
    }

    shutdown_children(&ctx);
    waitpid(ctx.child_pids[0], NULL, 0);
    waitpid(ctx.child_pids[1], NULL, 0);

    cleanup_master_ipc(&ctx);
    destroyClk(true);
    return 0;
}