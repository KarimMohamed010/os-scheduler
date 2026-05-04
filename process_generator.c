#include "headers.h"
#include "shared.h"
#include <errno.h>
#include <string.h>

static int msgqid = -1;          /* System-V message queue id          */
static int tick_semid = -1;      /* System-V semaphore id for tick sync */
static int tick_done_semid = -1; /* FCFS-2 generator/master tick ack */
static int start_semid = -1;     /* FCFS-2 generator/master startup sync */
static PCB *proc_table = NULL;   /* Heap-allocated process array       */

static int semaphore_up(int semid)
{
    struct sembuf op;
    op.sem_num = 0;
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

static int semaphore_down(int semid)
{
    struct sembuf op;
    op.sem_num = 0;
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

void clearResources(int signum);

static void cleanup_stale_clock_shm(void)
{
    int stale_shmid;

    stale_shmid = shmget(SHKEY, 4, 0666);
    if (stale_shmid != -1)
    {
        shmctl(stale_shmid, IPC_RMID, NULL);
    }
}

int main(int argc, char *argv[])
{
    const char *infile = (argc >= 2) ? argv[1] : "processes.txt";
    signal(SIGINT, clearResources);

    /* ============================================================
     * 1. Read processes.txt into a dynamic PCB array
     * ============================================================ */
    FILE *fp = fopen(infile, "r");
    if (!fp)
    {
        fprintf(stderr, "Cannot open %s\n", infile);
        exit(EXIT_FAILURE);
    }

    int capacity = 32;
    int count = 0;
    proc_table = (PCB *)malloc(capacity * sizeof(PCB));
    if (!proc_table)
    {
        perror("malloc proc_table");
        exit(EXIT_FAILURE);
    }

    char line[256];
    while (fgets(line, sizeof(line), fp))
    {
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r')
            continue;

        /* Grow array if needed */
        if (count == capacity)
        {
            capacity *= 2;
            proc_table = (PCB *)realloc(proc_table, capacity * sizeof(PCB));
            if (!proc_table)
            {
                perror("realloc proc_table");
                exit(EXIT_FAILURE);
            }
        }

        PCB p;
        memset(&p, 0, sizeof(PCB));
        /* Format: id  arrival  runtime  priority  base  limit */
        if (sscanf(line, "%d\t%d\t%d\t%d\t%d\t%d",
                   &p.id, &p.arrival, &p.runtime, &p.priority, &p.base, &p.limit) != 6)
        {
            fprintf(stderr, "Warning: skipping malformed line: %s", line);
            continue;
        }
        p.remaining = p.runtime; /* initialise remaining = runtime   */
        p.started = 0;
        p.finished = 0;
        proc_table[count++] = p;
    }
    fclose(fp);
    printf("[Generator] Loaded %d processes from %s\n", count, infile);

    /* ============================================================
     * 2. Ask user for scheduling algorithm and its parameters
     * ============================================================ */
    int algo = 0;
    int quantum = 0; /* used by RR                */
    int N = 0;       /* check interval (2-CPU)    */
    int M = 0;       /* steal threshold (2-CPU)   */

    printf("\nChoose a scheduling algorithm:\n");
    printf("  %d - Preemptive Highest Priority First (HPF)\n", ALGO_HPF);
    printf("  %d - Round Robin (RR)\n", ALGO_RR);
    printf("  %d - 2-CPU FCFS with Work Stealing\n", ALGO_FCFS_2);
    printf("Enter choice: ");
    scanf("%d", &algo);

    if (algo == ALGO_RR)
    {
        printf("Enter time quantum Q: ");
        scanf("%d", &quantum);
        if (quantum < 1)
        {
            fprintf(stderr, "Quantum must be >= 1\n");
            exit(1);
        }
    }
    else if (algo == ALGO_FCFS_2)
    {
        printf("Enter check interval N (clock cycles between stealing checks): ");
        scanf("%d", &N);
        printf("Enter stealing threshold M (max allowed remaining-time difference): ");
        scanf("%d", &M);
        if (N < 1 || M < 1)
        {
            fprintf(stderr, "N and M must be >= 1\n");
            exit(1);
        }
    }
    else if (algo != ALGO_HPF)
    {
        fprintf(stderr, "Invalid algorithm choice.\n");
        exit(EXIT_FAILURE);
    }

    /* ============================================================
     * 3. Create the message queue BEFORE forking the scheduler
     *    so the scheduler can open it immediately on startup.
     * ============================================================ */
    msgqid = msgget(MSG_KEY, IPC_CREAT | 0644);
    if (msgqid == -1)
    {
        perror("msgget: failed to create message queue");
        exit(EXIT_FAILURE);
    }
    printf("[Generator] Message queue created (id=%d, key=%d)\n", msgqid, MSG_KEY);

    tick_semid = semget(TICK_SYNC_SEM_KEY, 1, IPC_CREAT | 0644);
    if (tick_semid == -1)
    {
        perror("semget: failed to create tick sync semaphore");
        exit(EXIT_FAILURE);
    }
    if (semctl(tick_semid, 0, SETVAL, 0) == -1)
    {
        perror("semctl SETVAL");
        exit(EXIT_FAILURE);
    }
    printf("[Generator] Tick sync semaphore created (id=%d, key=%d)\n", tick_semid, TICK_SYNC_SEM_KEY);

    if (algo == ALGO_FCFS_2)
    {
        tick_done_semid = semget(FCFS2_TICK_DONE_SEM_KEY, 1, IPC_CREAT | 0644);
        if (tick_done_semid == -1)
        {
            perror("semget: failed to create FCFS-2 tick-done semaphore");
            exit(EXIT_FAILURE);
        }
        if (semctl(tick_done_semid, 0, SETVAL, 0) == -1)
        {
            perror("semctl SETVAL tick_done");
            exit(EXIT_FAILURE);
        }

        start_semid = semget(FCFS2_START_SEM_KEY, 1, IPC_CREAT | 0644);
        if (start_semid == -1)
        {
            perror("semget: failed to create FCFS-2 startup semaphore");
            exit(EXIT_FAILURE);
        }
        if (semctl(start_semid, 0, SETVAL, 0) == -1)
        {
            perror("semctl SETVAL start");
            exit(EXIT_FAILURE);
        }
    }

    /* ============================================================
     * 4a. Fork the clock process
     * ============================================================ */
    cleanup_stale_clock_shm();

    pid_t clk_pid = fork();
    if (clk_pid == -1)
    {
        perror("fork clk");
        exit(EXIT_FAILURE);
    }
    if (clk_pid == 0)
    {
        /* Child: exec the clock binary */
        execl("./clk.out", "clk.out", NULL);
        perror("execl clk.out");
        exit(EXIT_FAILURE);
    }
    printf("[Generator] Clock process forked (pid=%d)\n", clk_pid);

    /* ============================================================
     * 4b. Fork scheduler stack, passing algo params as argv
     *
     *   argv layout for scheduler.out (HPF/RR):
     *     argv[0] = "scheduler"
     *     argv[1] = algo   (always)
     *     argv[2] = quantum (RR only; HPF: 0)
     *
     *   argv layout for master_scheduler.out (FCFS_2):
     *     argv[0] = "master_scheduler"
     *     argv[1] = algo   (= ALGO_FCFS_2)
     *     argv[2] = N
     *     argv[3] = M
     * ============================================================ */
    char s_algo[16], s_q[16], s_n[16], s_m[16];
    snprintf(s_algo, sizeof(s_algo), "%d", algo);
    snprintf(s_q, sizeof(s_q), "%d", quantum);
    snprintf(s_n, sizeof(s_n), "%d", N);
    snprintf(s_m, sizeof(s_m), "%d", M);

    pid_t sched_pid = fork();
    if (sched_pid == -1)
    {
        perror("fork scheduler/master");
        exit(EXIT_FAILURE);
    }
    if (sched_pid == 0)
    {
        if (algo == ALGO_FCFS_2)
        {
            execl("./master_scheduler.out", "master_scheduler.out", s_algo, s_n, s_m, NULL);
            perror("execl master_scheduler.out");
        }
        else
        {
            execl("./scheduler.out", "scheduler.out", s_algo, s_q, s_n, s_m, NULL);
            perror("execl scheduler.out");
        }
        exit(EXIT_FAILURE);
    }
    if (algo == ALGO_FCFS_2)
    {
        printf("[Generator] Master scheduler process forked (pid=%d)\n", sched_pid);
    }
    else
    {
        printf("[Generator] Scheduler process forked (pid=%d)\n", sched_pid);
    }

    /* ============================================================
     * 5. Connect to the clock (blocks until clock is ready)
     * ============================================================ */
    initClk();

    if (algo == ALGO_FCFS_2 && !semaphore_down(start_semid))
    {
        clearResources(0);
    }

    printf("[Generator] Clock initialised. Starting generation loop.\n");

    /* ============================================================
     * 6. Generation main loop
     *
     *    Strategy: sleep until the clock tick changes, then send
     *    every process whose arrival time == current tick.
     *    Processes in proc_table are sorted by arrival (per spec),
     *    so we keep a cursor `next_idx` into the array.
     * ============================================================ */
    int next_idx = 0;  /* index of the next unsent process */
    int prev_clk = -1; /* last observed clock value        */

    while (next_idx < count)
    {
        int clk = getClk();

        /* Poll until the clock advances */
        if (clk == prev_clk)
        {
            usleep(50000); /* sleep 50 ms to avoid busy-spinning */
            continue;
        }
        prev_clk = clk;

        /*
         * Send all processes whose arrival time is <= current clock.
         * (Handles simultaneous arrivals correctly.)
         */
        while (next_idx < count && proc_table[next_idx].arrival <= clk)
        {
            Message msg;
            msg.mtype = 1; /* mtype 1 = new process */
            msg.proc = proc_table[next_idx];

            if (msgsnd(msgqid, &msg, sizeof(PCB), !IPC_NOWAIT) == -1)
            {
                perror("msgsnd: failed to send process");
            }
            else
            {
                printf("[Generator] t=%d  Sent process id=%d  arrival=%d  "
                       "runtime=%d  priority=%d  base=%d  limit=%d\n",
                       clk,
                       proc_table[next_idx].id,
                       proc_table[next_idx].arrival,
                       proc_table[next_idx].runtime,
                       proc_table[next_idx].priority,
                       proc_table[next_idx].base,
                       proc_table[next_idx].limit);
            }
            next_idx++;
        }

        /* Signal scheduler that this tick's generation phase is complete. */
        if (!semaphore_up(tick_semid))
        {
            clearResources(0);
        }

        if (algo == ALGO_FCFS_2 && !semaphore_down(tick_done_semid))
        {
            clearResources(0);
        }
    }

    /* ============================================================
     * 7. Send the end-of-input sentinel (mtype = 2)
     *    Scheduler uses this to know no more processes are coming.
     * ============================================================ */
    Message sentinel;
    memset(&sentinel, 0, sizeof(Message));
    sentinel.mtype = 2;
    if (msgsnd(msgqid, &sentinel, sizeof(PCB), 0) == -1)
    {
        perror("msgsnd: failed to send sentinel");
    }
    else
    {
        printf("[Generator] Sentinel sent — no more processes.\n");
    }

    /* Wake scheduler one last time so it can consume the sentinel promptly. */
    if (!semaphore_up(tick_semid))
    {
        clearResources(0);
    }

    if (algo == ALGO_FCFS_2 && !semaphore_down(tick_done_semid))
    {
        clearResources(0);
    }

    /* ============================================================
     * 8. Wait for the scheduler to finish.
     *    The scheduler calls destroyClk(true) on exit, which sends
     *    SIGINT to the whole process group — this generator will be
     *    killed too and clearResources() will fire.
     *    If for any reason that doesn't happen, we wait manually.
     * ============================================================ */
    waitpid(sched_pid, NULL, 0);

    /* Should not normally reach here (group kill fires first) */
    free(proc_table);
    destroyClk(true);
    return 0;
}

/* ================================================================== */
/*  Signal handler — cleans up IPC resources on interruption          */
/* ================================================================== */
void clearResources(int signum)
{
    (void)signum; /* suppress unused-parameter warning */
    printf("\n[Generator] Caught signal — cleaning up IPC resources.\n");

    /* Remove the message queue */
    if (msgqid != -1)
    {
        if (msgctl(msgqid, IPC_RMID, NULL) == -1)
            perror("msgctl IPC_RMID");
        else
            printf("[Generator] Message queue (id=%d) removed.\n", msgqid);
        msgqid = -1;
    }

    if (tick_semid != -1)
    {
        if (semctl(tick_semid, 0, IPC_RMID) == -1)
            perror("semctl IPC_RMID");
        else
            printf("[Generator] Tick sync semaphore (id=%d) removed.\n", tick_semid);
        tick_semid = -1;
    }

    if (tick_done_semid != -1)
    {
        if (semctl(tick_done_semid, 0, IPC_RMID) == -1)
            perror("semctl IPC_RMID");
        tick_done_semid = -1;
    }

    if (start_semid != -1)
    {
        if (semctl(start_semid, 0, IPC_RMID) == -1)
            perror("semctl IPC_RMID");
        start_semid = -1;
    }

    /* Free heap memory */
    if (proc_table != NULL)
    {
        free(proc_table);
        proc_table = NULL;
    }

    exit(0);
}