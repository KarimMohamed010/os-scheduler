#include "headers.h"
#include "shared.h"
#include <string.h>

static int msgqid = -1;        /* System-V message queue id          */
static PCB *proc_table = NULL; /* Heap-allocated process array       */

void clearResources(int signum);

int main(int argc, char *argv[])
{
    (void)argc;
    (void)argv; /* generator takes no command-line arguments */
    signal(SIGINT, clearResources);

    /* ============================================================
     * 1. Read processes.txt into a dynamic PCB array
     * ============================================================ */
    FILE *fp = fopen("processes.txt", "r");
    if (!fp)
    {
        perror("Cannot open processes.txt");
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
        /* Format: id  arrival  runtime  priority  (tab-separated) */
        if (sscanf(line, "%d\t%d\t%d\t%d",
                   &p.id, &p.arrival, &p.runtime, &p.priority) != 4)
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
    printf("[Generator] Loaded %d processes from processes.txt\n", count);

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

    /* ============================================================
     * 4a. Fork the clock process
     * ============================================================ */
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
     * 4b. Fork the scheduler process, passing algo params as argv
     *
     *   argv layout for scheduler:
     *     argv[0] = "scheduler"
     *     argv[1] = algo   (always)
     *     argv[2] = quantum / N  (RR: quantum; FCFS_2: N; HPF: 0)
     *     argv[3] = M            (FCFS_2 only; else 0)
     * ============================================================ */
    char s_algo[16], s_q[16], s_n[16], s_m[16];
    snprintf(s_algo, sizeof(s_algo), "%d", algo);
    snprintf(s_q, sizeof(s_q), "%d", quantum);
    snprintf(s_n, sizeof(s_n), "%d", N);
    snprintf(s_m, sizeof(s_m), "%d", M);

    pid_t sched_pid = fork();
    if (sched_pid == -1)
    {
        perror("fork scheduler");
        exit(EXIT_FAILURE);
    }
    if (sched_pid == 0)
    {
        /* Child: exec the scheduler binary */
        execl("./scheduler.out", "scheduler.out", s_algo, s_q, s_n, s_m, NULL);
        perror("execl scheduler.out");
        exit(EXIT_FAILURE);
    }
    printf("[Generator] Scheduler process forked (pid=%d)\n", sched_pid);

    /* ============================================================
     * 5. Connect to the clock (blocks until clock is ready)
     * ============================================================ */
    initClk();
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
                       "runtime=%d  priority=%d\n",
                       clk,
                       proc_table[next_idx].id,
                       proc_table[next_idx].arrival,
                       proc_table[next_idx].runtime,
                       proc_table[next_idx].priority);
            }
            next_idx++;
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

    /* Free heap memory */
    if (proc_table != NULL)
    {
        free(proc_table);
        proc_table = NULL;
    }

    exit(0);
}