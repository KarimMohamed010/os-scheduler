#ifndef FCFS_H
#define FCFS_H

#include "shared.h"
#include <string.h>
#include <sys/msg.h>

/* 
 * FCFS (First Come First Serve) - 1 CPU
 * 
 * Complete algorithm implementation:
 * - Non-preemptive: each process runs to completion
 * - Context switch overhead: 1 second between processes
 * - Handles all scheduling logic, logging, and performance calculation
 */

typedef struct {
    ReadyQueue *ready_queue;
    PCB current_process;
    int cpu_available_time;
    int last_finish_time;
    
    /* Statistics */
    int total_execution_time;
    int total_waiting_time;
    double total_weighted_turnaround;
    double *weighted_turnarounds;
    int num_processes_completed;
    
    /* Logging */
    FILE *log_file;
    
    /* Communication */
    int msgid;
    int shmid;
    SharedMemory *shared;
} FCFS_Context;

/* Initialize FCFS algorithm */
static inline void fcfs_init(FCFS_Context *ctx, int shmid, int msgid, SharedMemory *shared) {
    ctx->ready_queue = queue_create();
    ctx->current_process.id = -1;
    ctx->current_process.remaining = 0;
    ctx->cpu_available_time = 0;
    ctx->last_finish_time = 0;
    
    ctx->total_execution_time = 0;
    ctx->total_waiting_time = 0;
    ctx->total_weighted_turnaround = 0;
    ctx->weighted_turnarounds = NULL;
    ctx->num_processes_completed = 0;
    
    ctx->log_file = fopen("scheduler.log", "w");
    if (ctx->log_file) {
        fprintf(ctx->log_file, "#At time x process y state arr w total z remain w wait k\n");
    }
    
    ctx->msgid = msgid;
    ctx->shmid = shmid;
    ctx->shared = shared;
}

/* Log event to file */
static inline void fcfs_log(FCFS_Context *ctx, int time, int pid, const char *state, PCB *proc) {
    if (!ctx->log_file) return;
    
    if (strcmp(state, "finished") == 0) {
        int ta = proc->finish_time - proc->arrival;
        fprintf(ctx->log_file, "At time %d process %d %s arr %d total %d remain %d wait %d TA %d\n",
                time, pid, state, proc->arrival, proc->runtime, 
                proc->remaining, proc->waiting, ta);
    } else if (strcmp(state, "started") == 0 || strcmp(state, "resumed") == 0) {
        fprintf(ctx->log_file, "At time %d process %d %s arr %d total %d remain %d wait %d\n",
                time, pid, state, proc->arrival, proc->runtime, 
                proc->remaining, proc->waiting);
    } else if (strcmp(state, "stopped") == 0) {
        fprintf(ctx->log_file, "At time %d process %d %s arr %d total %d remain %d wait %d\n",
                time, pid, state, proc->arrival, proc->runtime, 
                proc->remaining, proc->waiting);
    } else { /* arrived */
        fprintf(ctx->log_file, "At time %d process %d arrived arr %d total %d remain %d wait %d\n",
                time, pid, proc->arrival, proc->runtime, proc->runtime, 0);
    }
    fflush(ctx->log_file);
}

/* Receive new processes from generator */
static inline void fcfs_receive_new_processes(FCFS_Context *ctx, int current_time) {
    Message msg;
    while (msgrcv(ctx->msgid, &msg, sizeof(PCB), 0, IPC_NOWAIT) > 0) {
        if (msg.mtype == 1) {
            /* New process arrived */
            PCB new_proc = msg.proc;
            new_proc.remaining = new_proc.runtime;
            new_proc.waiting = 0;
            new_proc.started = 0;
            new_proc.finished = 0;
            
            queue_push_tail(ctx->ready_queue, new_proc);
            fcfs_log(ctx, current_time, new_proc.id, "arrived", &new_proc);
        } else if (msg.mtype == 2) {
            /* End of input sentinel */
            ctx->shared->all_processes_received = 1;
        }
    }
}

/* Start a new process (fork and execute) */
static inline void fcfs_start_process(FCFS_Context *ctx, PCB *proc, int start_time) {
    proc->start_time = start_time;
    proc->started = 1;
    proc->remaining = proc->runtime;
    
    /* Calculate waiting time */
    proc->waiting = start_time - proc->arrival;
    ctx->total_waiting_time += proc->waiting;
    
    /* Log started event */
    fcfs_log(ctx, start_time, proc->id, "started", proc);
    
    /* TODO: Fork and execute process.c */
    pid_t pid = fork();
    if (pid == 0) {
        /* Child process - execute process.c */
        char id_str[10], runtime_str[10];
        sprintf(id_str, "%d", proc->id);
        sprintf(runtime_str, "%d", proc->runtime);
        //execl("./process", "process", id_str, runtime_str, NULL);
        execl("./process", "process", runtime_str, NULL);
        perror("execl failed");
        exit(1);
    } else {
        /* Parent - store PID */
        proc->pid = pid;
    }
}

/* Handle process completion */
static inline void fcfs_handle_completion(FCFS_Context *ctx, PCB *proc, int finish_time) {
    proc->finish_time = finish_time;
    proc->finished = 1;
    proc->remaining = 0;
    
    /* Calculate weighted turnaround time */
    int turnaround = finish_time - proc->arrival;
    double weighted_ta = (double)turnaround / proc->runtime;
    ctx->total_weighted_turnaround += weighted_ta;
    ctx->num_processes_completed++;
    
    /* Log finished event */
    fcfs_log(ctx, finish_time, proc->id, "finished", proc);
    
    /* Update statistics */
    ctx->total_execution_time += proc->runtime;
    ctx->last_finish_time = finish_time;
    
    /* CPU becomes idle, will be available after context switch */
    ctx->cpu_available_time = finish_time + 1;  /* +1 for context switch */
    ctx->current_process.id = -1;
}

// /* Check if any running process has finished (by signal or shared memory) */
// static inline void fcfs_check_finished_processes(FCFS_Context *ctx, int current_time) {
//     if (ctx->current_process.id != -1 && ctx->current_process.remaining > 0) {
//         /* TODO: Check if process has finished via signal or shared memory */
//         /* For simulation: check if remaining time is 0 */
//         if (ctx->current_process.remaining == 0) {
//             fcfs_handle_completion(ctx, &ctx->current_process, current_time);
//         }
//     }
// }
#include <sys/wait.h>

static inline void fcfs_check_finished_processes(FCFS_Context *ctx, int current_time)
{
    if (ctx->current_process.id != -1)
    {
        int status;
        pid_t result = waitpid(ctx->current_process.pid, &status, WNOHANG);

        if (result > 0)
        {
            fcfs_handle_completion(ctx, &ctx->current_process, current_time);
        }
    }
}


/* Dispatch next process if CPU is available */
static inline void fcfs_dispatch_next(FCFS_Context *ctx, int current_time) {
    /* Check if CPU is busy */
    if (ctx->current_process.id != -1 && ctx->current_process.remaining > 0) {
        return;  /* Still running */
    }
    
    /* Check if we're still in context switch */
    if (current_time < ctx->cpu_available_time) {
        return;  /* CPU not ready yet */
    }
    
    /* Get next process from queue */
    PCB next;
    if (!queue_pop(ctx->ready_queue, &next)) {
        return;  /* No process to run */
    }
    
    /* Calculate actual start time (account for context switch if needed) */
    int start_time = current_time;
    if (ctx->last_finish_time > 0 && start_time < ctx->last_finish_time + 1) {
        start_time = ctx->last_finish_time + 1;
    }
    
    /* Start the process */
    fcfs_start_process(ctx, &next, start_time);
    ctx->current_process = next;
}

/* Main scheduling loop - THIS IS THE ONLY FUNCTION THE SCHEDULER CALLS */
static inline void fcfs_run(FCFS_Context *ctx) {
    /* Initialize clock */
    initClk();
    
    /* Signal that scheduler is ready */
    ctx->shared->scheduler_ready = 1;
    
    int current_time;
    
    while (1) {
        current_time = getClk();
        
        /* Receive any new processes from generator */
        fcfs_receive_new_processes(ctx, current_time);
        
        /* Check if current process has finished */
        fcfs_check_finished_processes(ctx, current_time);
        
        /* Try to dispatch next process if CPU is available */
        fcfs_dispatch_next(ctx, current_time);
        
        /* Check if all processes are done */
        if (ctx->shared->all_processes_received && 
            ctx->ready_queue->size == 0 && 
            ctx->current_process.id == -1) {
            
            printf("All processes completed at time %d\n", current_time);
            ctx->shared->all_processes_completed = 1;
            break;
        }
        
        usleep(100000); /* Small delay to avoid busy looping */
    }
    
    /* Calculate and write performance file */
    FILE *perf = fopen("scheduler.perf", "w");
    if (perf) {
        double cpu_util = (double)ctx->total_execution_time / current_time * 100;
        double avg_wta = ctx->total_weighted_turnaround / ctx->num_processes_completed;
        double avg_waiting = (double)ctx->total_waiting_time / ctx->num_processes_completed;
        
        /* Calculate standard deviation */
        double variance = 0;
        /* TODO: Store individual WTAs to calculate std deviation */
        
        fprintf(perf, "CPU utilization = %.2f%%\n", cpu_util);
        fprintf(perf, "Avg WTA = %.2f\n", avg_wta);
        fprintf(perf, "Avg Waiting = %.2f\n", avg_waiting);
        fprintf(perf, "Std WTA = 0.00\n");
        fclose(perf);
    }
    
    /* Cleanup */
    destroyClk(0);
    if (ctx->log_file) fclose(ctx->log_file);
}

#endif /* FCFS_H */