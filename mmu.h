#ifndef MMU_H
#define MMU_H

#include "shared.h"
#include <stdio.h>

#define RAM_SIZE     512
#define PAGE_SIZE    16
#define PHYS_FRAMES  32        /* RAM_SIZE / PAGE_SIZE          */
#define ADDR_BITS    10
#define OFFSET_BITS  4
#define VPN_BITS     6
#define MAX_VPAGES   64        /* 2^VPN_BITS                    */
#define OFFSET_MASK  (PAGE_SIZE - 1)
#define VPN_MASK     ((1 << VPN_BITS) - 1)

/* Disk timing (ticks) */
#define DISK_ACCESS_TICKS  10  /* one disk transfer             */
#define DISK_DIRTY_TICKS   20  /* write-back + load             */
#define FAULT_CHECK_TICKS   1  /* in-memory fault detection     */

typedef struct
{
    int owner_pid;
    int vpn;
    int is_page_table;
    int is_reserved;
    int R;
    int M;
} FrameEntry;

typedef struct
{
    int frame;
    int present;
} PageTableEntry;

typedef struct
{
    PageTableEntry entries[MAX_VPAGES];
} PageTable;

#define MAX_BLOCKED PHYS_FRAMES  /* at most one fault per frame */

typedef struct
{
    PCB  proc;
    int  ticks_remaining;
    int  fault_vpn;
    int  target_frame;
    int  fault_write;
    int  in_use;
} BlockedSlot;

extern FrameEntry frame_table[PHYS_FRAMES];
extern PageTable  page_tables[MAX_PROCESSES + 1];

void mmu_init(void);

int  mmu_alloc_frame(void);

void mmu_free_frame(int frame);

int  mmu_process_init(PCB *pcb, int now, FILE *log);

void mmu_process_exit(int pid);

int  mmu_translate(PCB *pcb, int va, int write, int *fault_vpn);

int  mmu_handle_fault(PCB *pcb, int vpn, int write,
                       int now, FILE *log, int *disk_ticks_out);

void mmu_complete_fault(PCB *pcb, int fault_vpn, int target_frame,
                         int fault_write, int now, FILE *log);

void mmu_clear_r_bits(void);

void mmu_log_page_fault(FILE *log, const char *va_str, int pid);

void mmu_log_free_frame(FILE *log, int frame);

void mmu_log_swap_out(FILE *log, int frame);

void mmu_log_loaded(FILE *log, int now, int disk_addr, int pid, int frame);

#endif /* MMU_H */
