#ifndef MMU_H
#define MMU_H

#include "shared.h"
#include <stdio.h>

/* =========================================================
 *  Physical memory constants
 *
 *  10-bit byte-addressable virtual address space:
 *    bits [9:4] = 6-bit virtual page number  (VPN)  → max 64 pages
 *    bits [3:0] = 4-bit page offset                 → 16 bytes/page
 *
 *  Physical RAM = 512 bytes / 16 bytes per page = 32 frames
 * ========================================================= */
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

/* =========================================================
 *  Frame table entry
 *
 *  One slot per physical frame.  Tracks ownership, which
 *  virtual page is resident, and the NRU status bits.
 *
 *  owner_pid == -1  →  frame is free
 *  is_page_table    →  frame holds a process page table;
 *                      it is never a candidate for eviction
 * ========================================================= */
typedef struct
{
    int owner_pid;       /* PID that owns this frame, -1 = free  */
    int vpn;             /* virtual page number loaded here       */
    int is_page_table;   /* 1 = holds a page table (no evict)    */
    int R;               /* referenced bit  (set on any access)  */
    int M;               /* modified  bit   (set on write)       */
} FrameEntry;

/* =========================================================
 *  Page-table entry
 *
 *  Design: each process has an array of MAX_VPAGES entries.
 *  The array is stored in the scheduler's address space (not
 *  inside the simulated 512-byte RAM).  One physical frame is
 *  reserved per process and tracked in FrameEntry so that the
 *  frame allocator counts it as occupied and the NRU eviction
 *  loop skips it.  The is_page_table flag enforces this.
 *
 *  present == 0  →  page is on disk; frame field is invalid
 * ========================================================= */
typedef struct
{
    int frame;     /* physical frame number (valid iff present)   */
    int present;   /* 1 = in RAM                                  */
    int R;         /* mirrored from frame_table for convenience   */
    int M;         /* mirrored from frame_table for convenience   */
} PageTableEntry;

/* =========================================================
 *  Per-process page table
 *  Indexed in the global page_tables[] array by process ID.
 * ========================================================= */
typedef struct
{
    PageTableEntry entries[MAX_VPAGES];
} PageTable;

/* =========================================================
 *  Blocked-process slot
 *
 *  When a page fault occurs, the faulting process is placed
 *  into one of these slots.  The scheduler decrements
 *  ticks_remaining every clock tick; when it reaches 0 the
 *  disk transfer is complete and the process is re-queued.
 *
 *  fault_vpn    – the virtual page being brought in
 *  target_frame – the physical frame reserved for the page
 *  fault_write  – 1 if the faulting access was a write
 * ========================================================= */
#define MAX_BLOCKED PHYS_FRAMES  /* at most one fault per frame */

typedef struct
{
    PCB  proc;
    int  ticks_remaining;
    int  fault_vpn;
    int  target_frame;
    int  fault_write;
    int  in_use;         /* 1 = slot occupied                  */
} BlockedSlot;

/* =========================================================
 *  Global memory state  (defined in mmu.c, extern here)
 * ========================================================= */
extern FrameEntry frame_table[PHYS_FRAMES];
extern PageTable  page_tables[MAX_PROCESSES + 1]; /* process ids are 1..MAX_PROCESSES */

/* =========================================================
 *  MMU API
 * ========================================================= */

/* One-time initialisation: all frames free, all PTEs invalid */
void mmu_init(void);

/* ── Frame allocator ── */

/* Returns a free frame number, or -1 if RAM is full */
int  mmu_alloc_frame(void);

/* Return frame to the free pool and clear its metadata */
void mmu_free_frame(int frame);

/* ── Process lifecycle ── */

/*
 * Called when a process is dispatched for the first time
 * (pcb->started == 0).  Allocates a page-table frame and loads
 * virtual page 0 into a data frame.  Both operations are
 * instantaneous per the project spec.
 *
 * If no free frame exists for either allocation, NRU eviction
 * is performed inline (still no extra time for the init).
 *
 * Returns 0 on success, -1 on unrecoverable error.
 */
int  mmu_process_init(PCB *pcb, int now, FILE *log);

/* Release every frame owned by pid (call on process finish) */
void mmu_process_exit(int pid);

/* ── Address translation ── */

/*
 * Translate virtual address va for process pcb.
 *
 * On hit:  sets R bit (and M if write == 1), mirrors to PTE,
 *          returns physical byte address.
 * On miss: sets *fault_vpn to the missing page number,
 *          returns -1.
 */
int  mmu_translate(PCB *pcb, int va, int write, int *fault_vpn);

/* ── Page fault handler ── */

/*
 * Prepare to bring in virtual page vpn for process pcb.
 *
 * 1. Tries to claim a free frame.
 * 2. If none: invokes NRU eviction, writes back dirty victim
 *    if needed, invalidates the victim's PTE.
 * 3. Reserves the selected frame for the incoming page
 *    (owner_pid and vpn are set but present is NOT set yet –
 *     that happens in mmu_complete_fault after disk completes).
 * 4. Sets *disk_ticks_out = DISK_ACCESS_TICKS (clean)
 *                        or DISK_DIRTY_TICKS   (dirty write-back).
 *
 * Returns the reserved frame number, or -1 on error.
 * Logging (free-frame or swap-out lines) is written to log.
 */
int  mmu_handle_fault(PCB *pcb, int vpn, int write,
                       int now, FILE *log, int *disk_ticks_out);

/*
 * Called by the scheduler after disk_ticks_out ticks have
 * elapsed.  Commits the PTE, sets R (and M if write), and
 * writes the "loaded" line to memory.log.
 */
void mmu_complete_fault(PCB *pcb, int fault_vpn, int target_frame,
                         int fault_write, int now, FILE *log);

/* ── NRU R-bit periodic reset ── */

/*
 * Clear R bit on every resident data frame and mirror the
 * change to the corresponding PTE.  Called every K RR quantums.
 */
void mmu_clear_r_bits(void);

/* ── memory.log helpers ── */

/* "PageFault upon VA <binary> from process <pid>" */
void mmu_log_page_fault(FILE *log, const char *va_str, int pid);

/* "Free Physical page <frame> allocated" */
void mmu_log_free_frame(FILE *log, int frame);

/* "Swapping out page <frame> to disk" */
void mmu_log_swap_out(FILE *log, int frame);

/* "At time <t> disk address <d> for process <p> is loaded into memory page <f>" */
void mmu_log_loaded(FILE *log, int now, int disk_addr, int pid, int frame);

#endif /* MMU_H */
