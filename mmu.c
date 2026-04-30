/* =========================================================
 *  mmu.c — Memory Management Unit implementation (Phase 2)
 *
 *  Provides physical frame allocation, per-process page tables,
 *  demand paging with NRU replacement, and memory.log output.
 * ========================================================= */

#include "mmu.h"
#include <string.h>

/* ── Global physical frame table ── */
FrameEntry frame_table[PHYS_FRAMES];

/* ── Per-process page tables (indexed by process id) ── */
PageTable page_tables[MAX_PROCESSES];

/* =========================================================
 *  Internal helper: NRU eviction
 *
 *  Scans frame_table[0..31] in ascending order, skips page-
 *  table frames.  Classifies each occupied data frame into
 *  NRU class 0–3 and returns the first frame found in the
 *  lowest non-empty class.
 *
 *  If the victim is modified, writes the swap-out log line
 *  and sets *dirty = 1 so the caller can add the write-back
 *  cost.
 *
 *  On return the victim frame is freed (metadata cleared,
 *  owner's PTE invalidated).  Returns frame number, or -1
 *  if no evictable frame exists (should never happen if
 *  page-table frames < 32).
 * ========================================================= */
static int nru_evict(int now, FILE *log, int *dirty)
{
    /*
     * NRU classes:
     *   0 → R=0, M=0   (best victim)
     *   1 → R=0, M=1
     *   2 → R=1, M=0
     *   3 → R=1, M=1   (worst victim)
     */
    int best_frame = -1;
    int best_class = 4;   /* higher than any real class */
    int f;

    for (f = 0; f < PHYS_FRAMES; f++)
    {
        int cls;

        /* Skip free frames and page-table frames */
        if (frame_table[f].owner_pid == -1)
            continue;
        if (frame_table[f].is_page_table)
            continue;

        cls = (frame_table[f].R << 1) | frame_table[f].M;

        if (cls < best_class)
        {
            best_class = cls;
            best_frame = f;

            if (best_class == 0)
                break;          /* can't do better than class 0 */
        }
    }

    if (best_frame == -1)
        return -1;              /* should not happen */

    /* ── Determine if write-back is needed ── */
    *dirty = frame_table[best_frame].M;

    /* Log the swap-out only if the page is dirty (needs write-back) */
    if (*dirty)
    {
        mmu_log_swap_out(log, best_frame);
    }

    /* ── Invalidate the victim's PTE ── */
    {
        int victim_pid = frame_table[best_frame].owner_pid;
        int victim_vpn = frame_table[best_frame].vpn;

        if (victim_pid >= 0 && victim_pid < MAX_PROCESSES)
        {
            page_tables[victim_pid].entries[victim_vpn].present = 0;
            page_tables[victim_pid].entries[victim_vpn].R = 0;
            page_tables[victim_pid].entries[victim_vpn].M = 0;
            page_tables[victim_pid].entries[victim_vpn].frame = -1;
        }
    }

    /* ── Free the frame ── */
    mmu_free_frame(best_frame);

    return best_frame;
}

/* =========================================================
 *  mmu_init — call once at scheduler startup
 * ========================================================= */
void mmu_init(void)
{
    int f, p, v;

    for (f = 0; f < PHYS_FRAMES; f++)
    {
        frame_table[f].owner_pid    = -1;
        frame_table[f].vpn          = -1;
        frame_table[f].is_page_table = 0;
        frame_table[f].R            = 0;
        frame_table[f].M            = 0;
    }

    for (p = 0; p < MAX_PROCESSES; p++)
    {
        for (v = 0; v < MAX_VPAGES; v++)
        {
            page_tables[p].entries[v].frame   = -1;
            page_tables[p].entries[v].present = 0;
            page_tables[p].entries[v].R       = 0;
            page_tables[p].entries[v].M       = 0;
        }
    }
}

/* =========================================================
 *  Frame allocator
 * ========================================================= */
int mmu_alloc_frame(void)
{
    int f;
    for (f = 0; f < PHYS_FRAMES; f++)
    {
        if (frame_table[f].owner_pid == -1)
            return f;
    }
    return -1;  /* RAM full */
}

void mmu_free_frame(int frame)
{
    if (frame < 0 || frame >= PHYS_FRAMES)
        return;

    frame_table[frame].owner_pid    = -1;
    frame_table[frame].vpn          = -1;
    frame_table[frame].is_page_table = 0;
    frame_table[frame].R            = 0;
    frame_table[frame].M            = 0;
}

/* =========================================================
 *  mmu_process_init — first dispatch of a process
 *
 *  1. Allocate a frame for the page table (never evicted).
 *  2. Allocate a frame for virtual page 0.
 *  Both are "free" in time per the spec.
 * ========================================================= */
int mmu_process_init(PCB *pcb, int now, FILE *log)
{
    int pt_frame, data_frame;
    int dirty;

    /* ── Allocate page-table frame ── */
    pt_frame = mmu_alloc_frame();
    if (pt_frame == -1)
    {
        /* NRU eviction needed even at startup */
        pt_frame = nru_evict(now, log, &dirty);
        if (pt_frame == -1)
            return -1;
    }
    /* No log output for PT frame — startup is instantaneous per spec */

    frame_table[pt_frame].owner_pid    = pcb->id;
    frame_table[pt_frame].vpn          = -1;   /* not a data page */
    frame_table[pt_frame].is_page_table = 1;
    frame_table[pt_frame].R            = 0;
    frame_table[pt_frame].M            = 0;
    pcb->page_table_frame = pt_frame;

    /* ── Allocate frame for virtual page 0 ── */
    data_frame = mmu_alloc_frame();
    if (data_frame == -1)
    {
        data_frame = nru_evict(now, log, &dirty);
        if (data_frame == -1)
            return -1;
    }
    /* No log output for initial page-0 — startup is instantaneous per spec */

    frame_table[data_frame].owner_pid    = pcb->id;
    frame_table[data_frame].vpn          = 0;
    frame_table[data_frame].is_page_table = 0;
    frame_table[data_frame].R            = 0;
    frame_table[data_frame].M            = 0;

    /* Update PTE for page 0 — no log; startup is instantaneous per spec */
    page_tables[pcb->id].entries[0].frame   = data_frame;
    page_tables[pcb->id].entries[0].present = 1;
    page_tables[pcb->id].entries[0].R       = 0;
    page_tables[pcb->id].entries[0].M       = 0;

    return 0;
}

/* =========================================================
 *  mmu_process_exit — free all frames owned by pid
 * ========================================================= */
void mmu_process_exit(int pid)
{
    int f, v;

    for (f = 0; f < PHYS_FRAMES; f++)
    {
        if (frame_table[f].owner_pid == pid)
        {
            mmu_free_frame(f);
        }
    }

    /* Clear all PTEs for this process */
    if (pid >= 0 && pid < MAX_PROCESSES)
    {
        for (v = 0; v < MAX_VPAGES; v++)
        {
            page_tables[pid].entries[v].frame   = -1;
            page_tables[pid].entries[v].present = 0;
            page_tables[pid].entries[v].R       = 0;
            page_tables[pid].entries[v].M       = 0;
        }
    }
}

/* =========================================================
 *  mmu_translate — virtual address → physical address
 *
 *  Returns physical byte address on hit, -1 on miss.
 *  On miss, *fault_vpn is set to the missing VPN.
 * ========================================================= */
int mmu_translate(PCB *pcb, int va, int write, int *fault_vpn)
{
    int vpn    = (va >> OFFSET_BITS) & VPN_MASK;
    int offset = va & OFFSET_MASK;
    PageTableEntry *pte;
    int frame;

    pte = &page_tables[pcb->id].entries[vpn];

    if (!pte->present)
    {
        *fault_vpn = vpn;
        return -1;
    }

    frame = pte->frame;

    /* Set Referenced bit */
    pte->R = 1;
    frame_table[frame].R = 1;

    /* Set Modified bit on write */
    if (write)
    {
        pte->M = 1;
        frame_table[frame].M = 1;
    }

    return (frame << OFFSET_BITS) | offset;
}

/* =========================================================
 *  mmu_handle_fault — prepare to bring in a faulting page
 *
 *  Finds a free frame or evicts via NRU.  Reserves the frame
 *  for the incoming page but does NOT set present yet (that
 *  waits for mmu_complete_fault after disk I/O).
 *
 *  Returns the reserved frame, sets *disk_ticks_out.
 * ========================================================= */
int mmu_handle_fault(PCB *pcb, int vpn, int write,
                      int now, FILE *log, int *disk_ticks_out)
{
    int frame;
    int dirty = 0;

    /* Try a free frame first */
    frame = mmu_alloc_frame();
    if (frame != -1)
    {
        mmu_log_free_frame(log, frame);
        *disk_ticks_out = DISK_ACCESS_TICKS;  /* 10 ticks: load only */
    }
    else
    {
        /* Must evict via NRU */
        frame = nru_evict(now, log, &dirty);
        if (frame == -1)
            return -1;

        if (dirty)
            *disk_ticks_out = DISK_DIRTY_TICKS;   /* 20 ticks: write-back + load */
        else
            *disk_ticks_out = DISK_ACCESS_TICKS;   /* 10 ticks: load only */
    }

    /* Reserve the frame for the incoming page (not present yet) */
    frame_table[frame].owner_pid    = pcb->id;
    frame_table[frame].vpn          = vpn;
    frame_table[frame].is_page_table = 0;
    frame_table[frame].R            = 0;
    frame_table[frame].M            = 0;

    return frame;
}

/* =========================================================
 *  mmu_complete_fault — called after disk I/O completes
 *
 *  Commits the PTE, sets R (and M if write), logs the
 *  "loaded" line.
 * ========================================================= */
void mmu_complete_fault(PCB *pcb, int fault_vpn, int target_frame,
                         int fault_write, int now, FILE *log)
{
    PageTableEntry *pte = &page_tables[pcb->id].entries[fault_vpn];

    pte->frame   = target_frame;
    pte->present = 1;
    pte->R       = 1;
    pte->M       = fault_write ? 1 : 0;

    frame_table[target_frame].R = 1;
    frame_table[target_frame].M = fault_write ? 1 : 0;

    /* disk_addr = base + vpn */
    mmu_log_loaded(log, now, pcb->base + fault_vpn, pcb->id, target_frame);
}

/* =========================================================
 *  mmu_clear_r_bits — periodic NRU R-bit reset
 *
 *  Called every K quantums.  Clears R on all resident data
 *  frames and mirrors to the corresponding PTE.
 * ========================================================= */
void mmu_clear_r_bits(void)
{
    int f;

    for (f = 0; f < PHYS_FRAMES; f++)
    {
        if (frame_table[f].owner_pid == -1)
            continue;
        if (frame_table[f].is_page_table)
            continue;

        frame_table[f].R = 0;

        /* Mirror to PTE */
        {
            int pid = frame_table[f].owner_pid;
            int vpn = frame_table[f].vpn;

            if (pid >= 0 && pid < MAX_PROCESSES &&
                vpn >= 0 && vpn < MAX_VPAGES)
            {
                page_tables[pid].entries[vpn].R = 0;
            }
        }
    }
}

/* =========================================================
 *  memory.log helpers
 *
 *  Exact format required by the auto-grader.
 * ========================================================= */

/* Helper: write an integer in binary with no leading zeros */
static void fprint_binary(FILE *f, int val)
{
    char buf[ADDR_BITS + 1];
    int i, started;

    if (val == 0)
    {
        fprintf(f, "0");
        return;
    }

    for (i = ADDR_BITS - 1; i >= 0; i--)
    {
        buf[ADDR_BITS - 1 - i] = ((val >> i) & 1) ? '1' : '0';
    }
    buf[ADDR_BITS] = '\0';

    /* Skip leading zeros */
    started = 0;
    for (i = 0; i < ADDR_BITS; i++)
    {
        if (buf[i] == '1')
            started = 1;
        if (started)
            fputc(buf[i], f);
    }
}

void mmu_log_page_fault(FILE *log, int va, int pid)
{
    fprintf(log, "PageFault upon VA ");
    fprint_binary(log, va);
    fprintf(log, " from process %d\n", pid);
    fflush(log);
}

void mmu_log_free_frame(FILE *log, int frame)
{
    fprintf(log, "Free Physical page %d allocated\n", frame);
    fflush(log);
}

void mmu_log_swap_out(FILE *log, int frame)
{
    fprintf(log, "Swapping out page %d to disk\n", frame);
    fflush(log);
}

void mmu_log_loaded(FILE *log, int now, int disk_addr, int pid, int frame)
{
    fprintf(log, "At time %d disk address %d for process %d is loaded into memory page %d.\n",
            now, disk_addr, pid, frame);
    fflush(log);
}
