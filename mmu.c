#include "mmu.h"
#include <string.h>

FrameEntry frame_table[PHYS_FRAMES];
PageTable  page_tables[MAX_PROCESSES + 1];

/* =========================================================
 *  Helpers
 * ========================================================= */

static int is_valid_pid(int pid)
{
    return pid >= 1 && pid <= MAX_PROCESSES;
}

static PageTableEntry *get_pte(int pid, int vpn)
{
    if (!is_valid_pid(pid) || vpn < 0 || vpn >= MAX_VPAGES)
    {
        return NULL;
    }
    return &page_tables[pid].entries[vpn];
}

static int va_to_vpn(int va)
{
    return (va >> OFFSET_BITS) & VPN_MASK;
}

static int va_to_offset(int va)
{
    return va & OFFSET_MASK;
}

static void clear_frame(FrameEntry *f)
{
    f->owner_pid = -1;
    f->vpn = -1;
    f->is_page_table = 0;
    f->R = 0;
    f->M = 0;
}

static void invalidate_pte_for_frame(const FrameEntry *f)
{
    PageTableEntry *pte;

    if (f->owner_pid < 1 || f->owner_pid > MAX_PROCESSES || f->vpn < 0)
    {
        return;
    }

    pte = get_pte(f->owner_pid, f->vpn);
    if (!pte)
    {
        return;
    }

    pte->present = 0;
    pte->frame = -1;
    pte->R = 0;
    pte->M = 0;
}

static void claim_frame(int frame, int pid, int vpn, int is_page_table, int R, int M)
{
    frame_table[frame].owner_pid = pid;
    frame_table[frame].vpn = vpn;
    frame_table[frame].is_page_table = is_page_table;
    frame_table[frame].R = R;
    frame_table[frame].M = M;
}

static int select_nru_victim(void)
{
    for (int cls = 0; cls < 4; ++cls)
    {
        for (int i = 0; i < PHYS_FRAMES; ++i)
        {
            FrameEntry *f = &frame_table[i];

            if (f->owner_pid == -1 || f->is_page_table)
            {
                continue;
            }

            int cur_class = (f->R ? 2 : 0) | (f->M ? 1 : 0);
            if (cur_class == cls)
            {
                return i;
            }
        }
    }

    return -1;
}

static int reserve_frame_quiet(void)
{
    int frame = mmu_alloc_frame();
    if (frame != -1)
    {
        return frame;
    }


    int victim = select_nru_victim();
    if (victim < 0)
    {
        return -1;
    }

    invalidate_pte_for_frame(&frame_table[victim]);
    clear_frame(&frame_table[victim]);
    return victim;
}

static void commit_page_to_frame(PCB *pcb, int vpn, int frame, int fault_write)
{
    PageTableEntry *pte = get_pte(pcb->id, vpn);
    if (!pte)
    {
        return;
    }

    pte->frame = frame;
    pte->present = 1;
    pte->R = 1;
    pte->M = fault_write ? 1 : 0;

    claim_frame(frame, pcb->id, vpn, 0, 1, fault_write ? 1 : 0);
}

/* =========================================================
 *  memory.log helpers
 * ========================================================= */

void mmu_log_page_fault(FILE *log, const char *va_bin, int pid)
{
    if (!log)
    {
        return;
    }

    fprintf(log, "PageFault upon VA %s from process %d\n", va_bin, pid);
    fflush(log);
}

void mmu_log_free_frame(FILE *log, int frame)
{
    if (!log)
    {
        return;
    }

    fprintf(log, "Free Physical page %d allocated\n", frame);
    fflush(log);
}

void mmu_log_swap_out(FILE *log, int frame)
{
    if (!log)
    {
        return;
    }

    fprintf(log, "Swapping out page %d to disk\n", frame);
    fflush(log);
}

void mmu_log_loaded(FILE *log, int now, int disk_addr, int pid, int frame)
{
    if (!log)
    {
        return;
    }

    fprintf(log, "At time %d disk address %d for process %d is loaded into memory page %d.\n",
            now, disk_addr, pid, frame);
    fflush(log);
}

/* =========================================================
 *  Core MMU
 * ========================================================= */

void mmu_init(void)
{
    for (int i = 0; i < PHYS_FRAMES; ++i)
    {
        clear_frame(&frame_table[i]);
    }

    for (int pid = 1; pid <= MAX_PROCESSES; ++pid)
    {
        for (int vpn = 0; vpn < MAX_VPAGES; ++vpn)
        {
            page_tables[pid].entries[vpn].frame = -1;
            page_tables[pid].entries[vpn].present = 0;
            page_tables[pid].entries[vpn].R = 0;
            page_tables[pid].entries[vpn].M = 0;
        }
    }
}

int mmu_alloc_frame(void)
{
    for (int i = 0; i < PHYS_FRAMES; ++i)
    {
        if (frame_table[i].owner_pid == -1)
        {
            return i;
        }
    }
    return -1;
}

void mmu_free_frame(int frame)
{
    if (frame < 0 || frame >= PHYS_FRAMES)
    {
        return;
    }

    clear_frame(&frame_table[frame]);
}

int mmu_process_init(PCB *pcb, int now, FILE *log)
{
    int pt_frame;
    int first_data_frame;

    if (!pcb || !is_valid_pid(pcb->id) || pcb->limit <= 0 || pcb->limit > MAX_VPAGES)
    {
        return -1;
    }

    /* Reset this process's page table entries */
    for (int vpn = 0; vpn < MAX_VPAGES; ++vpn)
    {
        page_tables[pcb->id].entries[vpn].frame   = -1;
        page_tables[pcb->id].entries[vpn].present = 0;
        page_tables[pcb->id].entries[vpn].R       = 0;
        page_tables[pcb->id].entries[vpn].M       = 0;
    }

    /* Allocate page-table frame. */
    pt_frame = mmu_alloc_frame();
    if (pt_frame != -1)
    {
        /* Free frame found; log it. */
        mmu_log_free_frame(log, pt_frame);
    }
    else
    {
        /* No free frame: use NRU eviction.
         * Per spec Q8/Q10: no time penalty at startup regardless. */
        pt_frame = select_nru_victim();
        if (pt_frame < 0)
        {
            fprintf(stderr, "mmu_process_init: no frame available for page table of pid %d\n",
                    pcb->id);
            return -1;
        }
        /* Dirty victim write-back is silently discarded at startup (Q10) */
        invalidate_pte_for_frame(&frame_table[pt_frame]);
        clear_frame(&frame_table[pt_frame]);
        /* No log line for NRU eviction at startup */
    }

    claim_frame(pt_frame, pcb->id, -1, 1, 0, 0);
    pcb->page_table_frame = pt_frame;

    /* Allocate frame for virtual page 0. */
    first_data_frame = mmu_alloc_frame();
    if (first_data_frame != -1)
    {
        /* Free frame found; log it. */
        mmu_log_free_frame(log, first_data_frame);
    }
    else
    {
        /* No free frame: use NRU eviction (page-table frame now occupied, so
         * it is correctly skipped by select_nru_victim via is_page_table flag) */
        first_data_frame = select_nru_victim();
        if (first_data_frame < 0)
        {
            /* Roll back PT frame */
            clear_frame(&frame_table[pt_frame]);
            pcb->page_table_frame = -1;
            fprintf(stderr, "mmu_process_init: no frame available for page 0 of pid %d\n",
                    pcb->id);
            return -1;
        }
        invalidate_pte_for_frame(&frame_table[first_data_frame]);
        clear_frame(&frame_table[first_data_frame]);
        /* No log line for NRU eviction at startup */
    }

    commit_page_to_frame(pcb, 0, first_data_frame, 0);

    /* Log the page-0 load; startup allocation has no time penalty. */
    mmu_log_loaded(log, now, pcb->base + 0, pcb->id, first_data_frame);

    pcb->state = PROC_RUNNING;
    return 0;
}

void mmu_process_exit(int pid)
{
    if (!is_valid_pid(pid))
    {
        return;
    }

    for (int i = 0; i < PHYS_FRAMES; ++i)
    {
        if (frame_table[i].owner_pid == pid)
        {
            mmu_free_frame(i);
        }
    }

    for (int vpn = 0; vpn < MAX_VPAGES; ++vpn)
    {
        page_tables[pid].entries[vpn].frame = -1;
        page_tables[pid].entries[vpn].present = 0;
        page_tables[pid].entries[vpn].R = 0;
        page_tables[pid].entries[vpn].M = 0;
    }
}

int mmu_translate(PCB *pcb, int va, int write, int *fault_vpn)
{
    int vpn = va_to_vpn(va);
    int offset = va_to_offset(va);
    PageTableEntry *pte;
    FrameEntry *f;

    if (!pcb || !is_valid_pid(pcb->id) || !fault_vpn)
    {
        return -1;
    }

    if (vpn < 0 || vpn >= pcb->limit)
    {
        *fault_vpn = vpn;
        return -1;
    }

    pte = get_pte(pcb->id, vpn);
    if (!pte || !pte->present || pte->frame < 0 || pte->frame >= PHYS_FRAMES)
    {
        *fault_vpn = vpn;
        return -1;
    }

    f = &frame_table[pte->frame];
    f->R = 1;
    pte->R = 1;

    if (write)
    {
        f->M = 1;
        pte->M = 1;
    }

    return (pte->frame * PAGE_SIZE) + offset;
}

int mmu_handle_fault(PCB *pcb, int vpn, int write,
                     int now, FILE *log, int *disk_ticks_out)
{
    int frame;
    int victim;

    if (!pcb || !is_valid_pid(pcb->id) || vpn < 0 ||
        vpn >= MAX_VPAGES || vpn >= pcb->limit)
    {
        return -1;
    }

    frame = mmu_alloc_frame();
    if (frame != -1)
    {
        mmu_log_free_frame(log, frame);
        claim_frame(frame, pcb->id, vpn, 0, 0, 0);
        if (disk_ticks_out)
        {
            *disk_ticks_out = FAULT_CHECK_TICKS + DISK_ACCESS_TICKS;
        }
        return frame;
    }

    victim = select_nru_victim();
    if (victim < 0)
    {
        return -1;
    }

    if (frame_table[victim].M)
    {
        mmu_log_swap_out(log, victim);
        if (disk_ticks_out)
        {
            *disk_ticks_out = FAULT_CHECK_TICKS + DISK_DIRTY_TICKS;
        }
    }
    else
    {
        if (disk_ticks_out)
        {
            *disk_ticks_out = FAULT_CHECK_TICKS + DISK_ACCESS_TICKS;
        }
    }

    invalidate_pte_for_frame(&frame_table[victim]);
    clear_frame(&frame_table[victim]);
    claim_frame(victim, pcb->id, vpn, 0, 0, 0);

    return victim;
}

void mmu_complete_fault(PCB *pcb, int fault_vpn, int target_frame,
                        int fault_write, int now, FILE *log)
{
    int disk_addr;

    if (!pcb || !is_valid_pid(pcb->id))
    {
        return;
    }

    disk_addr = pcb->base + fault_vpn;
    commit_page_to_frame(pcb, fault_vpn, target_frame, fault_write);
    mmu_log_loaded(log, now, disk_addr, pcb->id, target_frame);
}

void mmu_clear_r_bits(void)
{
    for (int i = 0; i < PHYS_FRAMES; ++i)
    {
        FrameEntry *f = &frame_table[i];
        PageTableEntry *pte;

        if (f->owner_pid == -1 || f->is_page_table)
        {
            continue;
        }

        f->R = 0;
        pte = get_pte(f->owner_pid, f->vpn);
        if (pte && pte->present)
        {
            pte->R = 0;
        }
    }
}
