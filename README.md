# OS Scheduler Simulator

A process scheduling and memory management simulator supporting multiple CPU scheduling algorithms with virtual memory and page replacement.

## Architecture

```
process_generator  →  clock  →  scheduler  →  process (per PCB)
                            ↘  master_scheduler
                                 ├─ scheduler (CPU 1)
                                 └─ scheduler (CPU 2)
```

| Component | File | Description |
|---|---|---|
| Process Generator | `process_generator.c` | Reads `processes.txt`, forks clock & scheduler, sends PCBs via SysV message queue |
| Clock | `clk.c` | Emulated system clock using shared memory (1 tick/sec) |
| Scheduler | `scheduler.c` | HPF / RR / FCFS-2 child scheduler with MMU integration |
| Master Scheduler | `master_scheduler.c` | FCFS-2 orchestrator: routes messages, handles work stealing between 2 CPUs |
| Process | `process.c` | Simulated process: sends compute/memory requests per tick |
| MMU | `mmu.c` / `mmu.h` | Virtual memory: 10-bit VA, 32 physical frames, NRU page replacement |
| Test Generator | `test_generator.c` | Generates random `processes.txt` files |

## Scheduling Algorithms

| ID | Algorithm | Description |
|---|---|---|
| 1 | **HPF** | Preemptive Highest Priority First — priority-ordered ready queue, preempts on higher-priority arrival |
| 2 | **RR** | Round Robin — FIFO queue with configurable quantum Q and R-bit clear period K (NRU) |
| 3 | **FCFS-2** | 2-CPU FCFS with Work Stealing — master distributes processes, steals tail process from heavier CPU every N ticks if imbalance > M |

## Memory Management (Phase 2)

- **Virtual address**: 10-bit (6-bit VPN + 4-bit offset) → 64 pages × 16 bytes
- **Physical RAM**: 512 bytes / 32 frames
- **Page replacement**: NRU (Not Recently Used) — R bit cleared every K RR quantums
- **Page fault handling**: process blocked for disk I/O ticks (10 clean, 20 dirty write-back)
- **Per-process page table**: stored in scheduler address space; one physical frame reserved per process

## Build & Run

```bash
make            # compile all binaries
make run        # run the simulator
make clean      # remove binaries and generated files
make all        # clean + build
```

### Input Format

`processes.txt` (Phase 2 format):

```
#id  arrival  runtime  priority  base  limit
1    0        30       3         0     4
2    5        20       1         10    2
```

Phase 1 format (4 fields) is also accepted — `base=0`, `limit=1` is assumed.

Memory request files: `requests_<id>.txt`

```
#tick  address  r/w
1      0x10     r
3      0x20     w
```

## IPC Resources

| Key | Type | Purpose |
|---|---|---|
| `MSG_KEY` | Message Queue | Generator → Scheduler PCB delivery |
| `TICK_SYNC_SEM_KEY` | Semaphore | Tick synchronization |
| `PROC_SYNC_SEM_KEY` | Semaphore | Process-scheduler per-tick sync |
| `PROC_REQ_MQ_KEY` | Message Queue | Process → Scheduler requests |
| `PROC_ACK_MQ_KEY` | Message Queue | Scheduler → Process acknowledgments |
| `FCFS2_CTRL_SHM_KEY` | Shared Memory | FCFS-2 control block |

## Output Files

| File | Description |
|---|---|
| `scheduler.log` | Process lifecycle events (started/stopped/resumed/finished) |
| `scheduler.perf` | Performance metrics (CPU util, avg WTA, avg waiting, std WTA) |
| `memory.log` | Page fault, frame allocation, swap-out, and load events |

## Test Cases

- `phase1_tc/` — Phase 1 test cases with expected results
- `phase2_tc/` — Phase 2 test cases (sample1–4) with processes and memory requests
- `phase2_expected/` — Expected `memory.log` outputs for Phase 2 samples
- `expected_perf/` — Expected performance outputs for Phase 1
