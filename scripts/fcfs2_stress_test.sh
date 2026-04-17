#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -x ./process_generator.out || ! -x ./master_scheduler.out || ! -x ./scheduler.out || ! -x ./process.out || ! -x ./clk.out ]]; then
    make build
fi

TMP_DIR="$(mktemp -d)"
ORIG_PROCESSES="$TMP_DIR/processes.orig"
if [[ -f processes.txt ]]; then
    cp processes.txt "$ORIG_PROCESSES"
fi

cleanup() {
    pkill -f 'process_generator.out|master_scheduler.out|scheduler.out|clk.out|process.out' >/dev/null 2>&1 || true
    ipcrm -Q 1234 >/dev/null 2>&1 || true
    ipcrm -S 1235 >/dev/null 2>&1 || true
    ipcrm -S 2235 >/dev/null 2>&1 || true
    ipcrm -S 2236 >/dev/null 2>&1 || true
    ipcrm -S 2237 >/dev/null 2>&1 || true
    ipcrm -S 2238 >/dev/null 2>&1 || true
    ipcrm -S 2239 >/dev/null 2>&1 || true
    ipcrm -M 2234 >/dev/null 2>&1 || true

    if [[ -f "$ORIG_PROCESSES" ]]; then
        cp "$ORIG_PROCESSES" processes.txt
    fi
    rm -rf "$TMP_DIR"
}
trap cleanup EXIT

require_file() {
    local path="$1"
    if [[ ! -s "$path" ]]; then
        echo "FAIL: missing or empty file: $path"
        return 1
    fi
}

check_perf_file() {
    local path="$1"
    require_file "$path"
    grep -Eq '^CPU utilization = [0-9]+(\.[0-9]+)?%$' "$path"
    grep -Eq '^Avg WTA = -?[0-9]+(\.[0-9]+)?$' "$path"
    grep -Eq '^Avg Waiting = -?[0-9]+(\.[0-9]+)?$' "$path"
    grep -Eq '^Std WTA = -?[0-9]+(\.[0-9]+)?$' "$path"
}

check_log_file() {
    local path="$1"
    require_file "$path"
    grep -Eq '^#At time x process y state arr w total z remain y wait k$' "$path"
}

run_case() {
    local case_name="$1"
    local n="$2"
    local m="$3"
    local expect_steal="$4"
    local output_file="$TMP_DIR/${case_name}.out"

    shift 4

    printf '%s\n' "$@" > processes.txt
    rm -f scheduler_1.log scheduler_2.log scheduler_1.perf scheduler_2.perf

    pkill -f 'process_generator.out|master_scheduler.out|scheduler.out|clk.out|process.out' >/dev/null 2>&1 || true
    ipcrm -Q 1234 >/dev/null 2>&1 || true
    ipcrm -S 1235 >/dev/null 2>&1 || true
    ipcrm -S 2235 >/dev/null 2>&1 || true
    ipcrm -S 2236 >/dev/null 2>&1 || true
    ipcrm -S 2237 >/dev/null 2>&1 || true
    ipcrm -S 2238 >/dev/null 2>&1 || true
    ipcrm -S 2239 >/dev/null 2>&1 || true
    ipcrm -M 2234 >/dev/null 2>&1 || true

    echo "=== Running case: $case_name (N=$n, M=$m) ==="

    local attempt
    local ok=0
    for attempt in 1 2; do
        if timeout 120s setsid bash -lc "cd '$ROOT_DIR' && printf '3\\n${n}\\n${m}\\n' | ./process_generator.out" >"$output_file" 2>&1; then
            ok=1
            break
        fi

        pkill -f 'process_generator.out|master_scheduler.out|scheduler.out|clk.out|process.out' >/dev/null 2>&1 || true
        ipcrm -Q 1234 >/dev/null 2>&1 || true
        ipcrm -S 1235 >/dev/null 2>&1 || true
        ipcrm -S 2235 >/dev/null 2>&1 || true
        ipcrm -S 2236 >/dev/null 2>&1 || true
        ipcrm -S 2237 >/dev/null 2>&1 || true
        ipcrm -S 2238 >/dev/null 2>&1 || true
        ipcrm -S 2239 >/dev/null 2>&1 || true
        ipcrm -M 2234 >/dev/null 2>&1 || true

        if [[ "$attempt" -eq 1 ]]; then
            echo "WARN: case '$case_name' attempt 1 failed, retrying once..."
        fi
    done

    if [[ "$ok" -ne 1 ]]; then
        echo "FAIL: case '$case_name' hung or crashed after retries"
        echo "--- process output ---"
        cat "$output_file"
        return 1
    fi

    check_log_file scheduler_1.log
    check_log_file scheduler_2.log
    check_perf_file scheduler_1.perf
    check_perf_file scheduler_2.perf

    local steal_count
    steal_count="$(awk '/^At time [0-9]+ process [0-9]+ was stolen$/{c++} END{print c+0}' scheduler_1.log scheduler_2.log)"

    if [[ "$expect_steal" == "yes" && "$steal_count" -eq 0 ]]; then
        echo "FAIL: case '$case_name' expected at least one steal log line"
        echo "--- scheduler_1.log ---"
        cat scheduler_1.log
        echo "--- scheduler_2.log ---"
        cat scheduler_2.log
        return 1
    fi

    if [[ "$expect_steal" == "no" && "$steal_count" -ne 0 ]]; then
        echo "FAIL: case '$case_name' expected no steal log lines, got $steal_count"
        echo "--- scheduler_1.log ---"
        cat scheduler_1.log
        echo "--- scheduler_2.log ---"
        cat scheduler_2.log
        return 1
    fi

    echo "PASS: $case_name (steal lines: $steal_count)"
}

run_case \
  "baseline_no_steal" \
  "3" \
  "100" \
  "no" \
  $'#id\tarrival\truntime\tpriority' \
  $'1\t1\t3\t4' \
  $'2\t2\t3\t3' \
  $'3\t3\t3\t2' \
  $'4\t4\t3\t1'

run_case \
  "spec_like_with_steal" \
  "3" \
  "3" \
  "yes" \
  $'#id\tarrival\truntime\tpriority' \
  $'1\t0\t10\t1' \
  $'2\t0\t9\t2' \
  $'3\t1\t8\t3' \
  $'4\t2\t6\t4' \
  $'5\t3\t1\t5'

run_case \
  "same_tick_burst" \
  "2" \
  "1" \
  "yes" \
  $'#id\tarrival\truntime\tpriority' \
  $'1\t0\t9\t1' \
  $'2\t0\t8\t2' \
  $'3\t0\t7\t3' \
  $'4\t0\t6\t4' \
  $'5\t0\t5\t5' \
  $'6\t1\t1\t6'

echo "All FCFS-2 stress checks passed."
