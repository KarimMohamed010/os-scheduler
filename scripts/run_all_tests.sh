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
trap cleanup SIGINT SIGTERM

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

files_equal_relaxed() {
    local left="$1"
    local right="$2"

    if diff -q "$left" "$right" >/dev/null 2>&1; then
        return 0
    fi

    # Ignore EOF newline-only mismatches in expected fixtures.
    if diff -q <(awk '{ print }' "$left") <(awk '{ print }' "$right") >/dev/null 2>&1; then
        return 0
    fi

    return 1
}

run_test() {
    local test_file="$1"
    local base_name="$(basename "$test_file" .txt)"
    local tc_num=$(echo "$base_name" | grep -o 'tc[0-9]*' | sed 's/tc//')
    
    # Defaults
    local algo_choice=""
    local algo_args=""
    local multi_cpu=0
    
    if echo "$base_name" | grep -q "hpf"; then
        algo_choice="1"
    elif echo "$base_name" | grep -q "fcfs"; then
        algo_choice="3"
        multi_cpu=1
        local n_val=$(echo "$base_name" | grep -o 'n_[0-9]*' | sed 's/n_//')
        local m_val=$(echo "$base_name" | grep -o 'm_[0-9]*' | sed 's/m_//')
        algo_args="${n_val}\n${m_val}\n"
    elif echo "$base_name" | grep -q "rr"; then
        algo_choice="2"
        local q_val=$(echo "$base_name" | grep -o 'rr[0-9]*' | sed 's/rr//')
        algo_args="${q_val}\n"
    else
        echo "ERROR: Unknown test type in filename: $base_name"
        exit 1
    fi

    cp "$test_file" processes.txt
    rm -f scheduler.log scheduler.perf scheduler_1.log scheduler_2.log scheduler_1.perf scheduler_2.perf


    local output_file="$TMP_DIR/${base_name}.out"
    
    timeout 120s bash -c "printf '${algo_choice}\n${algo_args}' | ./process_generator.out" >"$output_file" 2>&1 || true
    
    local test_passed=1
    local fail_reason=""
    
    if [[ $multi_cpu -eq 1 ]]; then
        for i in 1 2; do
            local expected_log="phase1_tc/results/${tc_num}_${i}.log"
            local expected_perf="phase1_tc/results/${tc_num}_${i}.perf"
            
            if [[ ! -f "$expected_perf" ]]; then
                # Some FCFS results are single .perf
                local expected_perf_fallback="phase1_tc/results/${tc_num}.perf"
                if [[ -f "$expected_perf_fallback" ]]; then
                    # Compare single perf file
                    if ! files_equal_relaxed "scheduler.perf" "$expected_perf_fallback"; then
                        test_passed=0
                        fail_reason+="Mismatched scheduler.perf and ${tc_num}.perf\n"
                    fi
                fi
            else
                if [[ ! -f "scheduler_${i}.perf" ]] || ! files_equal_relaxed "scheduler_${i}.perf" "$expected_perf"; then
                    test_passed=0
                    fail_reason+="Mismatched scheduler_${i}.perf and ${tc_num}_${i}.perf\n"
                fi
            fi
            
            # Since FCFS in this codebase outputs to scheduler_1.log and scheduler_2.log for multi cpu
            if [[ -f "$expected_log" ]]; then
                  if [[ ! -f "scheduler_${i}.log" ]] || ! files_equal_relaxed "scheduler_${i}.log" "$expected_log"; then
                    test_passed=0
                    fail_reason+="Mismatched scheduler_${i}.log and ${tc_num}_${i}.log\n"
                 fi
            fi
        done
        
        # fallback if logs are not split
        if [[ ! -f "phase1_tc/results/${tc_num}_1.log" && -f "phase1_tc/results/${tc_num}.log" ]]; then
               if [[ ! -f "scheduler.log" ]] || ! files_equal_relaxed "scheduler.log" "phase1_tc/results/${tc_num}.log"; then
                 test_passed=0
                 fail_reason+="Mismatched scheduler.log and ${tc_num}.log\n"
             fi
        fi

    else
        local expected_log="phase1_tc/results/${tc_num}.log"
        local expected_perf="phase1_tc/results/${tc_num}.perf"
        
        if [[ ! -f "$expected_log" ]]; then
            echo "ERROR: Missing expected log file $expected_log"
            exit 1
        fi
        if [[ ! -f "$expected_perf" ]]; then
            echo "ERROR: Missing expected perf file $expected_perf"
            exit 1
        fi
        
        if ! files_equal_relaxed "scheduler.log" "$expected_log"; then
            test_passed=0
            fail_reason+="Mismatched log file\n$(diff -u "$expected_log" "scheduler.log" || true)\n"
        fi
        
        if ! files_equal_relaxed "scheduler.perf" "$expected_perf"; then
            test_passed=0
            fail_reason+="Mismatched perf file\n$(diff -u "$expected_perf" "scheduler.perf" || true)\n"
        fi
    fi
    
    TOTAL_TESTS=$((TOTAL_TESTS+1))
    if [[ $test_passed -eq 1 ]]; then
        echo "PASS: $base_name"
        PASSED_TESTS=$((PASSED_TESTS+1))
    else
        echo "FAIL: $base_name"
        echo -e "Reason:\n$fail_reason"
        FAILED_TESTS=$((FAILED_TESTS+1))
    fi
}

echo "Starting tests..."
for f in phase1_tc/*rr*.txt; do
    # Skip alternate data streams (Windows WSL artifacts)
    if [[ "$f" == *":Zone.Identifier" ]]; then
        continue
    fi
    run_test "$f"
done

echo ""
echo "=== Test Summary ==="
echo "Total Tests: $TOTAL_TESTS"
echo "Passed: $PASSED_TESTS"
echo "Failed: $FAILED_TESTS"

if [[ $FAILED_TESTS -gt 0 ]]; then
    exit 1
else
    exit 0
fi
