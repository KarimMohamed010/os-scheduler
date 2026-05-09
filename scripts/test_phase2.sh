#!/bin/bash

cleanup() {
    echo "Cleaning IPC and processes..."
    pkill -f process_generator.out 2>/dev/null
    pkill -f scheduler.out 2>/dev/null
    pkill -f clk.out 2>/dev/null
    pkill -f process.out 2>/dev/null

    for id in $(ipcs -m | awk '$3 == ENVIRON["USER"] {print $2}'); do
        ipcrm -m "$id" 2>/dev/null
    done
    for id in $(ipcs -s | awk '$3 == ENVIRON["USER"] {print $2}'); do
        ipcrm -s "$id" 2>/dev/null
    done
    for id in $(ipcs -q | awk '$3 == ENVIRON["USER"] {print $2}'); do
        ipcrm -q "$id" 2>/dev/null
    done
}

trap cleanup EXIT SIGINT

normalize_log() {
    awk '!/^[[:space:]]*#/ && !/^[[:space:]]*$/' "$1"
}

echo "Building the project..."
make clean
make

echo "---------------------------------------------------"
echo "Running Phase 2 Sample 1 (RR, Q=3, K=1)"
echo "---------------------------------------------------"
cleanup
rm -f requests_*.txt memory.log scheduler.log
cp phase2_tc/sample1/* .

echo -e "2\n3\n1\n" | ./process_generator.out processes.txt

echo "Comparing memory.log for Sample 1..."
if diff -q <(normalize_log memory.log) <(normalize_log phase2_expected/sample1_memory.log) >/dev/null; then
    echo "[PASS] Sample 1 memory.log matches exactly."
else
    echo "[FAIL] Sample 1 memory.log differs."
    diff -u <(normalize_log memory.log) <(normalize_log phase2_expected/sample1_memory.log)
fi

echo "---------------------------------------------------"
echo "Running Phase 2 Sample 2 (RR, Q=1, K=100)"
echo "---------------------------------------------------"
cleanup
rm -f requests_*.txt memory.log scheduler.log
cp phase2_tc/sample2/* .

echo -e "2\n1\n100\n" | ./process_generator.out processes.txt

echo "Comparing memory.log for Sample 2..."
if diff -q <(normalize_log memory.log) <(normalize_log phase2_expected/sample2_memory.log) >/dev/null; then
    echo "[PASS] Sample 2 memory.log matches exactly."
else
    echo "[FAIL] Sample 2 memory.log differs."
    diff -u <(normalize_log memory.log) <(normalize_log phase2_expected/sample2_memory.log)
fi

echo "---------------------------------------------------"
echo "Tests Done."