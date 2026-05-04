#!/bin/bash

cleanup() {
    echo "Cleaning IPC and processes..."

    pkill -f process_generator.out 2>/dev/null
    pkill -f scheduler.out 2>/dev/null
    pkill -f clk.out 2>/dev/null
    pkill -f process.out 2>/dev/null

    for id in $(ipcs -m | awk '$3 == ENVIRON["USER"] {print $2}'); do
        ipcrm -m "$id"
    done

    for id in $(ipcs -s | awk '$3 == ENVIRON["USER"] {print $2}'); do
        ipcrm -s "$id"
    done

    for id in $(ipcs -q | awk '$3 == ENVIRON["USER"] {print $2}'); do
        ipcrm -q "$id"
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
cp phase2_tc/sample1/* .

# Run process_generator with RR (algo 2), Q=3, K=1
echo -e "2\n3\n1\n" | ./process_generator.out processes.txt

sleep 1 # Ensure cleanup

echo "Comparing memory.log for Sample 1..."
if diff -q <(normalize_log memory.log) <(normalize_log phase2_expected/sample1_memory.log) >/dev/null; then
    echo "[PASS] Sample 1 memory.log matches exactly."
else
    echo "[FAIL] Sample 1 memory.log differs."
    diff -u <(normalize_log memory.log) <(normalize_log phase2_expected/sample1_memory.log)
fi

echo "---------------------------------------------------"
echo "Running Phase 2 Sample 2 (RR, Q=4, K=1)"
echo "---------------------------------------------------"
# Delete previous request files
rm requests_*.txt 2>/dev/null
cp phase2_tc/sample2/* .

# Wait, the prompt says 'Sample 2' which appears to be RR with some quantum. 
# Prompt didn't mention quantum, let's assume Q=4, K=1? The output shows many processes finishing.
# Wait, Sample 2 doesn't mention quantum. From 1 to 16. 1 runtime 4... Q=4 seems a good guess since the time gap is 2, 4 is max runtime. Let's use Q=4.
echo -e "2\n4\n1\n" | ./process_generator.out processes.txt

sleep 1

echo "Comparing memory.log for Sample 2..."
if diff -q <(normalize_log memory.log) <(normalize_log phase2_expected/sample2_memory.log) >/dev/null; then
    echo "[PASS] Sample 2 memory.log matches exactly."
else
    echo "[FAIL] Sample 2 memory.log differs."
    diff -u <(normalize_log memory.log) <(normalize_log phase2_expected/sample2_memory.log)
fi

echo "---------------------------------------------------"
echo "Tests Done."
