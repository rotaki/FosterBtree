#!/usr/bin/env bash
set -euo pipefail

# Compare fullrow vs hotcold partition modes on TPC-C2
# Usage: ./scripts/compare_partition_modes.sh [warehouses] [exec_time]

WAREHOUSES="${1:-1}"
EXEC_TIME="${2:-10}"
WARMUP=3
THREADS="1 2 4 8 14"
MODES="fullrow hotcold"

RESULTS_DIR="results/partition_compare_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo "=== TPC-C2 Partition Mode Comparison ==="
echo "Warehouses: $WAREHOUSES"
echo "Exec time:  ${EXEC_TIME}s (warmup: ${WARMUP}s)"
echo "Threads:    $THREADS"
echo "Modes:      $MODES"
echo "Results:    $RESULTS_DIR"
echo ""

# Build release
echo "Building release binary..."
cargo build --release --bin tpcc2 2>&1 | tail -1
BIN="./target/release/tpcc2"
echo ""

# CSV header
CSV="$RESULTS_DIR/results.csv"
echo "mode,threads,throughput,commits,user_aborts,sys_aborts" > "$CSV"

for MODE in $MODES; do
    for T in $THREADS; do
        echo "--- Running: mode=$MODE threads=$T ---"
        LOG="$RESULTS_DIR/${MODE}_t${T}.log"

        "$BIN" \
            -w "$WAREHOUSES" \
            -t "$T" \
            -d "$WARMUP" \
            -D "$EXEC_TIME" \
            -p "$MODE" \
            2>&1 | tee "$LOG"

        # Parse results from log
        THROUGHPUT=$(grep "^Throughput:" "$LOG" | awk '{print $2}')
        COMMITS=$(grep "^    commits:" "$LOG" | awk '{print $2}')
        UA=$(grep "^    usr_aborts:" "$LOG" | awk '{print $2}')
        SA=$(grep "^    sys_aborts:" "$LOG" | awk '{print $2}')

        echo "$MODE,$T,$THROUGHPUT,$COMMITS,$UA,$SA" >> "$CSV"
        echo ""
    done
done

echo "=== Summary ==="
echo ""
column -t -s',' "$CSV"
echo ""
echo "Results saved to $RESULTS_DIR"
