#!/usr/bin/env bash
# Phase-shift benchmark: measures workload migration cost.
# First half accesses pages 0..N/2, second half accesses N/2..N.
# Under memory pressure (60K frames for 100K pages).
set -euo pipefail

PAGES=100000
FRAMES=60000
SECONDS=10
THREADS=1
WARMUP=2

OUTDIR="bench_phase_shift_results"
mkdir -p "$OUTDIR"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$OUTDIR/summary_${TIMESTAMP}.txt"

declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_pt|PT"
    "bp_pt_bucket|PT-FP-1"
    "bp_tlb|TLB-congee"
    "bp_tlb,tlb_victim_cache|TLB-victim"
)

echo "=== Phase-Shift Benchmark ===" | tee "$SUMMARY"
echo "Config: pages=$PAGES frames=$FRAMES threads=$THREADS seconds=$SECONDS warmup=$WARMUP" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

printf "%-12s %-12s %-12s\n" "variant" "ops" "avg_ns" | tee -a "$SUMMARY"

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')

    echo "Building $label..." | tee -a "$SUMMARY"
    cargo build --release --bin bp_translation_bench --features "$features" 2>&1 | tail -1

    LOGFILE="$OUTDIR/${tag}_${TIMESTAMP}.log"
    echo "Running $label..." | tee -a "$SUMMARY"

    cargo run --release --features "$features" --bin bp_translation_bench \
        -- --phase-shift -n "$PAGES" -f "$FRAMES" -s "$SECONDS" -t "$THREADS" -w "$WARMUP" \
        >"$LOGFILE" 2>&1 || true

    total_ops=$(grep "Total ops:" "$LOGFILE" | awk '{print $3}' || echo "N/A")
    avg_ns=$(grep "Avg latency:" "$LOGFILE" | awk '{print $3}' || echo "N/A")

    printf "%-12s %-12s %-12s\n" "$label" "$total_ops" "$avg_ns" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "=== Done. Logs in $OUTDIR/ ===" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
