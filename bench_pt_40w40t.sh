#!/usr/bin/env bash
# 40 warehouses, 40 threads, 40 GB BP (fully in-memory)
# Comparing LIPAH, PT-FP-1, TLB-Only
set -euo pipefail

W=40
T=40
BP=40
WARMUP=5
EXEC=30
BIN_SRC="tpcc_profile_neworder"

TARGET="./target/release"
OUTDIR="bench_pt_40w40t_results"
mkdir -p "$OUTDIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$OUTDIR/summary_${TIMESTAMP}.txt"

declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_pt_bucket|PT-FP-1"
    "bp_pt_tlb_only|TLB-only"
    "bp_tlb|TLB-congee"
    "bp_tlb,tlb_victim_cache|TLB-victim"
)

echo "=== TLB 40w40t benchmark ===" | tee "$SUMMARY"
echo "Config: w=$W t=$T bp=${BP}GB warmup=${WARMUP}s exec=${EXEC}s" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

# Build each variant (show errors instead of hiding them)
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    echo "Building $label (features: $features)..." | tee -a "$SUMMARY"
    cargo build --release --bin "$BIN_SRC" --features "$features" 2>&1 | tail -1
    cp -f "$TARGET/$BIN_SRC" "$TARGET/${BIN_SRC}_${tag}"
done
echo "" | tee -a "$SUMMARY"

# Header
printf "%-10s %-10s %-10s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
    "variant" "commits" "avg(µs)" "p50" "p75" "p90" "p95" "p99" "p99.9" "max" | tee -a "$SUMMARY"

# Run
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    BIN="$TARGET/${BIN_SRC}_${tag}"

    LOGFILE="$OUTDIR/${tag}_${TIMESTAMP}.log"

    echo "Running $label..." | tee -a "$SUMMARY"
    "$BIN" -w "$W" -t "$T" -b "$BP" -d "$WARMUP" -D "$EXEC" >"$LOGFILE" 2>&1 || true

    commits=$(grep "^NewOrder" "$LOGFILE" | grep -oP 'commits: \K[0-9,]+' | tr -d ',' || echo "N/A")
    avg=$(grep "^  mean" "$LOGFILE" | grep -oP '[0-9.]+' | head -1 || echo "N/A")
    p50=$(grep "^  p50" "$LOGFILE" | awk '{print $2}' || echo "N/A")
    p75=$(grep "^  p75" "$LOGFILE" | awk '{print $2}' || echo "N/A")
    p90=$(grep "^  p90" "$LOGFILE" | awk '{print $2}' || echo "N/A")
    p95=$(grep "^  p95" "$LOGFILE" | awk '{print $2}' || echo "N/A")
    p99=$(grep "^  p99 " "$LOGFILE" | awk '{print $2}' || echo "N/A")
    p999=$(grep "^  p99.9 " "$LOGFILE" | awk '{print $2}' || echo "N/A")
    max_us=$(grep "^  max" "$LOGFILE" | awk '{print $2}' || echo "N/A")

    printf "%-10s %-10s %-10s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
        "$label" "$commits" "$avg" "$p50" "$p75" "$p90" "$p95" "$p99" "$p999" "$max_us" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"

# Print histograms
echo "=== Latency Histograms ===" | tee -a "$SUMMARY"
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    LOGFILE="$OUTDIR/${tag}_${TIMESTAMP}.log"

    echo "" | tee -a "$SUMMARY"
    echo "--- $label ---" | tee -a "$SUMMARY"
    grep -A 20 "Log-scale histogram" "$LOGFILE" | head -20 | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "=== Done. Logs in $OUTDIR/ ===" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
