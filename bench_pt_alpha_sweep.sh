#!/usr/bin/env bash
# Sweep load factor alpha = dataset / BP_size, comparing
# LIPAH (bp_clock) vs PT-FP-1, PT-FP-2, PT-FP-4.
#
# Fixed: warehouses, threads, warmup, exec. Varies: BP size (GB).
# 10 warehouses ≈ 1.6 GB dataset, so:
#   8 GB -> alpha ~ 0.2
#   4 GB -> alpha ~ 0.4
#   2 GB -> alpha ~ 0.8
#   1 GB -> alpha ~ 1.6 (OOM; eviction + disk I/O active)

set -euo pipefail

W=10
T=4
WARMUP=3
EXEC=10
BIN_SRC="tpcc_profile_neworder"

TARGET="./target/release"
OUTDIR="bench_pt_alpha_results"
mkdir -p "$OUTDIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$OUTDIR/summary_${TIMESTAMP}.txt"

declare -a BP_SIZES=(8 4 2 1)
declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_pt_bucket|PT-FP-1"
    "bp_pt2_bucket|PT-FP-2"
    "bp_pt4_bucket|PT-FP-4"
)

echo "=== PT alpha sweep ===" | tee "$SUMMARY"
echo "Config: w=$W t=$T warmup=${WARMUP}s exec=${EXEC}s" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo ""  | tee -a "$SUMMARY"

# --- Build each variant once, copy binary to a tag ---
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    echo "Building $label (features: $features)..." | tee -a "$SUMMARY"
    cargo build --release --bin "$BIN_SRC" --features "$features" >/dev/null 2>&1
    cp -f "$TARGET/$BIN_SRC" "$TARGET/${BIN_SRC}_${tag}"
done
echo "" | tee -a "$SUMMARY"

# --- Header ---
printf "%-10s %-5s %-10s %-10s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
    "variant" "bp" "commits" "avg(µs)" "p50" "p90" "p99" "p99.9" "max" "pref%" "fault%" | tee -a "$SUMMARY"

# --- Sweep ---
for bp_gb in "${BP_SIZES[@]}"; do
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        BIN="$TARGET/${BIN_SRC}_${tag}"

        LOGFILE="$OUTDIR/${tag}_bp${bp_gb}_${TIMESTAMP}.log"

        "$BIN" -w "$W" -t "$T" -b "$bp_gb" -d "$WARMUP" -D "$EXEC" >"$LOGFILE" 2>&1 || true

        commits=$(grep "^NewOrder" "$LOGFILE" | grep -oP 'commits: \K[0-9,]+' | tr -d ',' || echo "N/A")
        avg_txn=$(grep "^  mean" "$LOGFILE" | grep -oP '[0-9.]+' | head -1 || echo "N/A")
        p50=$(grep "^  p50" "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p90=$(grep "^  p90" "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p99=$(grep "^  p99 " "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p999=$(grep "^  p99.9 " "$LOGFILE" | awk '{print $2}' || echo "N/A")
        max_us=$(grep "^  max" "$LOGFILE" | awk '{print $2}' || echo "N/A")
        pref=$(grep "Preferred frame hits:" "$LOGFILE" | grep -oP '\(\K[0-9.]+%' | head -1 || echo "N/A")
        faults=$(grep "Page faults:" "$LOGFILE" | grep -oP '\(\K[0-9.]+%' | head -1 || echo "N/A")

        printf "%-10s %-5s %-10s %-10s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
            "$label" "${bp_gb}G" "$commits" "$avg_txn" "$p50" "$p90" "$p99" "$p999" "$max_us" "$pref" "$faults" | tee -a "$SUMMARY"
    done
    echo "" | tee -a "$SUMMARY"
done

echo "=== Done. Logs in $OUTDIR/ ===" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
