#!/usr/bin/env bash
set -euo pipefail

W=10
T=40
WARMUP=3
EXEC=20
ARGS="-w $W -t $T -d $WARMUP -D $EXEC"
BIN="tpcc_profile_neworder"
OUTDIR="bench_pt_results"
mkdir -p "$OUTDIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$OUTDIR/summary_${TIMESTAMP}.txt"

echo "=== PT Comparison Benchmark ===" | tee "$SUMMARY"
echo "Config: $W warehouses, $T threads, ${WARMUP}s warmup, ${EXEC}s exec" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

declare -a VARIANTS=(
    "bp_clock|Clock"
    "bp_pt_bucket pt_counts|PT one-hash fp"
    "bp_pt2_bucket pt_counts|PT two-hash fp"
    "bp_pt4_bucket pt_counts|PT four-hash fp"
    "bp_dashmap|DashMap"
)

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    echo "--- Running: $label (features: $features) ---" | tee -a "$SUMMARY"

    LOGFILE="$OUTDIR/${label// /_}_${TIMESTAMP}.log"

    cargo run --release --bin "$BIN" --features "$features" -- $ARGS 2>&1 | tee "$LOGFILE"

    # Extract key metrics
    commits=$(grep "^NewOrder" "$LOGFILE" | grep -oP 'commits: \K[0-9,]+' | tr -d ',')
    avg_txn=$(grep "^Total Transaction" "$LOGFILE" | grep -oP 'avg:\s+\K[0-9.]+\s+[µm]s')
    max_txn=$(grep "^Total Transaction" "$LOGFILE" | grep -oP 'max:\s+\K[0-9.]+\s+[µm]s')
    pref_hits=$(grep "Preferred frame hits:" "$LOGFILE" | grep -oP '\(\K[0-9.]+%' || echo "N/A")
    overflow=$(grep "Overflow chain hits:" "$LOGFILE" | grep -oP '\(\K[0-9.]+%' || echo "N/A")

    printf "%-20s commits=%-10s avg=%-12s max=%-12s pref=%-8s overflow=%-8s\n" \
        "$label" "$commits" "$avg_txn" "$max_txn" "$pref_hits" "$overflow" | tee -a "$SUMMARY"
    echo "" | tee -a "$SUMMARY"
done

echo "=== Done. Full logs in $OUTDIR/ ===" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
