#!/usr/bin/env bash
# Sequential-scan micro-benchmark for the translation path.
#
# Configurable via env vars:
#   N=pages F=frames T=threads S=exec_secs W=warmup_secs
#
# Default sizing (TLB-favorable, exposes L1 vs L2/L3 metadata cost):
#   N=2000 (working set fits in 4K-entry TLB → ~100% hit after warmup)
#   F=200000 (LIPAH frame-metadata array = 12 MB → L3-resident)
#
# Other useful configs:
#   N=4000 F=4000      → both fit in L2; tighter TLB
#   N=100000 F=200000  → working set thrashes TLB; tests miss/prefill path
#   N=500 F=500        → both fit in L1; LIPAH usually wins
set -euo pipefail

N=${N:-2000}        # pages (working set)
F=${F:-200000}      # frames (BP capacity; sets LIPAH metadata array size)
T=${T:-1}           # threads
S=${S:-10}          # exec seconds
W=${W:-3}           # warmup seconds
BIN_SRC="bp_translation_bench"

TARGET="./target/release"
OUTDIR="bench_translation_seq_results"
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

echo "=== Sequential-scan translation benchmark ===" | tee "$SUMMARY"
echo "Config: n=$N f=$F t=$T warmup=${W}s exec=${S}s" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

# Build each variant
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    echo "Building $label (features: $features)..." | tee -a "$SUMMARY"
    cargo build --release --bin "$BIN_SRC" --features "$features" 2>&1 | tail -1
    cp -f "$TARGET/$BIN_SRC" "$TARGET/${BIN_SRC}_${tag}"
done
echo "" | tee -a "$SUMMARY"

printf "%-12s %-14s %-14s %-14s\n" "variant" "ops" "Mops/s" "ns/op" | tee -a "$SUMMARY"

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    BIN="$TARGET/${BIN_SRC}_${tag}"

    LOGFILE="$OUTDIR/${tag}_${TIMESTAMP}.log"
    echo "Running $label..." | tee -a "$SUMMARY"

    "$BIN" --sequential -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" >"$LOGFILE" 2>&1 || true

    ops=$(grep "^Total ops:" "$LOGFILE" | awk '{print $3}' || echo "N/A")
    mops=$(grep "^Throughput:" "$LOGFILE" | grep -oP '\(\K[0-9.]+' || echo "N/A")
    nsop=$(grep "^Avg latency:" "$LOGFILE" | grep -oP '[0-9.]+' | head -1 || echo "N/A")

    printf "%-12s %-14s %-14s %-14s\n" "$label" "$ops" "$mops" "$nsop" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "=== Done. Logs in $OUTDIR/ ===" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
