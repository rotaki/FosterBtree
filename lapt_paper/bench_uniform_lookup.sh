#!/usr/bin/env bash
# Uniform-random point lookup workload using bp_translation_bench --no-chain.
# Each access picks a uniformly random page index — no chain dependency, so
# OoO can hide some of the translation latency. Models OLTP point lookups.
#
# Three variants compared:
#   - LIPAH (saturated, --refresh-hints)
#   - PrediCache (PT-V2)
#   - LAPT (PT-FP-V2 + congee + ophash, formerly EnhancedPrediCache-OP)
set -euo pipefail

# --- self-locating header (added when packaged into lapt_paper/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
# -------------------------------------------------------------------


N=${N:-100000}
F=${F:-200000}
T=${T:-12}
S=${S:-15}
W=${W:-5}
C=${C:-500}
BIN_SRC="bp_translation_bench"

unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/uniform_lookup_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

declare -a VARIANTS=(
    "bp_clock|LIPAH|--refresh-hints"
    "bp_predicache|PrediCache|"
    "bp_lapt|LAPT|"
)

echo "=== Uniform random point lookup ===" | tee "$SUMMARY"
echo "Config: n=$N f=$F t=$T containers=$C warmup=${W}s exec=${S}s" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

declare -A BUILT_FEATURES
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label extra_args <<< "$entry"
    if [ -z "${BUILT_FEATURES[$features]+x}" ]; then
        tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        echo "Building $label..." | tee -a "$SUMMARY"
        cargo build --release --bin "$BIN_SRC" --features "$features" 2>&1 | tail -1
        cp -f "$TARGET/$BIN_SRC" "$TARGET/${BIN_SRC}_${tag}"
        BUILT_FEATURES[$features]=1
    fi
done
echo "" | tee -a "$SUMMARY"

printf "%-12s %-14s %-14s %-14s\n" "variant" "ops" "Mops/s" "ns/op" | tee -a "$SUMMARY"
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label extra_args <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    BIN="$TARGET/${BIN_SRC}_${tag}"
    LOGFILE="$OUTDIR/${tag}.log"
    "$BIN" --no-chain -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
        --num-containers "$C" --no-page-fold $extra_args \
        >"$LOGFILE" 2>&1 || true
    ops=$(grep "^Total ops:" "$LOGFILE" | awk '{print $3}' || echo "N/A")
    mops=$(grep "^Throughput:" "$LOGFILE" | grep -oP '\(\K[0-9.]+' || echo "N/A")
    nsop=$(grep "^Avg latency:" "$LOGFILE" | grep -oP '[0-9.]+' | head -1 || echo "N/A")
    printf "%-12s %-14s %-14s %-14s\n" "$label" "$ops" "$mops" "$nsop" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "Logs in $OUTDIR/" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
