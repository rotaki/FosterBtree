#!/usr/bin/env bash
# B-tree random GET workload using fbt_on_disk_get.
# Each thread inserts its share of unique keys, then performs random gets
# across them. We extract throughput from the GET-phase BENCH_RESULT line.
set -euo pipefail

# --- self-locating header (added when packaged into lapt_paper/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
# -------------------------------------------------------------------


NUM_KEYS=${NUM_KEYS:-2000000}
KEY_SIZE=${KEY_SIZE:-100}
VAL_MIN=${VAL_MIN:-50}
VAL_MAX=${VAL_MAX:-100}
T=${T:-12}
BP_SIZE=${BP_SIZE:-200000}
BIN_SRC="fbt_on_disk_get"

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/btree_get_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_predicache|PrediCache"
    "bp_lapt|LAPT"
)

echo "=== B-tree random GET ===" | tee "$SUMMARY"
echo "Config: keys=$NUM_KEYS key_size=$KEY_SIZE val=[$VAL_MIN,$VAL_MAX] threads=$T bp_size=$BP_SIZE" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

declare -A BUILT
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    if [ -z "${BUILT[$features]+x}" ]; then
        tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        echo "Building $label..." | tee -a "$SUMMARY"
        cargo build --release --bin "$BIN_SRC" --features "$features" 2>&1 | tail -1
        cp -f "$TARGET/$BIN_SRC" "$TARGET/${BIN_SRC}_${tag}"
        BUILT[$features]=1
    fi
done
echo "" | tee -a "$SUMMARY"

printf "%-12s %-14s %-14s\n" "variant" "Mops/s" "ns/op" | tee -a "$SUMMARY"
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    BIN="$TARGET/${BIN_SRC}_${tag}"
    LOGFILE="$OUTDIR/${tag}.log"

    "$BIN" --num_keys "$NUM_KEYS" --key_size "$KEY_SIZE" \
        --val_min_size "$VAL_MIN" --val_max_size "$VAL_MAX" \
        --num_threads "$T" --bp_size "$BP_SIZE" --unique_keys \
        >"$LOGFILE" 2>&1 || echo "  [$label] non-zero exit"

    # GET phase is the second BENCH_RESULT line (ratio=0:0:0:1)
    line=$(grep "ratio=0:0:0:1" "$LOGFILE" | tail -1 || true)
    mops=$(echo "$line" | grep -oP '\(\K[0-9.]+(?= Mops/s)' || echo "N/A")
    nsop=$(echo "$line" | grep -oP 'ns_per_op=\K[0-9.]+' || echo "N/A")
    printf "%-12s %-14s %-14s\n" "$label" "$mops" "$nsop" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "Logs in $OUTDIR/" | tee -a "$SUMMARY"
