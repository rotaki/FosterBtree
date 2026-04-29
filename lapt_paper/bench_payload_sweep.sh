#!/usr/bin/env bash
# Payload sweep: how does adding per-page work change the picture?
# Saturated config only (warmup=5s, --refresh-hints for LIPAH).
# Sweeps payload_bytes ∈ {0, 256, 1024, 4096, 16384}.
set -euo pipefail

# --- self-locating header (added when packaged into lapt_paper/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
# -------------------------------------------------------------------


N=${N:-100000}
F=${F:-200000}
T=${T:-12}
S=${S:-8}
W=${W:-5}
C=${C:-500}
BIN_SRC="bp_translation_bench"

unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/payload_sweep_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

declare -a VARIANTS=(
    "bp_clock|LIPAH-saturated|--refresh-hints"
    "bp_predicache|PrediCache|"
    "bp_pt_bucket_v2_congee|EnhancedPrediCache|"
    "bp_lapt|EnhancedPrediCache-OP|"
)

declare -a PAYLOADS=(0 1024 4096 8192 12288 16384)

echo "=== Payload sweep, sequential scan, saturated ===" | tee "$SUMMARY"
echo "Config: n=$N f=$F t=$T containers=$C warmup=${W}s exec=${S}s" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

# Build all variant binaries up-front
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

# Print header row
printf "%-22s" "variant \\ payload" | tee -a "$SUMMARY"
for p in "${PAYLOADS[@]}"; do
    printf " %10s" "${p}B" | tee -a "$SUMMARY"
done
echo "" | tee -a "$SUMMARY"

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label extra_args <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    BIN="$TARGET/${BIN_SRC}_${tag}"

    printf "%-22s" "$label" | tee -a "$SUMMARY"
    for p in "${PAYLOADS[@]}"; do
        LOGFILE="$OUTDIR/${tag}_p${p}.log"
        "$BIN" --sequential -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
            --num-containers "$C" --payload-bytes "$p" $extra_args \
            >"$LOGFILE" 2>&1 || true
        mops=$(grep "^Throughput:" "$LOGFILE" | grep -oP '\(\K[0-9.]+' || echo "N/A")
        printf " %10s" "$mops" | tee -a "$SUMMARY"
    done
    echo "" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "Logs in $OUTDIR/" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
