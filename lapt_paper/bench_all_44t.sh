#!/usr/bin/env bash
# Combined 44-thread sweep: 2 access patterns × 2 prediction states × 3 BPs.
#   - sequential vs uniform random
#   - saturated (warmup + refresh-hints + promotion) vs stale (scramble + no warmup + no promotion)
#   - LIPAH | PrediCache (PT-V2) | LAPT (PT-FP-V2 + congee + ophash)
set -euo pipefail

# --- self-locating header (added when packaged into lapt_paper/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
# -------------------------------------------------------------------


N=${N:-100000}
F=${F:-200000}
T=${T:-44}
S=${S:-15}
W_SAT=${W_SAT:-5}
C=${C:-500}
BIN_SRC="bp_translation_bench"

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/all_44t_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

# variants: features|label
declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_predicache|PrediCache"
    "bp_lapt|LAPT"
)

# scenarios: tag|access_flag|state|lipah_extra
#   access_flag = "--sequential" or "--no-chain"
#   state       = "saturated" or "stale" (controls warmup, scramble, env vars)
#   lipah_extra = "--refresh-hints" for saturated, empty for stale
declare -a SCENARIOS=(
    "seq-sat|--sequential|saturated|--refresh-hints"
    "seq-stale|--sequential|stale|"
    "uniform-sat|--no-chain|saturated|--refresh-hints"
    "uniform-stale|--no-chain|stale|"
)

echo "=== 44-thread combined sweep ===" | tee "$SUMMARY"
echo "Config: n=$N f=$F t=$T containers=$C exec=${S}s saturated_warmup=${W_SAT}s" | tee -a "$SUMMARY"
echo "Date: $(date)" | tee -a "$SUMMARY"
echo "Host: $(hostname)" | tee -a "$SUMMARY"
echo "" | tee -a "$SUMMARY"

# Build variants once.
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

# Run each (variant, scenario) cell.
declare -A RESULT_MOPS
declare -A RESULT_NSOP
for sc_entry in "${SCENARIOS[@]}"; do
    IFS='|' read -r sc_tag access_flag state lipah_extra <<< "$sc_entry"
    echo "--- $sc_tag ---" | tee -a "$SUMMARY"

    for v_entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$v_entry"
        tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        BIN="$TARGET/${BIN_SRC}_${tag}"

        # Per-scenario flags
        if [ "$state" = "saturated" ]; then
            W=$W_SAT
            extra_args=""
            if [ "$label" = "LIPAH" ]; then
                extra_args="$lipah_extra"
            fi
            unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE
        else
            W=0
            extra_args="--scramble"
            export PT_PROMOTE_PROB_NO_DEMOTE=4294967295
            export PT_PROMOTE_PROB_DEMOTE=4294967295
        fi

        LOGFILE="$OUTDIR/${sc_tag}_${tag}.log"
        "$BIN" $access_flag -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
            --num-containers "$C" --no-page-fold $extra_args \
            >"$LOGFILE" 2>&1 || echo "  [$sc_tag/$label] non-zero exit"

        mops=$(grep "^Throughput:" "$LOGFILE" | grep -oP '\(\K[0-9.]+' || echo "N/A")
        nsop=$(grep "^Avg latency:" "$LOGFILE" | grep -oP '[0-9.]+' | head -1 || echo "N/A")
        RESULT_MOPS["$sc_tag|$label"]="$mops"
        RESULT_NSOP["$sc_tag|$label"]="$nsop"
        printf "  %-12s %8s Mops/s   %8s ns/op\n" "$label" "$mops" "$nsop" | tee -a "$SUMMARY"
    done
done

# Final crosstab.
echo "" | tee -a "$SUMMARY"
echo "=== Throughput crosstab (Mops/s) ===" | tee -a "$SUMMARY"
printf "%-14s" "variant" | tee -a "$SUMMARY"
for sc_entry in "${SCENARIOS[@]}"; do
    IFS='|' read -r sc_tag _ _ _ <<< "$sc_entry"
    printf " %14s" "$sc_tag" | tee -a "$SUMMARY"
done
echo "" | tee -a "$SUMMARY"
for v_entry in "${VARIANTS[@]}"; do
    IFS='|' read -r _ label <<< "$v_entry"
    printf "%-14s" "$label" | tee -a "$SUMMARY"
    for sc_entry in "${SCENARIOS[@]}"; do
        IFS='|' read -r sc_tag _ _ _ <<< "$sc_entry"
        printf " %14s" "${RESULT_MOPS[$sc_tag|$label]}" | tee -a "$SUMMARY"
    done
    echo "" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "Logs in $OUTDIR/" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
