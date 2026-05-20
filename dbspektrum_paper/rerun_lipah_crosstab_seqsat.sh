#!/usr/bin/env bash
# Re-run only the LIPAH-sequential-sat cell of the 44t crosstab. Matches
# run_all_benchmarks.sh's run_crosstab_44t exactly: --no-page-fold, S=15,
# W=W_SAT, --refresh-hints, payload defaults.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

N=${N:-100000}
F=${F:-200000}
C=${C:-500}
T=${T:-44}
S=${S:-15}
W_SAT=${W_SAT:-5}
TRIALS=${TRIALS:-10}

BIN="$PROJECT_ROOT/target/release/bp_translation_bench_lipah"
[ -x "$BIN" ] || { echo "missing $BIN — build first"; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/lipah_crosstab_seqsat_${TS}"
RAW="$OUTDIR/raw"
mkdir -p "$RAW"

CSV="$OUTDIR/crosstab_seqsat_lipah.csv"
echo "variant,access,state,trial,mops,ns_per_op" > "$CSV"

unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE

extract_mops() { grep "^Throughput:" "$1" | grep -oP '\(\K[0-9.]+' | head -1 || echo "NaN"; }
extract_nsop() { grep "^Avg latency:" "$1" | grep -oP '[0-9.]+' | head -1 || echo "NaN"; }

echo "[$(date)] starting LIPAH crosstab seq×sat re-run, ${TRIALS} trials"
echo "  N=$N F=$F C=$C T=$T S=$S W=$W_SAT --no-page-fold --refresh-hints"

for trial in $(seq 1 "$TRIALS"); do
    log="$RAW/lipah_seqsat_t${trial}.log"
    "$BIN" --sequential -n "$N" -f "$F" -t "$T" -s "$S" -w "$W_SAT" \
        --num-containers "$C" --no-page-fold --refresh-hints \
        >"$log" 2>&1 || echo "  [t=$trial] non-zero exit"
    mops=$(extract_mops "$log")
    nsop=$(extract_nsop "$log")
    echo "LIPAH,sequential,sat,$trial,$mops,$nsop" >> "$CSV"
    printf "  t=%-2d  %s Mops/s  %s ns/op\n" "$trial" "$mops" "$nsop"
done

echo "[$(date)] done. csv: $CSV"
