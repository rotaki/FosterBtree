#!/usr/bin/env bash
# Re-run only LIPAH-preferred sequential payload sweep (sat state w/ --refresh-hints).
# Uses the already-built bp_translation_bench_lipah binary. Designed to
# characterize the high run-to-run variance seen in the full bench.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Match defaults from run_all_benchmarks.sh
N=${N:-100000}
F=${F:-200000}
C=${C:-500}
T=${T:-44}
S_PAYLOAD=${S_PAYLOAD:-10}
W_SAT=${W_SAT:-5}
TRIALS=${TRIALS:-10}
PAYLOADS=(0 256 1024 4096 16384)

BIN="$PROJECT_ROOT/target/release/bp_translation_bench_lipah"
[ -x "$BIN" ] || { echo "missing $BIN — build first"; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/lipah_seq_sat_${TS}"
RAW="$OUTDIR/raw"
mkdir -p "$RAW"

CSV="$OUTDIR/seq_payload_sat_lipah.csv"
echo "variant,state,payload_bytes,trial,mops,ns_per_op" > "$CSV"

unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE

extract_mops() { grep "^Throughput:" "$1" | grep -oP '\(\K[0-9.]+' | head -1 || echo "NaN"; }
extract_nsop() { grep "^Avg latency:" "$1" | grep -oP '[0-9.]+' | head -1 || echo "NaN"; }

echo "[$(date)] starting LIPAH-preferred seq sat re-run, ${TRIALS} trials per payload"
echo "  N=$N F=$F C=$C T=$T S=$S_PAYLOAD W=$W_SAT"

for p in "${PAYLOADS[@]}"; do
    for trial in $(seq 1 "$TRIALS"); do
        log="$RAW/lipah_p${p}_t${trial}.log"
        "$BIN" --sequential -n "$N" -f "$F" -t "$T" -s "$S_PAYLOAD" -w "$W_SAT" \
            --num-containers "$C" --payload-bytes "$p" --refresh-hints \
            >"$log" 2>&1 || echo "  [p=$p t=$trial] non-zero exit"
        mops=$(extract_mops "$log")
        nsop=$(extract_nsop "$log")
        echo "LIPAH,sat,$p,$trial,$mops,$nsop" >> "$CSV"
        printf "  p=%-5d t=%-2d  %s Mops/s  %s ns/op\n" "$p" "$trial" "$mops" "$nsop"
    done
done

echo "[$(date)] done. csv: $CSV"
