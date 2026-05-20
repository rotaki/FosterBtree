#!/usr/bin/env bash
# Bypass-vs-always-probe Pareto experiment.
#
# Sweeps `--prefer-prob F` (the fraction of accesses that target pages
# currently at their preferred frame) for two PrediCache variants:
#   - always : vanilla PrediCache (uniform placement, always probes OPM)
#   - bypass : PrediCache + pc_bypass (uniform placement, skips OPM on hit)
#
# Crosses with payload sizes {0, 512, 2048, 8192} to see how the
# bypass-vs-always crossover migrates with payload. Random access (the
# bench auto-disables chain mode when --prefer-prob is set), promotion
# locked via PT_PROMOTE_PROB_* env vars set inside the bench.
#
# Writes CSV: dbspektrum_paper/results/prefer_<ts>/sweep.csv
#   columns: variant,payload,prefer_prob,observed_hit_rate,mops
# Plots a 2x2 grid (one panel per payload) into the same dir.
#
# Env knobs:
#   N=100000 F=200000 C=500 T=44 S=10 W=10  TRIALS=1
#   SKIP_BUILD=1   reuse existing tagged binaries
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

N=${N:-100000}
F=${F:-200000}
C=${C:-500}
T=${T:-44}
S=${S:-10}
W=${W:-10}
TRIALS=${TRIALS:-1}
SKIP_BUILD=${SKIP_BUILD:-0}

# 9 F-values, dense on the high end where the crossover is expected.
PREFER_PROBS=(0.0 0.25 0.5 0.75 0.85 0.90 0.95 0.98 1.0)
PAYLOADS=(0 512 2048 8192)

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/prefer_${TIMESTAMP}"
RAW="$OUTDIR/raw"
mkdir -p "$RAW"
CSV="$OUTDIR/sweep.csv"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUTDIR/run.log"; }

cat > "$OUTDIR/config.txt" <<EOF
date          : $(date)
host          : $(hostname)
N             : $N
F             : $F
C             : $C
T             : $T
S             : $S
W             : $W
TRIALS        : $TRIALS
PREFER_PROBS  : ${PREFER_PROBS[*]}
PAYLOADS      : ${PAYLOADS[*]}
EOF
log "config -> $OUTDIR/config.txt"

# variants: feature_str | label | binary tag
declare -a VARIANTS=(
    "bp_predicache|always|always"
    "bp_predicache,pc_bypass|bypass|bypass"
)

build() {
    local features=$1 tag=$2
    local bin="$TARGET/bp_translation_bench_${tag}"
    if [ "$SKIP_BUILD" = "1" ] && [ -x "$bin" ]; then
        log "skip build: $bin"
        return
    fi
    log "build $tag [$features]"
    cargo build --release --bin bp_translation_bench --features "$features" 2>&1 | tail -1
    cp -f "$TARGET/bp_translation_bench" "$bin"
}

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features _label tag <<< "$entry"
    build "$features" "$tag"
done

echo "variant,payload,prefer_prob,observed_hit_rate,mops" > "$CSV"

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r _features label tag <<< "$entry"
    bin="$TARGET/bp_translation_bench_${tag}"
    for p in "${PAYLOADS[@]}"; do
        for pp in "${PREFER_PROBS[@]}"; do
            for trial in $(seq 1 "$TRIALS"); do
                logf="$RAW/${tag}_p${p}_pp${pp}_t${trial}.log"
                "$bin" -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
                    --num-containers "$C" \
                    --payload-bytes "$p" \
                    --callback-path \
                    --prefer-prob "$pp" \
                    > "$logf" 2>&1 || log "  [$label/p=$p/pp=$pp/t=$trial] non-zero exit"
                mops=$(grep "^Throughput:" "$logf" | grep -oP '\(\K[0-9.]+' | head -1)
                # The classification line prints natural hit_rate, but we
                # also want the *observed* hit rate during the timed window.
                # Approximate via the classification rate (close enough since
                # promotion is locked).
                hit=$(grep "natural hit_rate" "$logf" | grep -oP 'hit_rate = \K[0-9.]+' | head -1)
                echo "$label,$p,$pp,${hit:-NaN},${mops:-NaN}" >> "$CSV"
                printf "  %-7s p=%-5d pp=%-5s hit=%s mops=%s\n" "$label" "$p" "$pp" "$hit" "$mops"
            done
        done
    done
done

log "=== plotting ==="
python3 "$SCRIPT_DIR/plot_prefer_sweep.py" --csv "$CSV" --outdir "$OUTDIR" \
    2>&1 | tee -a "$OUTDIR/run.log" \
    || log "  plot failed (check matplotlib/pandas)"

log "=== Done ==="
log "results: $OUTDIR/"
echo "$OUTDIR" > .last_prefer_run
