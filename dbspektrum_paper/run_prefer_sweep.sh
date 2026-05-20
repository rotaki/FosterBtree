#!/usr/bin/env bash
# Bypass-vs-always-probe Pareto experiment, demonstrating the FRAMING.md
# hypothesis that the bypass mechanism's win shrinks as payload grows.
#
# Sweeps `--prefer-prob F` (the fraction of accesses that target pages
# currently at their preferred frame) across payload sizes, for two
# PrediCache configurations:
#   - always : vanilla PrediCache (uniform placement, always probes OPM)
#   - bypass : PrediCache + pc_bypass (uniform placement, skips OPM on hit)
#
# Expected shape (one panel per payload, x = prefer_prob, y = throughput):
#   - Small payload: bypass has steep positive slope; always is flat;
#                    clear crossover near pp ≈ 0.2.
#   - Medium payload: same shape, smaller spread between lines.
#   - Large payload: lines converge — lookup cost is small relative to
#                    payload-load bandwidth so saving it doesn't show.
#
# All cells use the same 10k+10k balanced working set (via --prefer-cap)
# so cache footprint stays comparable across F. Promotion is locked off
# before classification (via the runtime setter inside the bench), so the
# preferred / displaced sets don't drift during the timed window.
#
# Outputs: dbspektrum_paper/results/prefer_<ts>/{sweep.csv, run.log,
# prefer_sweep.{png,pdf}}.
#
# Env knobs (with current defaults):
#   N=100000 F=200000 C=500 T=44 S=10 W=10  TRIALS=3  PREFER_CAP=10000
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
TRIALS=${TRIALS:-3}
PREFER_CAP=${PREFER_CAP:-10000}
SKIP_BUILD=${SKIP_BUILD:-0}

# 6 F-values, denser near the top where the crossover lives at small payload.
PREFER_PROBS=(0.0 0.25 0.5 0.75 0.9 1.0)
# 7 payload sizes from "translation-only" up through "full page" (16 KiB =
# default page size). The bench now respects --payload-bytes on the random-
# access path (was a bug, fixed), so 1024 actually reads 1024 bytes, not the
# full 16 KiB page; 16384 explicitly tests the full-page case.
PAYLOADS=(0 128 512 2048 4096 8192 16384)

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
PREFER_CAP    : $PREFER_CAP
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

echo "variant,payload,prefer_prob,natural_hit_rate,mops" > "$CSV"

n_cells=$(( ${#VARIANTS[@]} * ${#PAYLOADS[@]} * ${#PREFER_PROBS[@]} * TRIALS ))
log "running $n_cells cells (~$(( n_cells * (S + W) / 60 )) min at ${S}s+${W}s each)"

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
                    --prefer-cap "$PREFER_CAP" \
                    > "$logf" 2>&1 || log "  [$label/p=$p/pp=$pp/t=$trial] non-zero exit"
                mops=$(grep "^Throughput:" "$logf" | grep -oP '\(\K[0-9.]+' | head -1)
                # Natural hit rate from classification — independent of pp;
                # included for sanity (should be ~0.78 every time).
                hit=$(grep "natural_hit_rate" "$logf" | grep -oP 'natural_hit_rate = \K[0-9.]+' | head -1)
                echo "$label,$p,$pp,${hit:-NaN},${mops:-NaN}" >> "$CSV"
                printf "  %-7s p=%-5d pp=%-5s hit=%s  mops=%s\n" "$label" "$p" "$pp" "$hit" "$mops"
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

# ----- email -----
EMAIL="${EMAIL:-rotaki@uchicago.edu}"
if [ "${SKIP_EMAIL:-0}" != "1" ]; then
    log "=== emailing $EMAIL ==="
    SUBJECT="Prefer-prob sweep done on $(hostname)"
    BODY=$(mktemp)
    {
        echo "Prefer-prob (bypass vs always-probe) sweep done at $(date)."
        echo "Host: $(hostname -f)"
        echo "Results: $OUTDIR"
        echo
        if [ -f "$OUTDIR/config.txt" ]; then
            echo "=== config ==="
            cat "$OUTDIR/config.txt"
            echo
        fi
        echo "=== run log (last 80 lines) ==="
        tail -80 "$OUTDIR/run.log"
    } > "$BODY"
    ATTACHMENTS=()
    for f in prefer_sweep.png prefer_sweep.pdf sweep.csv; do
        p="$OUTDIR/$f"
        [ -f "$p" ] && ATTACHMENTS+=("$p")
    done
    python3 "$SCRIPT_DIR/_send_email.py" "$EMAIL" "$SUBJECT" "$BODY" "${ATTACHMENTS[@]}"
    log "  mail exit: $?"
    rm -f "$BODY"
fi
