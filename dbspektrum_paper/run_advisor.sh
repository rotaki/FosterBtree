#!/usr/bin/env bash
# Focused run for an advisor discussion: 4 variants (LIPAH, PrediCache,
# LAPT, PrediCache2) × 2 workloads (sequential payload sweep + 44-thread
# crosstab). Renders TWO plot sets from the same CSVs:
#   plots_3way/  — LIPAH, PrediCache*, LAPT (original translation micro)
#   plots_4way/  — LIPAH, PrediCache*, PrediCache2, LAPT (with placement variant)
#
# Skips both B-tree workloads and LAPT3 — those aren't part of the
# advisor narrative for this run.
#
# Knobs (env vars):
#   N=100000  F=200000  C=500
#   T=44  S=15  W_SAT=5  TRIALS=3  S_PAYLOAD=10
#   SKIP_BUILD=1   reuse existing tagged binaries
#   SKIP_PLOTS=1   skip plot generation
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
S_PAYLOAD=${S_PAYLOAD:-10}
TRIALS=${TRIALS:-3}
SKIP_BUILD=${SKIP_BUILD:-0}
SKIP_PLOTS=${SKIP_PLOTS:-0}

PAYLOADS=(0 256 1024 4096 16384)

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/advisor_${TIMESTAMP}"
RAW="$OUTDIR/raw"
CSV="$OUTDIR/csv"
PLOTS_3="$OUTDIR/plots_3way"
PLOTS_4="$OUTDIR/plots_4way"
mkdir -p "$RAW" "$CSV" "$PLOTS_3" "$PLOTS_4"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUTDIR/run.log"; }

# Variants: features|label. Order matters for the 4-way plot (left→right
# in grouped bars). No `pt_counts`/`pt_profile` — those would add atomic
# fetch_adds to the hot path.
declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_predicache|PrediCache"
    "bp_predicache2|PrediCache2"
    "bp_lapt|LAPT"
)

cat > "$OUTDIR/config.txt" <<EOF
date              : $(date)
host              : $(hostname)
N (pages)         : $N
F (frames)        : $F
C (containers)    : $C
T (threads)       : $T
S (exec sec)      : $S
S_PAYLOAD         : $S_PAYLOAD
W_SAT (warmup)    : $W_SAT
TRIALS            : $TRIALS
PAYLOADS          : ${PAYLOADS[*]}
VARIANTS          : LIPAH, PrediCache, PrediCache2, LAPT
EOF
log "Config written to $OUTDIR/config.txt"

# ----- build only bp_translation_bench (no btree binaries needed) -----
build_variant() {
    local features=$1 label=$2
    local tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    local bin="bp_translation_bench"
    if [ "$SKIP_BUILD" = "1" ] && [ -x "$TARGET/${bin}_${tag}" ]; then
        log "Skip build: $TARGET/${bin}_${tag} already exists"
        return
    fi
    log "Building $bin [$features] -> ${bin}_${tag}"
    cargo build --release --bin "$bin" --features "$features" 2>&1 | tail -1 \
        | tee -a "$OUTDIR/build.log"
    cp -f "$TARGET/$bin" "$TARGET/${bin}_${tag}"
}

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    build_variant "$features" "$label"
done

get_translation_throughput() {
    grep "^Throughput:" "$1" | grep -oP '\(\K[0-9.]+' | head -1 || echo "NaN"
}
get_translation_nsop() {
    grep "^Avg latency:" "$1" | grep -oP '[0-9.]+' | head -1 || echo "NaN"
}

# ----- workload 1: sequential payload sweep -----
run_payload_sweep() {
    local state=$1
    local out_csv="$CSV/seq_payload_${state}.csv"
    echo "variant,state,payload_bytes,trial,mops" > "$out_csv"

    log "=== Workload 1: sequential payload sweep ($state) ==="

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        local tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        local bin="$TARGET/bp_translation_bench_${tag}"

        for p in "${PAYLOADS[@]}"; do
            for trial in $(seq 1 "$TRIALS"); do
                local logf="$RAW/seq_payload_${state}_${tag}_p${p}_t${trial}.log"
                local extra=""
                local W=0
                if [ "$state" = "sat" ]; then
                    W=$W_SAT
                    if [ "$label" = "LIPAH" ]; then
                        extra="--refresh-hints"
                    fi
                    unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE
                else
                    extra="--scramble"
                    export PT_PROMOTE_PROB_NO_DEMOTE=4294967295
                    export PT_PROMOTE_PROB_DEMOTE=4294967295
                fi
                extra="$extra --callback-path"

                "$bin" --sequential -n "$N" -f "$F" -t "$T" -s "$S_PAYLOAD" -w "$W" \
                    --num-containers "$C" --payload-bytes "$p" $extra \
                    >"$logf" 2>&1 || log "  [$state/$label/p=$p/t=$trial] non-zero exit"
                local mops=$(get_translation_throughput "$logf")
                echo "$label,$state,$p,$trial,$mops" >> "$out_csv"
                printf "  %-12s %-6s p=%-5d t=%d  %s Mops/s\n" "$label" "$state" "$p" "$trial" "$mops"
            done
        done
    done
}

# ----- workload 2: 44-thread crosstab -----
run_crosstab_44t() {
    local out_csv="$CSV/crosstab_44t.csv"
    echo "variant,access,state,trial,mops,ns_per_op" > "$out_csv"

    log "=== Workload 2: 44-thread crosstab (sequential/uniform × sat/stale) ==="

    declare -a SCENARIOS=(
        "seq-sat|--sequential|sat"
        "seq-stale|--sequential|stale"
        "uniform-sat|--no-chain|sat"
        "uniform-stale|--no-chain|stale"
    )

    for sc in "${SCENARIOS[@]}"; do
        IFS='|' read -r sc_tag access state <<< "$sc"
        for entry in "${VARIANTS[@]}"; do
            IFS='|' read -r features label <<< "$entry"
            local tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
            local bin="$TARGET/bp_translation_bench_${tag}"

            for trial in $(seq 1 "$TRIALS"); do
                local logf="$RAW/crosstab_${sc_tag}_${tag}_t${trial}.log"
                local extra=""
                local W=0
                if [ "$state" = "sat" ]; then
                    W=$W_SAT
                    if [ "$label" = "LIPAH" ]; then extra="--refresh-hints"; fi
                    unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE
                else
                    extra="--scramble"
                    export PT_PROMOTE_PROB_NO_DEMOTE=4294967295
                    export PT_PROMOTE_PROB_DEMOTE=4294967295
                fi
                extra="$extra --callback-path"

                "$bin" $access -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
                    --num-containers "$C" --no-page-fold $extra \
                    >"$logf" 2>&1 || log "  [$sc_tag/$label/t=$trial] non-zero exit"
                local mops=$(get_translation_throughput "$logf")
                local nsop=$(get_translation_nsop "$logf")
                local access_kind=$([ "$access" = "--sequential" ] && echo "sequential" || echo "uniform")
                echo "$label,$access_kind,$state,$trial,$mops,$nsop" >> "$out_csv"
                printf "  %-12s %-15s t=%d  %s Mops/s  %s ns/op\n" "$label" "$sc_tag" "$trial" "$mops" "$nsop"
            done
        done
    done
}

run_payload_sweep "sat"
run_payload_sweep "stale"
run_crosstab_44t

# ----- plots: render two subsets from the same CSVs -----
if [ "$SKIP_PLOTS" != "1" ] && command -v python3 >/dev/null 2>&1; then
    log "=== Generating plots (3-way: LIPAH, PrediCache*, LAPT) ==="
    python3 "$SCRIPT_DIR/plot_results.py" \
        --indir "$CSV" --outdir "$PLOTS_3" \
        --variants "LIPAH,PrediCache*,LAPT" \
        2>&1 | tee -a "$OUTDIR/run.log" \
        || log "  3-way plot generation failed"

    log "=== Generating plots (4-way: LIPAH, PrediCache*, PrediCache2, LAPT) ==="
    python3 "$SCRIPT_DIR/plot_results.py" \
        --indir "$CSV" --outdir "$PLOTS_4" \
        --variants "LIPAH,PrediCache*,PrediCache2,LAPT" \
        2>&1 | tee -a "$OUTDIR/run.log" \
        || log "  4-way plot generation failed"
fi

log "=== Done ==="
log "Results:        $OUTDIR/"
log "  config        : $OUTDIR/config.txt"
log "  csvs          : $CSV/"
log "  3-way plots   : $PLOTS_3/   (seq_payload.pdf, crosstab_44t.pdf)"
log "  4-way plots   : $PLOTS_4/   (seq_payload.pdf, crosstab_44t.pdf)"
echo "$OUTDIR" > .last_advisor_run
