#!/usr/bin/env bash
# Master benchmark runner: builds every variant binary once, runs all four
# workloads with the parameters described in BENCHMARK_RESULTS.md, and emits
# CSVs + plots into bench_run_<timestamp>/.
#
# Knobs (env vars):
#   N=100000  F=200000  C=500
#   T=44  S=15  W_SAT=5  TRIALS=3
#   NUM_KEYS=2000000  KEY_SIZE=100  SCAN_SIZE=1000  EXEC_SECS=15
#   SKIP_BUILD=1   reuse existing variant binaries
#   SKIP_PLOTS=1   skip plot generation
set -euo pipefail

# --- self-locating header (added when packaged into lapt_paper/) ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
# -------------------------------------------------------------------


# ----- params -----
N=${N:-100000}
F=${F:-200000}
C=${C:-500}
T=${T:-44}
S=${S:-15}
W_SAT=${W_SAT:-5}
S_PAYLOAD=${S_PAYLOAD:-10}     # shorter exec for payload sweep (5×variants×payloads cells)
TRIALS=${TRIALS:-3}
TRIALS_BTREE_GET=${TRIALS_BTREE_GET:-5}
NUM_KEYS=${NUM_KEYS:-2000000}
KEY_SIZE=${KEY_SIZE:-100}
VAL_MIN=${VAL_MIN:-50}
VAL_MAX=${VAL_MAX:-100}
SCAN_SIZE=${SCAN_SIZE:-1000}
EXEC_SECS=${EXEC_SECS:-15}
SKIP_BUILD=${SKIP_BUILD:-0}
SKIP_PLOTS=${SKIP_PLOTS:-0}

PAYLOADS=(0 256 1024 4096 16384)

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/run_${TIMESTAMP}"
RAW="$OUTDIR/raw"
CSV="$OUTDIR/csv"
PLOTS="$OUTDIR/plots"
mkdir -p "$RAW" "$CSV" "$PLOTS"

log() { echo "[$(date +%H:%M:%S)] $*" | tee -a "$OUTDIR/run.log"; }

# Variants: features|label
declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_predicache|PrediCache"
    "bp_lapt|LAPT"
)

# ----- record config -----
cat > "$OUTDIR/config.txt" <<EOF
date              : $(date)
host              : $(hostname)
N (pages)         : $N
F (frames)        : $F
C (containers)    : $C
T (threads)        : $T

S (exec sec)      : $S
S_PAYLOAD         : $S_PAYLOAD
W_SAT (warmup)    : $W_SAT
TRIALS            : $TRIALS
TRIALS_BTREE_GET  : $TRIALS_BTREE_GET
NUM_KEYS          : $NUM_KEYS
KEY_SIZE          : $KEY_SIZE
VAL               : [$VAL_MIN,$VAL_MAX]
SCAN_SIZE         : $SCAN_SIZE
EXEC_SECS         : $EXEC_SECS
PAYLOADS          : ${PAYLOADS[*]}
EOF
log "Config written to $OUTDIR/config.txt"

# ----- build variant binaries -----
build_variant() {
    local features=$1 label=$2 bin=$3
    local tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
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
    for bin in bp_translation_bench fbt_on_disk_get fbt_on_disk_scan; do
        build_variant "$features" "$label" "$bin"
    done
done

# ----- helpers to extract metrics -----
get_translation_throughput() {
    grep "^Throughput:" "$1" | grep -oP '\(\K[0-9.]+' | head -1 || echo "NaN"
}
get_translation_nsop() {
    grep "^Avg latency:" "$1" | grep -oP '[0-9.]+' | head -1 || echo "NaN"
}
get_btree_get_mops() {
    grep "ratio=0:0:0:1" "$1" | tail -1 | grep -oP '\(\K[0-9.]+(?= Mops/s)' || echo "NaN"
}
get_scan_mkvs() {
    grep "BENCH_SCAN_RESULT" "$1" | tail -1 | grep -oP '\(\K[0-9.]+(?= M kvs/s)' || echo "NaN"
}

# ----- workload 1: sequential payload sweep -----
run_payload_sweep() {
    local state=$1                # "sat" or "stale"
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

    log "=== Workload 2: 44-thread crosstab (sequential×saturated, sequential×stale, uniform×saturated, uniform×stale) ==="

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

# ----- workload 3: B-tree GET -----
run_btree_get() {
    local out_csv="$CSV/btree_get.csv"
    echo "variant,trial,mops" > "$out_csv"

    log "=== Workload 3: B-tree random GET (T=$T, $NUM_KEYS keys, no-copy) ==="

    unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        local tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        local bin="$TARGET/fbt_on_disk_get_${tag}"

        for trial in $(seq 1 "$TRIALS_BTREE_GET"); do
            local logf="$RAW/btree_get_${tag}_t${trial}.log"
            "$bin" --num_keys "$NUM_KEYS" --key_size "$KEY_SIZE" \
                --val_min_size "$VAL_MIN" --val_max_size "$VAL_MAX" \
                --num_threads "$T" --bp_size "$F" --unique_keys \
                >"$logf" 2>&1 || log "  [btree-get/$label/t=$trial] non-zero exit"
            local mops=$(get_btree_get_mops "$logf")
            echo "$label,$trial,$mops" >> "$out_csv"
            printf "  %-12s t=%d  %s Mops/s\n" "$label" "$trial" "$mops"
        done
    done
}

# ----- workload 4: B-tree range scan -----
run_btree_scan() {
    local out_csv="$CSV/btree_range_scan.csv"
    echo "variant,trial,mkvs_per_s" > "$out_csv"

    log "=== Workload 4: B-tree random-start range scan (T=$T, scan_size=$SCAN_SIZE) ==="

    unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        local tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
        local bin="$TARGET/fbt_on_disk_scan_${tag}"

        for trial in $(seq 1 "$TRIALS"); do
            local logf="$RAW/btree_scan_${tag}_t${trial}.log"
            SCAN_SIZE="$SCAN_SIZE" EXEC_SECS="$EXEC_SECS" \
                "$bin" --num_keys "$NUM_KEYS" --key_size "$KEY_SIZE" \
                --val_min_size "$VAL_MIN" --val_max_size "$VAL_MAX" \
                --num_threads "$T" --bp_size "$F" --unique_keys \
                >"$logf" 2>&1 || log "  [btree-scan/$label/t=$trial] non-zero exit"
            local mkvs=$(get_scan_mkvs "$logf")
            echo "$label,$trial,$mkvs" >> "$out_csv"
            printf "  %-12s t=%d  %s M kvs/s\n" "$label" "$trial" "$mkvs"
        done
    done
}

# ----- run everything -----
run_payload_sweep "sat"
run_payload_sweep "stale"
run_crosstab_44t
run_btree_get
run_btree_scan

# ----- plots -----
if [ "$SKIP_PLOTS" != "1" ]; then
    if command -v python3 >/dev/null 2>&1; then
        log "=== Generating plots ==="
        python3 "$SCRIPT_DIR/plot_results.py" --indir "$CSV" --outdir "$PLOTS" 2>&1 | tee -a "$OUTDIR/run.log" || \
            log "  plot generation failed (check python deps: matplotlib, pandas)"
    else
        log "  python3 not found, skipping plots"
    fi
fi

log "=== Done ==="
log "Results: $OUTDIR/"
log "  config       : $OUTDIR/config.txt"
log "  raw logs     : $RAW/"
log "  csvs         : $CSV/"
log "  plots        : $PLOTS/"
echo "$OUTDIR" > .last_bench_run
