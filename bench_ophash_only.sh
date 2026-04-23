#!/usr/bin/env bash
# Quick benchmark for ophash variants only (after bug fix)
set -euo pipefail

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="bench_ophash_fix_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

echo "=== ophash-only Benchmark (Post-Fix Validation) ===" | tee "$SUMMARY"
echo "Started: $(date)" | tee -a "$SUMMARY"

# Only ophash variants
declare -a VARIANTS=(
    "bp_pt_v2_ophash,pt_counts|PT-V2-ophash|pt_v2_ophash"
    "bp_pt_bucket_v2_ophash,pt_counts|PT-FP-V2-ophash|pt_fp_v2_ophash"
)

build_variant() {
    local bin="$1"
    local features="$2"
    local tag="$3"
    echo "Building $bin with features: $features" | tee -a "$SUMMARY"
    cargo build --release --bin "$bin" --features "$features" 2>&1 | tail -1
    cp -f "$TARGET/$bin" "$TARGET/${bin}_${tag}"
}

# Build all binaries
echo "" | tee -a "$SUMMARY"
echo "--- Building binaries ---" | tee -a "$SUMMARY"
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label tag <<< "$entry"
    build_variant "bp_translation_bench" "$features" "$tag"
    build_variant "pt_fastpath_coverage" "$features" "$tag"
    build_variant "pt_phase_shift" "$features" "$tag"
done

# Part A: Throughput tests
PAGES=100000
FRAMES=200000
THREADS=12
RUN_SECONDS=15
WARMUP=5

echo "" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"
echo "Part A: Throughput (pages=$PAGES frames=$FRAMES threads=$THREADS)" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"

for pattern in sequential random hotspots; do
    echo "" | tee -a "$SUMMARY"
    if [ "$pattern" = "hotspots" ]; then
        echo "--- pattern=hotspots (4 hotspots x 50 pages, theta=0.99) ---" | tee -a "$SUMMARY"
    else
        echo "--- pattern=${pattern} ---" | tee -a "$SUMMARY"
    fi
    printf "%-22s %-14s %-14s %-14s\n" "variant" "ops/s(Mops)" "avg_ns_per_op" "coverage" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        log="$OUTDIR/partA_${pattern}_${tag}.log"
        extra=""

        if [ "$pattern" = "sequential" ]; then
            extra="--sequential"
        elif [ "$pattern" = "hotspots" ]; then
            extra="--hotspots 4 --hotspot-size 50 --hotspot-theta 0.99"
        fi

        "$TARGET/bp_translation_bench_${tag}" -n "$PAGES" -f "$FRAMES" -t "$THREADS" \
            -s "$RUN_SECONDS" -w "$WARMUP" $extra --no-frame-hint \
            >"$log" 2>&1 || true

        mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        avg=$(grep -oP 'Avg latency:\s*\K[0-9.]+' "$log" | head -1)
        cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-14s %-14s %-14s\n" "$label" "${mops:-N/A}" "${avg:-N/A}" "${cov:-N/A}" | tee -a "$SUMMARY"
    done
done

# Part B2: Collision tests
# For ophash with 10 containers, we need enough pages to create collision slots.
# With 200k frames and 800k pages across 10 containers (80k pages/container),
# we get sufficient collision density.
COLLISION_PAGES=800000

echo "" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"
echo "Part B2: Collision coverage (frames=$FRAMES pages=$COLLISION_PAGES hot_sets=256)" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"

for k in 1 2 4 8; do
    echo "" | tee -a "$SUMMARY"
    echo "--- collision_width=${k} (with conflicts) ---" | tee -a "$SUMMARY"
    printf "%-22s %-14s %-14s\n" "variant" "coverage" "ops/s(Mops)" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        log="$OUTDIR/partB2_k${k}_${tag}.log"

        # ophash variants need multiple containers to create collisions
        "$TARGET/pt_fastpath_coverage_${tag}" \
            --num-frames "$FRAMES" --num-pages "$COLLISION_PAGES" \
            --collision-width "$k" --hot-sets 256 \
            --threads "$THREADS" --seconds "$RUN_SECONDS" --warmup "$WARMUP" \
            --num-containers 10 \
            >"$log" 2>&1 || true

        cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
        mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-14s %-14s\n" "$label" "${cov:-N/A}" "${mops:-N/A}" | tee -a "$SUMMARY"
    done

    # No-conflict control
    echo "--- collision_width=${k} (NO conflicts / control) ---" | tee -a "$SUMMARY"
    printf "%-22s %-14s %-14s\n" "variant" "coverage" "ops/s(Mops)" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        log="$OUTDIR/partB2_k${k}_noconf_${tag}.log"

        # ophash variants need multiple containers even in no-conflict mode
        "$TARGET/pt_fastpath_coverage_${tag}" \
            --num-frames "$FRAMES" --num-pages "$COLLISION_PAGES" \
            --collision-width "$k" --hot-sets 256 \
            --threads "$THREADS" --seconds "$RUN_SECONDS" --warmup "$WARMUP" \
            --no-conflict --num-containers 10 \
            >"$log" 2>&1 || true

        cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
        mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-14s %-14s\n" "$label" "${cov:-N/A}" "${mops:-N/A}" | tee -a "$SUMMARY"
    done
done

# Part B3: Promotion probability sweep
echo "" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"
echo "Part B3: Promotion probability sweep at k=4" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"
printf "%-22s %-10s %-14s %-14s\n" "variant" "p=1/x" "coverage" "ops/s(Mops)" | tee -a "$SUMMARY"

for p in 1 8 64 512; do
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        log="$OUTDIR/partB3_p${p}_${tag}.log"

        # ophash variants need multiple containers to create collisions
        PT_PROMOTE_PROB_DEMOTE="$p" \
        PT_PROMOTE_PROB_NO_DEMOTE="$p" \
        "$TARGET/pt_fastpath_coverage_${tag}" \
            --num-frames "$FRAMES" --num-pages "$COLLISION_PAGES" \
            --collision-width 4 --hot-sets 256 \
            --threads "$THREADS" --seconds "$RUN_SECONDS" --warmup "$WARMUP" \
            --num-containers 10 \
            >"$log" 2>&1 || true

        cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
        mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-10s %-14s %-14s\n" "$label" "$p" "${cov:-N/A}" "${mops:-N/A}" | tee -a "$SUMMARY"
    done
done

# Part B4: Phase shift
echo "" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"
echo "Part B4: Phase-shift coverage trace" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label tag <<< "$entry"
    log="$OUTDIR/partB4_phase_${tag}.tsv"

    "$TARGET/pt_phase_shift_${tag}" \
        --num-pages 20000 --num-frames 5000 \
        --num-phases 4 --phase-hot-size 2000 \
        --phase-interval 5 --sample-interval-ms 500 \
        --threads "$THREADS" --warmup 3 \
        >"$log" 2>&1 || true

    echo "  $label -> $log" | tee -a "$SUMMARY"
done

echo "" | tee -a "$SUMMARY"
echo "=== Done ===" | tee -a "$SUMMARY"
echo "Completed: $(date)" | tee -a "$SUMMARY"
echo "Results in $OUTDIR" | tee -a "$SUMMARY"
