#!/usr/bin/env bash
# Quick collision benchmark for ophash variants only (Part B2)
set -euo pipefail

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="bench_ophash_collision_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

echo "=== ophash Collision Benchmark (Part B2) ===" | tee "$SUMMARY"
echo "Started: $(date)" | tee -a "$SUMMARY"

# Only ophash variants
declare -a VARIANTS=(
    "PT-V2-ophash|pt_v2_ophash"
    "PT-FP-V2-ophash|pt_fp_v2_ophash"
)

FRAMES=200000
COLLISION_PAGES=800000
THREADS=12
RUN_SECONDS=15
WARMUP=5
NUM_CONTAINERS=20  # Increased from 10 to create more collision density for k=8

echo "" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"
echo "Part B2: Collision coverage (frames=$FRAMES pages=$COLLISION_PAGES)" | tee -a "$SUMMARY"
echo "================================================================" | tee -a "$SUMMARY"

for k in 1 2 4 8; do
    echo "" | tee -a "$SUMMARY"
    echo "--- collision_width=${k} (with conflicts) ---" | tee -a "$SUMMARY"
    printf "%-22s %-14s %-14s\n" "variant" "coverage" "ops/s(Mops)" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r label tag <<< "$entry"
        log="$OUTDIR/partB2_k${k}_${tag}.log"

        # ophash variants need multiple containers to create collisions
        "$TARGET/pt_fastpath_coverage_${tag}" \
            --num-frames "$FRAMES" --num-pages "$COLLISION_PAGES" \
            --collision-width "$k" --hot-sets 256 \
            --threads "$THREADS" --seconds "$RUN_SECONDS" --warmup "$WARMUP" \
            --num-containers "$NUM_CONTAINERS" \
            >"$log" 2>&1 || true

        cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
        mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-14s %-14s\n" "$label" "${cov:-N/A}" "${mops:-N/A}" | tee -a "$SUMMARY"
    done

    # No-conflict control
    echo "--- collision_width=${k} (NO conflicts / control) ---" | tee -a "$SUMMARY"
    printf "%-22s %-14s %-14s\n" "variant" "coverage" "ops/s(Mops)" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r label tag <<< "$entry"
        log="$OUTDIR/partB2_k${k}_noconf_${tag}.log"

        # ophash variants need multiple containers even in no-conflict mode
        "$TARGET/pt_fastpath_coverage_${tag}" \
            --num-frames "$FRAMES" --num-pages "$COLLISION_PAGES" \
            --collision-width "$k" --hot-sets 256 \
            --threads "$THREADS" --seconds "$RUN_SECONDS" --warmup "$WARMUP" \
            --no-conflict --num-containers "$NUM_CONTAINERS" \
            >"$log" 2>&1 || true

        cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
        mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-14s %-14s\n" "$label" "${cov:-N/A}" "${mops:-N/A}" | tee -a "$SUMMARY"
    done
done

echo "" | tee -a "$SUMMARY"
echo "=== Done ===" | tee -a "$SUMMARY"
echo "Completed: $(date)" | tee -a "$SUMMARY"
echo "Results in $OUTDIR" | tee -a "$SUMMARY"
