#!/usr/bin/env bash
# Fast end-to-end sanity for bench_pt_strength_weakness + plotter.
# Runs only 3 variants with tiny benchmark parameters so the whole pipeline
# completes in < 1 minute. Produces a directory the plotter can consume.
set -euo pipefail

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="bench_pt_strength_weakness_quick_${TIMESTAMP}"
mkdir -p "$OUTDIR"
TARGET="./target/release"

declare -a V=(
    "bp_tlb_v2,pt_counts|TLB-V2|tlb_v2"
    "bp_clock_v2,pt_counts|LIPAH-V2|lipah_v2"
    "bp_pt_bucket_v2,pt_counts|PT-FP-V2|pt_fp_v2"
)

for entry in "${V[@]}"; do
    IFS='|' read -r features label tag <<< "$entry"
    echo "Building ${label} for pt_fastpath_coverage"
    cargo build --release --bin pt_fastpath_coverage --features "$features" 2>&1 | tail -1
    cp -f "$TARGET/pt_fastpath_coverage" "$TARGET/pt_fastpath_coverage_${tag}"
    echo "Building ${label} for pt_phase_shift"
    cargo build --release --bin pt_phase_shift --features "$features" 2>&1 | tail -1
    cp -f "$TARGET/pt_phase_shift" "$TARGET/pt_phase_shift_${tag}"
done

# B2: collision sweep
for k in 1 2 4 8; do
    for entry in "${V[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        log="$OUTDIR/partB2_k${k}_${tag}.log"
        "$TARGET/pt_fastpath_coverage_${tag}" \
            --num-frames 10000 --num-pages 40000 \
            --collision-width "$k" --hot-sets 32 \
            --threads 4 --seconds 3 --warmup 1 \
            >"$log" 2>&1
    done
done

# B4: phase-shift
for entry in "${V[@]}"; do
    IFS='|' read -r features label tag <<< "$entry"
    log="$OUTDIR/partB4_phase_${tag}.tsv"
    "$TARGET/pt_phase_shift_${tag}" \
        --num-pages 20000 --num-frames 5000 \
        --num-phases 3 --phase-hot-size 2000 \
        --phase-interval 4 --sample-interval-ms 500 \
        --threads 4 --warmup 2 \
        >"$log" 2>&1
done

echo "Quick bench done: $OUTDIR"
echo "Run: python3 scripts/plot_pt_strength_weakness.py $OUTDIR"
