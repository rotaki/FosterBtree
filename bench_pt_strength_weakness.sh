#!/usr/bin/env bash
# Strength / weakness study for PT and PT(FP).
#
# Part A (strength) — show that PT's scan weakness is a hash-layout artifact:
#   sequentially chase a chain across all six variants and compare throughput.
#
# Part B (weakness) — demonstrate PT(FP)'s fast-path scarcity and adaptation
#   lag via: (B1) coverage counters in bp_translation_bench (pt_counts feature),
#   (B2) `pt_fastpath_coverage` binary w/ collision groups + no-conflict control,
#   (B3) promotion-probability sweep, (B4) phase-shift coverage trace.
#
# Usage:
#   ./bench_pt_strength_weakness.sh [part_a|part_b|all]   # default: all
#
# Each sub-run builds the binary into a suffixed name so that variants can
# coexist without rebuilding, mirroring bench_v2_comparison.sh.
set -euo pipefail

PART="${1:-all}"
TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="bench_pt_strength_weakness_${TIMESTAMP}"
mkdir -p "$OUTDIR"
SUMMARY="$OUTDIR/summary.txt"

# Variants we benchmark. Kept in one table so Part A and Part B stay in sync.
# features|label|tag
declare -a VARIANTS=(
    "bp_clock_v2,pt_counts|LIPAH-V2|lipah_v2"
    "bp_tlb_v2,pt_counts|TLB-V2|tlb_v2"
    "bp_pt_v2,pt_counts|PT-V2|pt_v2"
    "bp_pt_v2_ophash,pt_counts|PT-V2-ophash|pt_v2_ophash"
    "bp_pt_bucket_v2,pt_counts|PT-FP-V2|pt_fp_v2"
    "bp_pt_bucket_v2_ophash,pt_counts|PT-FP-V2-ophash|pt_fp_v2_ophash"
)

build_variants() {
    local bin="$1"
    echo "--- Building ${bin} across ${#VARIANTS[@]} variants ---" | tee -a "$SUMMARY"
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        cargo build --release --bin "$bin" --features "$features" 2>&1 | tail -1
        cp -f "$TARGET/$bin" "$TARGET/${bin}_${tag}"
    done
}

# ---------------- Part A: scan / random / hotspot ----------------------------
run_part_a() {
    local BIN="bp_translation_bench"
    build_variants "$BIN"

    # All three patterns share the same BP size + threads so lines are directly
    # comparable across variants within a pattern.
    local PAGES=100000
    local FRAMES=200000
    local THREADS=12
    local SECONDS=15
    local WARMUP=5

    echo "" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    echo "Part A: chain-traversal throughput (pages=$PAGES frames=$FRAMES threads=$THREADS)" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"

    for pattern in sequential random; do
        echo "" | tee -a "$SUMMARY"
        echo "--- pattern=${pattern} ---" | tee -a "$SUMMARY"
        printf "%-22s %-14s %-14s\n" "variant" "ops/s(Mops)" "avg_ns_per_op" | tee -a "$SUMMARY"
        for entry in "${VARIANTS[@]}"; do
            IFS='|' read -r features label tag <<< "$entry"
            local log="$OUTDIR/partA_${pattern}_${tag}.log"
            local extra=""
            [[ "$pattern" == "sequential" ]] && extra="--sequential"
            "$TARGET/${BIN}_${tag}" -n "$PAGES" -f "$FRAMES" -t "$THREADS" \
                -s "$SECONDS" -w "$WARMUP" $extra --no-frame-hint \
                >"$log" 2>&1 || true
            local mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
            local avg=$(grep -oP 'Avg latency:\s*\K[0-9.]+' "$log" | head -1)
            printf "%-22s %-14s %-14s\n" "$label" "${mops:-N/A}" "${avg:-N/A}" | tee -a "$SUMMARY"
        done
    done

    # Multi-hotspot (skew test)
    echo "" | tee -a "$SUMMARY"
    echo "--- pattern=hotspots (4 hotspots x 50 pages, theta=0.99) ---" | tee -a "$SUMMARY"
    printf "%-22s %-14s %-14s\n" "variant" "ops/s(Mops)" "avg_ns_per_op" | tee -a "$SUMMARY"
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        local log="$OUTDIR/partA_hotspots_${tag}.log"
        "$TARGET/${BIN}_${tag}" -n "$PAGES" -f "$FRAMES" -t "$THREADS" \
            -s "$SECONDS" -w "$WARMUP" \
            --hotspots 4 --hotspot-size 50 --hotspot-theta 0.99 \
            --no-frame-hint \
            >"$log" 2>&1 || true
        local mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
        local avg=$(grep -oP 'Avg latency:\s*\K[0-9.]+' "$log" | head -1)
        printf "%-22s %-14s %-14s\n" "$label" "${mops:-N/A}" "${avg:-N/A}" | tee -a "$SUMMARY"
    done
}

# ---------------- Part B: fast-path coverage / scarcity ----------------------
run_part_b() {
    local BIN_BENCH="bp_translation_bench"
    local BIN_COV="pt_fastpath_coverage"
    build_variants "$BIN_BENCH"
    build_variants "$BIN_COV"

    local FRAMES=200000
    local HOT_SETS=256
    local THREADS=12
    local SECONDS=15
    local WARMUP=5

    # B2: collision-width sweep at p=1/512 (default).
    echo "" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    echo "Part B2: fast-path coverage vs collision width (frames=$FRAMES hot_sets=$HOT_SETS)" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    for k in 1 2 4 8; do
        echo "" | tee -a "$SUMMARY"
        echo "--- collision_width=${k} (with conflicts) ---" | tee -a "$SUMMARY"
        printf "%-22s %-14s %-14s %-14s\n" "variant" "coverage" "ops/s(Mops)" "p99_ns" | tee -a "$SUMMARY"
        for entry in "${VARIANTS[@]}"; do
            IFS='|' read -r features label tag <<< "$entry"
            local log="$OUTDIR/partB2_k${k}_${tag}.log"
            "$TARGET/${BIN_COV}_${tag}" \
                --num-frames "$FRAMES" --collision-width "$k" \
                --hot-sets "$HOT_SETS" --threads "$THREADS" \
                --seconds "$SECONDS" --warmup "$WARMUP" \
                >"$log" 2>&1 || true
            local cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
            local mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
            local p99=$(grep -oP 'p99\s*:\s*\K[0-9.]+' "$log" | head -1)
            printf "%-22s %-14s %-14s %-14s\n" "$label" "${cov:-N/A}" "${mops:-N/A}" "${p99:-N/A}" | tee -a "$SUMMARY"
        done

        # No-conflict control run.
        echo "--- collision_width=${k} (NO conflicts / control) ---" | tee -a "$SUMMARY"
        printf "%-22s %-14s %-14s %-14s\n" "variant" "coverage" "ops/s(Mops)" "p99_ns" | tee -a "$SUMMARY"
        for entry in "${VARIANTS[@]}"; do
            IFS='|' read -r features label tag <<< "$entry"
            local log="$OUTDIR/partB2_k${k}_noconf_${tag}.log"
            "$TARGET/${BIN_COV}_${tag}" \
                --num-frames "$FRAMES" --collision-width "$k" \
                --hot-sets "$HOT_SETS" --threads "$THREADS" \
                --seconds "$SECONDS" --warmup "$WARMUP" \
                --no-conflict \
                >"$log" 2>&1 || true
            local cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
            local mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
            local p99=$(grep -oP 'p99\s*:\s*\K[0-9.]+' "$log" | head -1)
            printf "%-22s %-14s %-14s %-14s\n" "$label" "${cov:-N/A}" "${mops:-N/A}" "${p99:-N/A}" | tee -a "$SUMMARY"
        done
    done

    # B3: promotion-probability sweep at fixed collision width=4.
    echo "" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    echo "Part B3: PT(FP) promotion-probability sweep at k=4" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    printf "%-22s %-10s %-14s %-14s\n" "variant" "p=1/x" "coverage" "ops/s(Mops)" | tee -a "$SUMMARY"
    for p in 1 8 64 512; do
        for entry in "${VARIANTS[@]}"; do
            IFS='|' read -r features label tag <<< "$entry"
            # Only the PT-FP variants honor promotion probability; skip others
            # to keep the table compact. The env var is a no-op on non-PT BPs.
            case "$tag" in pt_fp_v2|pt_fp_v2_ophash|pt_v2|pt_v2_ophash) ;; *) continue ;; esac
            local log="$OUTDIR/partB3_p${p}_${tag}.log"
            PT_PROMOTE_PROB_DEMOTE="$p" \
            PT_PROMOTE_PROB_NO_DEMOTE="$p" \
            "$TARGET/${BIN_COV}_${tag}" \
                --num-frames "$FRAMES" --collision-width 4 \
                --hot-sets "$HOT_SETS" --threads "$THREADS" \
                --seconds "$SECONDS" --warmup "$WARMUP" \
                >"$log" 2>&1 || true
            local cov=$(grep -oP 'fast_path_coverage:\s*\K[0-9.]+' "$log" | head -1)
            local mops=$(grep -oP 'Throughput:.*?\(\K[0-9.]+' "$log" | head -1)
            printf "%-22s %-10s %-14s %-14s\n" "$label" "$p" "${cov:-N/A}" "${mops:-N/A}" | tee -a "$SUMMARY"
        done
    done

    # B4: phase-shift coverage trace (pt_phase_shift bin; TSV per-window).
    local BIN_PHASE="pt_phase_shift"
    build_variants "$BIN_PHASE"
    echo "" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    echo "Part B4: phase-shift coverage trace (pt_phase_shift TSV per sample window)" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"
    # hot_size tuned so ~40% of frames are "in play" per phase — this is the
    # regime where PT(FP)'s preferred-slot scarcity shows up as a coverage dip
    # at phase boundaries.
    local P_PAGES=20000
    local P_FRAMES=5000
    local P_HOT=2000
    local P_NUM=4
    local P_INT=5
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label tag <<< "$entry"
        case "$tag" in pt_fp_v2|pt_fp_v2_ophash|tlb_v2|lipah_v2|pt_v2|pt_v2_ophash) ;; *) continue ;; esac
        local log="$OUTDIR/partB4_phase_${tag}.tsv"
        "$TARGET/${BIN_PHASE}_${tag}" \
            --num-pages "$P_PAGES" --num-frames "$P_FRAMES" \
            --num-phases "$P_NUM" --phase-hot-size "$P_HOT" \
            --phase-interval "$P_INT" --sample-interval-ms 500 \
            --threads "$THREADS" --warmup 3 \
            >"$log" 2>&1 || true
        echo "  $label -> $log" | tee -a "$SUMMARY"
    done
}

case "$PART" in
    part_a) run_part_a ;;
    part_b) run_part_b ;;
    all)    run_part_a; run_part_b ;;
    *) echo "Usage: $0 [part_a|part_b|all]"; exit 2 ;;
esac

echo "" | tee -a "$SUMMARY"
echo "=== Done. Results in $OUTDIR ===" | tee -a "$SUMMARY"
