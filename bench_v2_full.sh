#!/usr/bin/env bash
# Head-to-head comparison of the three FrameManager-based V2 buffer pools
# across four workloads.
#
#   Variants (V2 only):
#     - bp_clock_v2      (LIPAH-V2)   — clock + DashMap translator
#     - bp_pt_bucket_v2  (PT-V2)      — predictive translation + overflow table (FP-1 wrapper)
#     - bp_tlb_v2        (TLB-V2)     — standalone TLB + congee overflow
#
#   Workloads:
#     1. TPC-C 40w / 40t / in-memory           (no pressure; translation-cost isolated)
#     2. TPC-C 10w / 10t / 1 GB BP             (memory-pressured; eviction on the hot path)
#     3. Sequential scan (100k pages, 80k frames, 40 threads)
#     4. Uniform random access (100k pages, 80k frames, 40 threads)
#
# Env overrides:
#   CORES=<N>          (default: 40)           used for TPC-C 40/40 if your box has <40 cores
#   WARMUP_TPCC=<s>    (default: 5)
#   EXEC_TPCC=<s>      (default: 20)
#   SCAN_PAGES=<N>     (default: 100000)
#   SCAN_FRAMES=<N>    (default: 80000)
#   SCAN_THREADS=<N>   (default: 40)
#   SCAN_SECONDS=<s>   (default: 15)
#   WARMUP_SCAN=<s>    (default: 3)
#   SKIP_BUILD=1       reuse existing binaries (no rebuild)
#
# Outputs:
#   bench_v2_full_results/summary_<timestamp>.txt
#   bench_v2_full_results/<scenario>_<variant>_<timestamp>.log
set -euo pipefail

TPCC_BIN="tpcc_profile_neworder"
TRANS_BIN="bp_translation_bench"
TARGET="./target/release"
OUTDIR="bench_v2_full_results"
mkdir -p "$OUTDIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
SUMMARY="$OUTDIR/summary_${TIMESTAMP}.txt"

CORES="${CORES:-40}"
WARMUP_TPCC="${WARMUP_TPCC:-5}"
EXEC_TPCC="${EXEC_TPCC:-20}"
SCAN_PAGES="${SCAN_PAGES:-100000}"
SCAN_FRAMES="${SCAN_FRAMES:-80000}"
SCAN_THREADS="${SCAN_THREADS:-40}"
SCAN_SECONDS="${SCAN_SECONDS:-15}"
WARMUP_SCAN="${WARMUP_SCAN:-3}"
SKIP_BUILD="${SKIP_BUILD:-0}"

declare -a VARIANTS=(
    "bp_clock_v2|LIPAH-V2"
    "bp_pt_bucket_v2|PT-V2"
    "bp_tlb_v2|TLB-V2"
)

# label|warehouses|threads|bpGB (0 = 1GB/warehouse = in-memory)
declare -a TPCC_SCENARIOS=(
    "tpcc_inmem_40w40t|40|${CORES}|0"
    "tpcc_1gb_10w10t|10|10|1"
)

# label|sequential(1/0)|pages|frames|threads|seconds|warmup
declare -a SCAN_SCENARIOS=(
    "seq_scan|1|${SCAN_PAGES}|${SCAN_FRAMES}|${SCAN_THREADS}|${SCAN_SECONDS}|${WARMUP_SCAN}"
    "uniform|0|${SCAN_PAGES}|${SCAN_FRAMES}|${SCAN_THREADS}|${SCAN_SECONDS}|${WARMUP_SCAN}"
)

echo "=== V2 buffer-pool comparison (4 workloads) ===" | tee "$SUMMARY"
echo "Date: $(date)"                                  | tee -a "$SUMMARY"
echo "Host: $(hostname)"                              | tee -a "$SUMMARY"
echo "Cores: $CORES"                                  | tee -a "$SUMMARY"
echo "TPC-C warmup/exec: ${WARMUP_TPCC}s / ${EXEC_TPCC}s" | tee -a "$SUMMARY"
echo "Scan pages/frames/threads/seconds/warmup: ${SCAN_PAGES}/${SCAN_FRAMES}/${SCAN_THREADS}/${SCAN_SECONDS}/${WARMUP_SCAN}" | tee -a "$SUMMARY"
echo ""                                               | tee -a "$SUMMARY"

# ---------------------------------------------------------------------------
# Build all binaries up-front (one release build per bin × variant).
# ---------------------------------------------------------------------------
if [ "$SKIP_BUILD" != "1" ]; then
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        tag=$(echo "$label" | tr '[:upper:]' '[:lower:]' | tr -d ' -')
        echo "Building $label [$features] ..." | tee -a "$SUMMARY"

        cargo build --release --bin "$TPCC_BIN"  --features "$features" 2>&1 | tail -1
        cp -f "$TARGET/$TPCC_BIN"  "$TARGET/${TPCC_BIN}_${tag}"

        cargo build --release --bin "$TRANS_BIN" --features "$features" 2>&1 | tail -1
        cp -f "$TARGET/$TRANS_BIN" "$TARGET/${TRANS_BIN}_${tag}"
    done
    echo "" | tee -a "$SUMMARY"
else
    echo "Skipping build (SKIP_BUILD=1)" | tee -a "$SUMMARY"; echo "" | tee -a "$SUMMARY"
fi

# ---------------------------------------------------------------------------
# TPC-C scenarios
# ---------------------------------------------------------------------------
for sc in "${TPCC_SCENARIOS[@]}"; do
    IFS='|' read -r sc_label W T BP <<< "$sc"
    echo "================================================================" | tee -a "$SUMMARY"
    if [ "$BP" = "0" ]; then
        echo "Scenario: $sc_label  (w=$W t=$T bp=1GB/warehouse = in-memory, warmup=${WARMUP_TPCC}s exec=${EXEC_TPCC}s)" | tee -a "$SUMMARY"
    else
        echo "Scenario: $sc_label  (w=$W t=$T bp=${BP}GB, warmup=${WARMUP_TPCC}s exec=${EXEC_TPCC}s)" | tee -a "$SUMMARY"
    fi
    echo "================================================================" | tee -a "$SUMMARY"

    printf "%-12s %-12s %-10s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
        "variant" "commits" "avg(µs)" "p50" "p75" "p90" "p95" "p99" "p99.9" "max" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        tag=$(echo "$label" | tr '[:upper:]' '[:lower:]' | tr -d ' -')
        BIN="$TARGET/${TPCC_BIN}_${tag}"
        LOGFILE="$OUTDIR/${sc_label}_${tag}_${TIMESTAMP}.log"

        "$BIN" -w "$W" -t "$T" -b "$BP" -d "$WARMUP_TPCC" -D "$EXEC_TPCC" >"$LOGFILE" 2>&1 || \
            echo "[$sc_label/$label] exited non-zero" | tee -a "$SUMMARY"

        commits=$(grep "^NewOrder" "$LOGFILE" | grep -oP 'commits: \K[0-9,]+' | tr -d ',' || echo "N/A")
        avg=$(grep "^  mean" "$LOGFILE" | grep -oP '[0-9.]+' | head -1 || echo "N/A")
        p50=$(grep "^  p50"  "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p75=$(grep "^  p75"  "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p90=$(grep "^  p90"  "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p95=$(grep "^  p95"  "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p99=$(grep "^  p99 " "$LOGFILE" | awk '{print $2}' || echo "N/A")
        p999=$(grep "^  p99.9 " "$LOGFILE" | awk '{print $2}' || echo "N/A")
        max_us=$(grep "^  max" "$LOGFILE" | awk '{print $2}' || echo "N/A")

        printf "%-12s %-12s %-10s %-8s %-8s %-8s %-8s %-8s %-8s %-8s\n" \
            "$label" "$commits" "$avg" "$p50" "$p75" "$p90" "$p95" "$p99" "$p999" "$max_us" | tee -a "$SUMMARY"
    done
    echo "" | tee -a "$SUMMARY"
done

# ---------------------------------------------------------------------------
# Scan / uniform scenarios via bp_translation_bench
# ---------------------------------------------------------------------------
for sc in "${SCAN_SCENARIOS[@]}"; do
    IFS='|' read -r sc_label SEQ PAGES FRAMES THREADS SECS WARM <<< "$sc"
    echo "================================================================" | tee -a "$SUMMARY"
    pattern=$([ "$SEQ" = "1" ] && echo "sequential" || echo "uniform random")
    echo "Scenario: $sc_label  ($pattern, pages=$PAGES frames=$FRAMES threads=$THREADS seconds=$SECS warmup=${WARM}s)" | tee -a "$SUMMARY"
    echo "================================================================" | tee -a "$SUMMARY"

    printf "%-12s %-16s %-12s\n" "variant" "ops" "avg_ns" | tee -a "$SUMMARY"

    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r features label <<< "$entry"
        tag=$(echo "$label" | tr '[:upper:]' '[:lower:]' | tr -d ' -')
        BIN="$TARGET/${TRANS_BIN}_${tag}"
        LOGFILE="$OUTDIR/${sc_label}_${tag}_${TIMESTAMP}.log"

        if [ "$SEQ" = "1" ]; then
            SEQ_FLAG="--sequential"
        else
            SEQ_FLAG=""
        fi

        "$BIN" -n "$PAGES" -f "$FRAMES" -t "$THREADS" -s "$SECS" -w "$WARM" --theta 0.0 $SEQ_FLAG \
            >"$LOGFILE" 2>&1 || echo "[$sc_label/$label] exited non-zero" | tee -a "$SUMMARY"

        total_ops=$(grep "Total ops:"   "$LOGFILE" | awk '{print $3}' || echo "N/A")
        avg_ns=$(grep   "Avg latency:" "$LOGFILE" | awk '{print $3}' || echo "N/A")

        printf "%-12s %-16s %-12s\n" "$label" "$total_ops" "$avg_ns" | tee -a "$SUMMARY"
    done
    echo "" | tee -a "$SUMMARY"
done

echo "=== Done. Logs in $OUTDIR/ ===" | tee -a "$SUMMARY"
echo "Summary: $SUMMARY"
