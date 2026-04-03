#!/usr/bin/env bash
# tpcc_bench_10w10t.sh — TPC-C benchmark: clock vs fp vs fp_two vs fp_four
# ---------------------------------------------------------------------------
# Sweeps warehouse counts (1,2,4,8,16) with 16 threads by default.
#
# Usage:
#   ./scripts/tpcc_bench_10w10t.sh                  # defaults
#   ./scripts/tpcc_bench_10w10t.sh -D 60 -n 3       # 60s exec, 3 runs each
#
# Flags:
#   -d  warmup seconds         (default: 10)
#   -D  exec seconds           (default: 30)
#   -T  per-run timeout secs   (default: 600)
#   -n  number of runs         (default: 1)
#   -h  show help
# ---------------------------------------------------------------------------

set -euo pipefail

# ─────────── Defaults ────────────────────────────────────────────────────────

WAREHOUSE_COUNTS=(1 2 4 8 16)
NUM_THREADS=16
WARMUP=10
EXEC_TIME=30
RUN_TIMEOUT=600
NUM_RUNS=1

# ─────────── Parse CLI ───────────────────────────────────────────────────────

usage() {
  cat <<'EOF'
Usage: tpcc_bench_10w10t.sh [OPTIONS]

Options:
  -d NUM    Warmup duration in seconds    (default: 10)
  -D NUM    Exec duration in seconds      (default: 30)
  -T NUM    Per-run timeout in seconds    (default: 600)
  -n NUM    Number of runs per variant    (default: 1)
  -h        Show this help

Fixed: warehouses=1,2,4,8,16  threads=16
EOF
  exit 0
}

while getopts ":d:D:T:n:h" opt; do
  case $opt in
    d) WARMUP="$OPTARG" ;;
    D) EXEC_TIME="$OPTARG" ;;
    T) RUN_TIMEOUT="$OPTARG" ;;
    n) NUM_RUNS="$OPTARG" ;;
    h) usage ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

# ─────────── Variant table ───────────────────────────────────────────────────

declare -a VARIANT_NAMES=(
  "clock"
  "fp"
  "fp_two"
  "fp_four"
)
declare -A VARIANT_FEATURES=(
  [clock]="bp_clock"
  [fp]="bp_pt_bucket"
  [fp_two]="bp_pt2_bucket"
  [fp_four]="bp_pt4_bucket"
)

# ─────────── Output setup ────────────────────────────────────────────────────

TARGET_DIR="./target/release"
RESULTS_DIR="./bench_results"
mkdir -p "$RESULTS_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOGFILE="${RESULTS_DIR}/tpcc_sweep_${TIMESTAMP}.log"

# Duplicate all stdout+stderr to the log file while still printing to terminal
exec > >(tee -a "$LOGFILE") 2>&1

echo "Log file: ${LOGFILE}"
echo ""

# ─────────── Build phase ─────────────────────────────────────────────────────

echo "================================================================"
echo "  BUILD PHASE"
echo "================================================================"

for VNAME in "${VARIANT_NAMES[@]}"; do
  FEATURES="${VARIANT_FEATURES[$VNAME]}"
  BIN="tpcc_${VNAME}"
  echo -n "  Building ${BIN} (features: ${FEATURES}) ... "
  cargo build --release --bin tpcc --features "$FEATURES"
  cp -f "${TARGET_DIR}/tpcc" "${TARGET_DIR}/${BIN}"
  echo "done"
done

echo ""

# ─────────── Benchmark phase ────────────────────────────────────────────────

echo "================================================================"
echo "  TPC-C BENCHMARK SWEEP"
echo "  warehouses=${WAREHOUSE_COUNTS[*]}  threads=${NUM_THREADS}"
echo "  warmup=${WARMUP}s  exec=${EXEC_TIME}s  timeout=${RUN_TIMEOUT}s"
echo "  runs=${NUM_RUNS}"
echo "================================================================"
echo ""

# results[variant:warehouses] = "tp1 tp2 ..."
declare -A ALL_THROUGHPUTS

for NUM_WAREHOUSES in "${WAREHOUSE_COUNTS[@]}"; do
  echo "────────────────────────────────────────────────────────────────"
  echo "  Warehouses: ${NUM_WAREHOUSES}  Threads: ${NUM_THREADS}"
  echo "────────────────────────────────────────────────────────────────"

  for VNAME in "${VARIANT_NAMES[@]}"; do
    BIN="${TARGET_DIR}/tpcc_${VNAME}"
    THROUGHPUTS=()

    for ((run=1; run<=NUM_RUNS; run++)); do
      OUTFILE="${RESULTS_DIR}/tpcc_${VNAME}_w${NUM_WAREHOUSES}_t${NUM_THREADS}_D${EXEC_TIME}_run${run}.txt"

      echo -n "  Running ${VNAME} w=${NUM_WAREHOUSES} [run ${run}/${NUM_RUNS}] ... "

      START=$(date +%s%N)
      timeout "${RUN_TIMEOUT}s" "$BIN" \
        -w "$NUM_WAREHOUSES" -t "$NUM_THREADS" \
        -d "$WARMUP" -D "$EXEC_TIME" \
        > "$OUTFILE" 2>&1 || true
      END=$(date +%s%N)

      ELAPSED_MS=$(( (END - START) / 1000000 ))
      ELAPSED_S=$(echo "scale=1; $ELAPSED_MS / 1000" | bc)

      THROUGHPUT=$(grep -oP 'Throughput: \K[0-9.]+' "$OUTFILE" 2>/dev/null || echo "0")
      THROUGHPUTS+=("$THROUGHPUT")

      echo "${THROUGHPUT} txn/s  (${ELAPSED_S}s)"
    done

    KEY="${VNAME}:${NUM_WAREHOUSES}"
    ALL_THROUGHPUTS[$KEY]="${THROUGHPUTS[*]}"
  done

  echo ""
done

# ─────────── Summary ────────────────────────────────────────────────────────

echo "================================================================"
echo "  SUMMARY  (threads=${NUM_THREADS} D=${EXEC_TIME}s runs=${NUM_RUNS})"
echo "================================================================"
echo ""

# Helper: compute mean from space-separated values
mean_of() {
  echo "$@" | tr ' ' '\n' | awk '{ sum+=$1; n++ } END { if(n>0) printf "%.2f", sum/n; else printf "N/A" }'
}

if [ "$NUM_RUNS" -eq 1 ]; then
  # Simple table: one throughput per cell
  printf "%-15s" "VARIANT"
  for W in "${WAREHOUSE_COUNTS[@]}"; do
    printf " %15s" "w=${W}"
  done
  echo ""
  printf "%-15s" "-------"
  for W in "${WAREHOUSE_COUNTS[@]}"; do
    printf " %15s" "----------"
  done
  echo ""

  for VNAME in "${VARIANT_NAMES[@]}"; do
    printf "%-15s" "$VNAME"
    for W in "${WAREHOUSE_COUNTS[@]}"; do
      TP="${ALL_THROUGHPUTS[${VNAME}:${W}]}"
      printf " %12s txn/s" "$TP"
    done
    echo ""
  done
else
  # Mean table
  printf "%-15s" "VARIANT"
  for W in "${WAREHOUSE_COUNTS[@]}"; do
    printf " %15s" "w=${W} (mean)"
  done
  echo ""
  printf "%-15s" "-------"
  for W in "${WAREHOUSE_COUNTS[@]}"; do
    printf " %15s" "----------"
  done
  echo ""

  for VNAME in "${VARIANT_NAMES[@]}"; do
    printf "%-15s" "$VNAME"
    for W in "${WAREHOUSE_COUNTS[@]}"; do
      TPS="${ALL_THROUGHPUTS[${VNAME}:${W}]}"
      M=$(mean_of $TPS)
      printf " %12s txn/s" "$M"
    done
    echo ""
  done
fi

echo ""
echo "================================================================"
echo "  DONE — full output in ${RESULTS_DIR}/"
echo "  Log: ${LOGFILE}"
echo "================================================================"
