#!/usr/bin/env bash
# tpcc_bench.sh — TPC-C benchmark across buffer pool backends.
# ---------------------------------------------------------------------------
# Uses the `tpcc` binary (in-memory data gen) so no pre-generated DB needed.
# Data generation takes ~1 min per warehouse; the benchmark itself is short.
#
# Usage:
#   ./scripts/tpcc_bench.sh                        # defaults (1W, 1T, 10s)
#   ./scripts/tpcc_bench.sh -w 2 -t 2 -D 30       # 2 warehouses, 2 threads, 30s exec
#
# Flags:
#   -w  number of warehouses   (default: 1)
#   -t  number of threads      (default: 1)
#   -d  warmup seconds         (default: 0)
#   -D  exec seconds           (default: 10)
#   -T  per-run timeout secs   (default: 300)
#   -S  stress preset: -w 2 -t 8 -D 30 (more threads + longer run so PT can show
#       gains from lock-free overflow and superscalar; override with -w/-t/-D after -S)
#
# To run the tpcc binary directly with PT and the same preset, use:
#   cargo run --release --features bp_pt --bin tpcc -- -w 2 -t 8 -D 30
# ---------------------------------------------------------------------------

set -euo pipefail

# ─────────── Defaults ────────────────────────────────────────────────────────

NUM_WAREHOUSES=1
NUM_THREADS=1
WARMUP=0
EXEC_TIME=10
RUN_TIMEOUT=300   # kill a run if it exceeds this many seconds

# ─────────── Parse CLI ───────────────────────────────────────────────────────

usage() {
  cat <<'EOF'
Usage: tpcc_bench.sh [OPTIONS]

Options:
  -w NUM    Number of warehouses          (default: 1)
  -t NUM    Number of threads             (default: 1)
  -d NUM    Warmup duration in seconds    (default: 0)
  -D NUM    Exec duration in seconds      (default: 10)
  -T NUM    Per-run timeout in seconds    (default: 300)
  -S        Stress preset (w=2, t=8, D=30) to see PT gains under concurrent load
  -h        Show this help
EOF
  exit 0
}

while getopts ":w:t:d:D:T:Sh" opt; do
  case $opt in
    w) NUM_WAREHOUSES="$OPTARG" ;;
    t) NUM_THREADS="$OPTARG" ;;
    d) WARMUP="$OPTARG" ;;
    D) EXEC_TIME="$OPTARG" ;;
    T) RUN_TIMEOUT="$OPTARG" ;;
    S)
      NUM_WAREHOUSES=2
      NUM_THREADS=8
      EXEC_TIME=30
      ;;
    h) usage ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

# ─────────── Variant table ───────────────────────────────────────────────────

declare -a VARIANT_NAMES=(
  "bp_lru"
  "bp_clock"
  "bp_pt"
  "bp_basic_hashmap"
)
declare -A VARIANT_FEATURES=(
  [bp_lru]=""
  [bp_clock]="bp_clock"
  [bp_pt]="bp_pt"
  [bp_basic_hashmap]="bp_basic_hashmap"
)

# ─────────── Build phase ─────────────────────────────────────────────────────

TARGET_DIR="./target/release"
RESULTS_DIR="./bench_results"
mkdir -p "$RESULTS_DIR"

echo "================================================================"
echo "  BUILD PHASE"
echo "================================================================"

for VNAME in "${VARIANT_NAMES[@]}"; do
  FEATURES="${VARIANT_FEATURES[$VNAME]}"
  BIN="tpcc_${VNAME}"
  echo -n "  Building ${BIN} ... "
  cargo build --release --bin tpcc --features "$FEATURES" 2>/dev/null
  cp -f "${TARGET_DIR}/tpcc" "${TARGET_DIR}/${BIN}"
  echo "done"
done

echo ""

# ─────────── Benchmark phase ────────────────────────────────────────────────

echo "================================================================"
echo "  TPC-C BENCHMARK"
echo "  warehouses=${NUM_WAREHOUSES}  threads=${NUM_THREADS}"
echo "  warmup=${WARMUP}s  exec=${EXEC_TIME}s  timeout=${RUN_TIMEOUT}s"
echo "================================================================"
echo ""

printf "%-15s %12s %12s\n" "VARIANT" "THROUGHPUT" "TIME (s)"
printf "%-15s %12s %12s\n" "-------" "----------" "--------"

for VNAME in "${VARIANT_NAMES[@]}"; do
  BIN="${TARGET_DIR}/tpcc_${VNAME}"
  OUTFILE="${RESULTS_DIR}/tpcc_${VNAME}_w${NUM_WAREHOUSES}_t${NUM_THREADS}_D${EXEC_TIME}.txt"

  START=$(date +%s%N)
  timeout "${RUN_TIMEOUT}s" "$BIN" \
    -w "$NUM_WAREHOUSES" -t "$NUM_THREADS" \
    -d "$WARMUP" -D "$EXEC_TIME" \
    > "$OUTFILE" 2>&1 || true
  END=$(date +%s%N)

  ELAPSED_MS=$(( (END - START) / 1000000 ))
  ELAPSED_S=$(echo "scale=1; $ELAPSED_MS / 1000" | bc)

  # Extract throughput line
  THROUGHPUT=$(grep -oP 'Throughput: \K[0-9.]+' "$OUTFILE" 2>/dev/null || echo "N/A")

  printf "%-15s %10s %10s\n" "$VNAME" "${THROUGHPUT} txn/s" "${ELAPSED_S}"
done

echo ""
echo "================================================================"
echo "  DONE — full output in ${RESULTS_DIR}/"
echo "================================================================"
