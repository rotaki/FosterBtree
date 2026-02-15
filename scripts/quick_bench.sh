#!/usr/bin/env bash
# quick_bench.sh — Fast B-tree insert benchmark across buffer pool backends.
# ---------------------------------------------------------------------------
# Uses fbt_on_disk (direct B-tree inserts, NO transaction layer) to compare
# buffer pool implementations.  Each run finishes in seconds, not hours.
#
# Usage:
#   ./scripts/quick_bench.sh                          # defaults
#   ./scripts/quick_bench.sh -n 1000000 -t "1 4 8"   # 1M keys, multi-thread
#   ./scripts/quick_bench.sh -b 5000                  # larger buffer pool
#
# Flags:
#   -n  number of keys     (default: 200000)
#   -t  thread counts      (default: "1")
#   -k  key size bytes     (default: 100)
#   -b  buffer pool frames (default: 10000)
#   -r  ops ratio          (default: "1:0:0:0" = insert only)
#                          format: insert:update:delete:get
# ---------------------------------------------------------------------------

set -euo pipefail

# ─────────── Defaults ──────────────────────────────────────────────────────

NUM_KEYS=200000
THREAD_COUNTS="1"
KEY_SIZE=100
BP_SIZE=10000
OPS_RATIO="1:0:0:0"

# ─────────── Parse CLI ─────────────────────────────────────────────────────

usage() {
  cat <<'EOF'
Usage: quick_bench.sh [OPTIONS]

Options:
  -n NUM           Number of keys per thread   (default: 200000)
  -t "1 2 4"       Thread counts to test        (default: "1")
  -k NUM           Key size in bytes            (default: 100)
  -b NUM           Buffer pool size in frames   (default: 10000)
  -r RATIO         Ops ratio insert:update:delete:get (default: "1:0:0:0")
  -h               Show this help
EOF
  exit 0
}

while getopts ":n:t:k:b:r:h" opt; do
  case $opt in
    n) NUM_KEYS="$OPTARG" ;;
    t) THREAD_COUNTS="$OPTARG" ;;
    k) KEY_SIZE="$OPTARG" ;;
    b) BP_SIZE="$OPTARG" ;;
    r) OPS_RATIO="$OPTARG" ;;
    h) usage ;;
    \?) echo "Unknown option: -$OPTARG" >&2; usage ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

# ─────────── Variant table ─────────────────────────────────────────────────

declare -a VARIANT_NAMES=(
  "bp_lru"
  "bp_clock"
  "bp_pt"
)
declare -A VARIANT_FEATURES=(
  [bp_lru]=""
  [bp_clock]="bp_clock"
  [bp_pt]="bp_pt"
)

# ─────────── Build phase ───────────────────────────────────────────────────

TARGET_DIR="./target/release"
RESULTS_DIR="./bench_results"
mkdir -p "$RESULTS_DIR"

echo "================================================================"
echo "  BUILD PHASE"
echo "================================================================"

for VNAME in "${VARIANT_NAMES[@]}"; do
  FEATURES="${VARIANT_FEATURES[$VNAME]}"
  BIN="fbt_${VNAME}"
  echo -n "  Building ${BIN} ... "
  cargo build --release --bin fbt_on_disk --features "$FEATURES" 2>/dev/null
  cp -f "${TARGET_DIR}/fbt_on_disk" "${TARGET_DIR}/${BIN}"
  echo "done"
done

echo ""

# ─────────── Benchmark phase ──────────────────────────────────────────────

echo "================================================================"
echo "  BENCHMARK: fbt_on_disk insert"
echo "  keys=${NUM_KEYS}  key_size=${KEY_SIZE}  bp_frames=${BP_SIZE}"
echo "  ops_ratio=${OPS_RATIO}  threads=${THREAD_COUNTS}"
echo "================================================================"
echo ""

printf "%-15s %-8s %10s\n" "VARIANT" "THREADS" "TIME (s)"
printf "%-15s %-8s %10s\n" "-------" "-------" "--------"

for VNAME in "${VARIANT_NAMES[@]}"; do
  BIN="${TARGET_DIR}/fbt_${VNAME}"
  for T in $THREAD_COUNTS; do
    OUTFILE="${RESULTS_DIR}/fbt_${VNAME}_t${T}_n${NUM_KEYS}.txt"

    # Time the run (wall-clock seconds)
    START=$(date +%s%N)
    "$BIN" -t "$T" -n "$NUM_KEYS" -k "$KEY_SIZE" -b "$BP_SIZE" \
           -r "$OPS_RATIO" -u \
           > "$OUTFILE" 2>&1 || true
    END=$(date +%s%N)

    ELAPSED_MS=$(( (END - START) / 1000000 ))
    ELAPSED_S=$(echo "scale=3; $ELAPSED_MS / 1000" | bc)

    printf "%-15s %-8s %10s\n" "$VNAME" "$T" "${ELAPSED_S}"
  done
done

echo ""
echo "================================================================"
echo "  DONE — detailed output in ${RESULTS_DIR}/"
echo "================================================================"
