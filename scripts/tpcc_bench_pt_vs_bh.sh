#!/usr/bin/env bash
# PT vs baselines: longer runs, warmup, multiple iterations.
# Baselines: dashmap (DashMap) and hashmap (single RwLock).
# Usage: ./scripts/tpcc_bench_pt_vs_bh.sh [ -w W ] [ -t T ] [ -d warmup ] [ -D exec ] [ -n runs ]
# For PT to show gains over hashmap, use more threads (e.g. -t 8); paper used 192 threads.
set -euo pipefail

W=1
T=1
WARMUP=15
EXEC=60
RUNS=5
TIMEOUT=600

usage() {
  echo "Usage: $0 [ -w warehouses ] [ -t threads ] [ -d warmup_sec ] [ -D exec_sec ] [ -n num_runs ]"
  echo "  Default: w=${W} t=${T} warmup=${WARMUP}s exec=${EXEC}s runs=${RUNS}"
  echo "  Baselines: PT, dashmap (DashMap), hashmap (single RwLock)."
  exit 0
}
while getopts "w:t:d:D:n:T:h" o; do
  case "$o" in
    w) W="$OPTARG" ;;
    t) T="$OPTARG" ;;
    d) WARMUP="$OPTARG" ;;
    D) EXEC="$OPTARG" ;;
    n) RUNS="$OPTARG" ;;
    T) TIMEOUT="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

TARGET="./target/release"
RESULTS="./bench_results"
mkdir -p "$RESULTS"

echo "================================================================"
echo "  PT vs baselines (dashmap + hashmap)"
echo "  warehouses=$W  threads=$T  warmup=${WARMUP}s  exec=${EXEC}s  runs=$RUNS"
echo "================================================================"

echo "Building PT, dashmap, and hashmap..."
cargo build --release --bin tpcc --features "bp_pt" 2>/dev/null
cp -f "$TARGET/tpcc" "$TARGET/tpcc_pt"
cargo build --release --bin tpcc --features "bp_dashmap" 2>/dev/null
cp -f "$TARGET/tpcc" "$TARGET/tpcc_dashmap"
cargo build --release --bin tpcc --features "bp_hashmap" 2>/dev/null
cp -f "$TARGET/tpcc" "$TARGET/tpcc_hashmap"
echo ""

run_one() {
  local bin=$1
  local run_id=$2
  local out
  out=$(timeout "${TIMEOUT}s" "$bin" -w "$W" -t "$T" -d "$WARMUP" -D "$EXEC" 2>&1) || true
  echo "$out" | grep -oP 'Throughput: \K[0-9.]+' || echo ""
}

collect_runs() {
  local name=$1
  local bin=$2
  local i
  local vals=()
  echo -n "  $name: "
  for i in $(seq 1 "$RUNS"); do
    local v
    v=$(run_one "$bin" "$i")
    if [ -z "$v" ]; then v="0"; fi
    vals+=("$v")
    echo -n "${v} "
  done
  echo ""
  for v in "${vals[@]}"; do echo "$v"; done | awk -v name="$name" '
    { sum += $1; sumsq += $1*$1; n++ }
    END {
      if (n<1) { mean=0; std=0 } else {
        mean = sum/n;
        std = (n>1) ? sqrt((sumsq/n - mean*mean)*n/(n-1)) : 0;
      }
      printf "    %s  mean = %.1f  std = %.1f  (txn/s)\n", name, mean, std;
    }'
}

echo "Running PT ($RUNS runs)..."
collect_runs "PT   " "$TARGET/tpcc_pt"

echo "Running dashmap ($RUNS runs)..."
collect_runs "Dash " "$TARGET/tpcc_dashmap"

echo "Running hashmap ($RUNS runs)..."
collect_runs "Hash " "$TARGET/tpcc_hashmap"

echo ""
echo "Done. Compare mean throughputs above."
