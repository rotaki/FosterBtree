#!/usr/bin/env bash
# Test NUMA placement hypothesis for LIPAH sequential-sat variance.
# numactl isn't installed, so we use taskset to pin CPUs.  First-touch policy
# means a process pinned to node N's CPUs will allocate all its pages on
# node N's DRAM — so taskset is enough to test the locality hypothesis.
#
#   A: baseline (no pinning, 44 threads) — reproduces variance
#   B: pinned to node 0 (CPUs 0-11,24-35, 24 threads) — guaranteed local
#   C: pinned to node 1 (CPUs 12-23,36-47, 24 threads) — guaranteed local
# If B and C are both stable and high → NUMA non-locality is the cause.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

N=100000
F=200000
C=500
S=15
W=5
TRIALS=5

BIN="$PROJECT_ROOT/target/release/bp_translation_bench_lipah"
[ -x "$BIN" ] || { echo "missing $BIN — build first"; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/numa_test_${TS}"
mkdir -p "$OUTDIR/raw"
CSV="$OUTDIR/numa_test.csv"
echo "mode,threads,trial,mops,ns_per_op" > "$CSV"

unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE

extract_mops() { grep "^Throughput:" "$1" | grep -oP '\(\K[0-9.]+' | head -1 || echo "NaN"; }
extract_nsop() { grep "^Avg latency:" "$1" | grep -oP '[0-9.]+'    | head -1 || echo "NaN"; }

run_trials() {
    local mode=$1 threads=$2 prefix=$3
    echo "=== $mode  (threads=$threads, prefix='$prefix') ==="
    for t in $(seq 1 "$TRIALS"); do
        log="$OUTDIR/raw/${mode}_t${t}.log"
        $prefix "$BIN" --sequential -n "$N" -f "$F" -t "$threads" -s "$S" -w "$W" \
            --num-containers "$C" --no-page-fold --refresh-hints \
            >"$log" 2>&1 || echo "  [$mode t=$t] non-zero exit"
        mops=$(extract_mops "$log"); nsop=$(extract_nsop "$log")
        echo "$mode,$threads,$t,$mops,$nsop" >> "$CSV"
        printf "  t=%-2d  %7.2f Mops/s  %7.1f ns/op\n" "$t" "$mops" "$nsop"
    done
}

echo "[$(date)] starting NUMA hypothesis test (taskset-based)"

run_trials "baseline_44t"       44 ""
run_trials "node0_pinned_24t"   24 "taskset -c 0-11,24-35"
run_trials "node1_pinned_24t"   24 "taskset -c 12-23,36-47"

echo
echo "=== summary ==="
python3 - <<PY
import csv, statistics as st
rows = list(csv.DictReader(open("$CSV")))
by = {}
for r in rows:
    by.setdefault(r["mode"], []).append(float(r["mops"]))
print(f"{'mode':<22} {'n':>2} {'min':>7} {'max':>7} {'median':>7} {'mean':>7} {'stdev':>6} {'cv%':>5}")
for m in ["baseline_44t","node0_pinned_24t","node1_pinned_24t"]:
    v = by.get(m, [])
    if not v: continue
    print(f"{m:<22} {len(v):>2} {min(v):>7.2f} {max(v):>7.2f} {st.median(v):>7.2f} {st.mean(v):>7.2f} {st.stdev(v):>6.2f} {100*st.stdev(v)/st.mean(v):>4.1f}%")
PY

echo "[$(date)] done. csv: $CSV"
