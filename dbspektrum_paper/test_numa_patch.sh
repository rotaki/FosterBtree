#!/usr/bin/env bash
# Verify the NUMA first-touch patch: compare default (interleave on) vs
# --no-numa-interleave (legacy behavior). Same workload as the prior NUMA
# hypothesis test for direct comparison.
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

N=100000; F=200000; C=500; T=44; S=15; W=5; TRIALS=5

BIN="$PROJECT_ROOT/target/release/bp_translation_bench_lipah"
[ -x "$BIN" ] || { echo "missing $BIN"; exit 1; }

TS=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/numa_patch_${TS}"
mkdir -p "$OUTDIR/raw"
CSV="$OUTDIR/numa_patch.csv"
echo "mode,trial,mops,ns_per_op" > "$CSV"

unset PT_PROMOTE_PROB_NO_DEMOTE PT_PROMOTE_PROB_DEMOTE

extract_mops() { grep "^Throughput:" "$1" | grep -oP '\(\K[0-9.]+' | head -1 || echo "NaN"; }
extract_nsop() { grep "^Avg latency:" "$1" | grep -oP '[0-9.]+'    | head -1 || echo "NaN"; }

run_trials() {
    local mode=$1 extra=$2
    echo "=== $mode  (extra='$extra') ==="
    for t in $(seq 1 "$TRIALS"); do
        log="$OUTDIR/raw/${mode}_t${t}.log"
        "$BIN" --sequential -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
            --num-containers "$C" --no-page-fold --refresh-hints $extra \
            >"$log" 2>&1 || echo "  [$mode t=$t] non-zero exit"
        mops=$(extract_mops "$log"); nsop=$(extract_nsop "$log")
        echo "$mode,$t,$mops,$nsop" >> "$CSV"
        printf "  t=%-2d  %7.2f Mops/s  %7.1f ns/op\n" "$t" "$mops" "$nsop"
    done
}

echo "[$(date)] starting NUMA patch verification"

run_trials "interleave_on"  ""
run_trials "interleave_off" "--no-numa-interleave"

echo
echo "=== summary ==="
python3 - <<PY
import csv, statistics as st
rows = list(csv.DictReader(open("$CSV")))
by = {}
for r in rows:
    by.setdefault(r["mode"], []).append(float(r["mops"]))
print(f"{'mode':<18} {'n':>2} {'min':>7} {'max':>7} {'median':>7} {'mean':>7} {'stdev':>6} {'cv%':>5}")
for m in ["interleave_on","interleave_off"]:
    v = by.get(m, [])
    if not v: continue
    print(f"{m:<18} {len(v):>2} {min(v):>7.2f} {max(v):>7.2f} {st.median(v):>7.2f} {st.mean(v):>7.2f} {st.stdev(v):>6.2f} {100*st.stdev(v)/st.mean(v):>4.1f}%")
PY

echo "[$(date)] done. csv: $CSV"
