#!/usr/bin/env bash
# Focused payload sweep: 1K / 2K / 4K across LIPAH, PrediCache, LAPT, LAPT2.
# Sat state, --callback-path on every variant so the API is symmetric.
# Emails a summary table when done. Designed for nohup-friendly background use.
#
# Env knobs:
#   N=100000  F=200000  C=500
#   T=44  S=10  W=5  TRIALS=3
#   PAYLOADS="1024 2048 4096"
#   EMAIL=rotaki@uchicago.edu
#   SKIP_BUILD=1   reuse existing variant binaries
set -uo pipefail

EMAIL="${EMAIL:-rotaki@uchicago.edu}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

N=${N:-100000}
F=${F:-200000}
C=${C:-500}
T=${T:-44}
S=${S:-10}
W=${W:-5}
TRIALS=${TRIALS:-3}
SKIP_BUILD=${SKIP_BUILD:-0}
read -r -a PAYLOADS <<< "${PAYLOADS:-1024 2048 4096}"

TARGET="./target/release"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR="$SCRIPT_DIR/results/payload_focus_${TIMESTAMP}"
RAW="$OUTDIR/raw"
mkdir -p "$RAW"

LOG="$OUTDIR/run.log"
exec >"$LOG" 2>&1
echo "[$(date)] starting payload-focus run -> $OUTDIR"

declare -a VARIANTS=(
    "bp_clock|LIPAH"
    "bp_predicache|PrediCache"
    "bp_lapt|LAPT"
)

# ----- build (or reuse) variant binaries -----
for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    bin="$TARGET/bp_translation_bench_${tag}"
    if [ "$SKIP_BUILD" = "1" ] && [ -x "$bin" ]; then
        echo "[$(date)] reuse $bin"
        continue
    fi
    echo "[$(date)] build $tag ($features)"
    cargo build --release --bin bp_translation_bench --features "$features"
    cp -f "$TARGET/bp_translation_bench" "$bin"
done

# ----- run -----
CSV="$OUTDIR/results.csv"
echo "variant,payload_bytes,trial,mops" > "$CSV"

for entry in "${VARIANTS[@]}"; do
    IFS='|' read -r features label <<< "$entry"
    tag=$(echo "$label" | tr -d ' -' | tr 'A-Z' 'a-z')
    bin="$TARGET/bp_translation_bench_${tag}"
    for p in "${PAYLOADS[@]}"; do
        for trial in $(seq 1 "$TRIALS"); do
            logf="$RAW/payload_${tag}_p${p}_t${trial}.log"
            extra="--callback-path"
            if [ "$label" = "LIPAH" ]; then extra="$extra --refresh-hints"; fi
            echo "[$(date)] $label p=$p t=$trial"
            "$bin" --sequential -n "$N" -f "$F" -t "$T" -s "$S" -w "$W" \
                --num-containers "$C" --payload-bytes "$p" $extra \
                >"$logf" 2>&1 || echo "[$(date)] [WARN] $label p=$p t=$trial non-zero exit"
            mops=$(grep "^Throughput:" "$logf" | grep -oP '\(\K[0-9.]+' | head -1)
            echo "$label,$p,$trial,${mops:-NaN}" >> "$CSV"
        done
    done
done

# ----- summarize: mean + stddev per (variant, payload), in declared order -----
SUMMARY="$OUTDIR/summary.txt"
{
    echo "Payload-focus sweep — $(date)"
    echo "Host: $(hostname)"
    echo "Params: N=$N F=$F C=$C T=$T S=$S W=$W TRIALS=$TRIALS"
    echo "Payloads: ${PAYLOADS[*]}"
    echo "API: --callback-path on all variants; --refresh-hints on LIPAH"
    echo
    printf "%-12s %-10s %-14s %-12s %-6s\n" "variant" "payload" "mean Mops/s" "stddev" "n"
    printf "%-12s %-10s %-14s %-12s %-6s\n" "-------" "-------" "-----------" "------" "-"
    for entry in "${VARIANTS[@]}"; do
        IFS='|' read -r _features label <<< "$entry"
        for p in "${PAYLOADS[@]}"; do
            read -r mean stddev n <<< "$(awk -F',' -v v="$label" -v p="$p" '
                $1==v && $2==p && $4!="NaN" { sum+=$4; sq+=$4*$4; n++ }
                END {
                    if (n==0) { print "NaN NaN 0"; exit }
                    m=sum/n; var=sq/n - m*m; if (var<0) var=0
                    printf "%.2f %.3f %d", m, sqrt(var), n
                }' "$CSV")"
            printf "%-12s %-10s %-14s %-12s %-6s\n" "$label" "$p" "$mean" "$stddev" "$n"
        done
    done
} > "$SUMMARY"

cat "$SUMMARY"

# ----- email -----
echo "[$(date)] sending email to $EMAIL"
python3 "$SCRIPT_DIR/_send_email.py" \
    "$EMAIL" \
    "Payload-focus bench done on $(hostname)" \
    "$SUMMARY" \
    "$CSV"
echo "[$(date)] mail exit: $?"

echo "$OUTDIR" > .last_payload_focus_run
echo "[$(date)] done"
