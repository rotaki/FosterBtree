#!/usr/bin/env bash
# Wrapper: run the full LAPT benchmark with TRIALS=5, then email rotaki@uchicago.edu
# with the resulting plots attached. Designed for unattended background use
# (nohup-friendly, no TTY required).
set -uo pipefail

EMAIL="rotaki@uchicago.edu"
# Phone push: install ntfy app on your phone (iOS/Android), pick any
# hard-to-guess topic name and subscribe to it. Set NTFY_TOPIC below (or
# `export NTFY_TOPIC=...` before running). Leave empty to disable.
NTFY_TOPIC="${NTFY_TOPIC:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

LOG="$SCRIPT_DIR/run_and_email.log"
exec >"$LOG" 2>&1
echo "[$(date)] starting"

# ----- run the benchmark -----
# Defaults: 5 trials each + 10 s warmup for sat-state. The W_SAT bump is to
# ensure the meta + page cachelines + OPM bucket are warm before timing —
# shorter warmup leaves outliers (e.g. LIPAH at S=10/W=3 read 53 Mops/s on
# one run vs ~107 after a proper W=10 warmup). Override any of these as
# env vars if you want a quicker smoke or a tighter long run.
TRIALS=${TRIALS:-5} \
TRIALS_BTREE_GET=${TRIALS_BTREE_GET:-5} \
W_SAT=${W_SAT:-10} \
    bash dbspektrum_paper/run_all_benchmarks.sh
BENCH_EXIT=$?
echo "[$(date)] bench exit code: $BENCH_EXIT"

# ----- locate the output directory -----
# run_all_benchmarks.sh writes its OUTDIR path to .last_bench_run on success
LATEST_DIR=""
if [ -f .last_bench_run ]; then
    LATEST_DIR=$(cat .last_bench_run 2>/dev/null)
fi
if [ -z "$LATEST_DIR" ] || [ ! -d "$LATEST_DIR" ]; then
    # Fall back: pick newest run_* under lapt_paper/results/
    LATEST_DIR=$(ls -td "$SCRIPT_DIR"/results/run_*/ 2>/dev/null | head -1)
fi
echo "[$(date)] LATEST_DIR=$LATEST_DIR"

# ----- compose + send email -----
SUBJECT="LAPT bench done (exit $BENCH_EXIT) on $(hostname)"
BODY=$(mktemp)
{
    echo "LAPT benchmark completed at $(date)."
    echo "Host: $(hostname -f)"
    echo "Exit code: $BENCH_EXIT (0 = success)"
    echo
    if [ -n "$LATEST_DIR" ] && [ -f "$LATEST_DIR/config.txt" ]; then
        echo "=== config ==="
        cat "$LATEST_DIR/config.txt"
        echo
    fi
    if [ -n "$LATEST_DIR" ] && [ -f "$LATEST_DIR/run.log" ]; then
        echo "=== run log (last 80 lines) ==="
        tail -80 "$LATEST_DIR/run.log"
    fi
    echo
    echo "Results live at: $LATEST_DIR"
    echo "(Plots attached to this email if mailx -a worked.)"
} > "$BODY"

# Collect existing PNG plots
ATTACHMENTS=()
for f in seq_payload.png crosstab_44t.png btree_get.png btree_range_scan.png; do
    p="$LATEST_DIR/plots/$f"
    if [ -f "$p" ]; then
        ATTACHMENTS+=("$p")
    fi
done

# Use the python MIME mailer (bsd-mailx on this box doesn't actually attach
# files — its -a flag is for additional headers).
python3 "$SCRIPT_DIR/_send_email.py" "$EMAIL" "$SUBJECT" "$BODY" "${ATTACHMENTS[@]}"
MAIL_EXIT=$?
echo "[$(date)] mailx exit: $MAIL_EXIT"

rm -f "$BODY"

# ----- phone push via ntfy.sh -----
if [ -n "$NTFY_TOPIC" ]; then
    PUSH_MSG="LAPT bench done (exit $BENCH_EXIT) on $(hostname)
results: $LATEST_DIR"
    curl -s --max-time 15 \
        -H "Title: LAPT bench done" \
        -H "Priority: default" \
        -d "$PUSH_MSG" \
        "https://ntfy.sh/$NTFY_TOPIC" >/dev/null \
        && echo "[$(date)] ntfy push sent to $NTFY_TOPIC" \
        || echo "[$(date)] ntfy push FAILED"
    # Attach the headline plot if it exists
    PLOT="$LATEST_DIR/plots/seq_payload.png"
    if [ -f "$PLOT" ]; then
        curl -s --max-time 30 \
            -T "$PLOT" \
            -H "Filename: seq_payload.png" \
            "https://ntfy.sh/$NTFY_TOPIC" >/dev/null \
            && echo "[$(date)] ntfy plot sent" \
            || echo "[$(date)] ntfy plot upload FAILED"
    fi
fi

echo "[$(date)] done"
exit $BENCH_EXIT
