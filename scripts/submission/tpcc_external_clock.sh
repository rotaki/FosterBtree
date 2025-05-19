#!/usr/bin/env bash
# benchmark_tpcc.sh
# ---------------------------------------------------------------------------
# Build tpcc_external variants, then regenerate & benchmark the TPCC database.
# Now accepts -w, -t, -D command‑line overrides so you can do quick local runs.
# Results from each benchmark run are captured under ./results/ …
# ---------------------------------------------------------------------------

set -euo pipefail

# ───────────── Defaults (overridable with -w, -t, -D) ───────────────────────
WAREHOUSES=500       # -w
THREADS=40           # -t
DURATION=200         # -D

# ───────────── Parse command‑line switches  ────────────────────────────────
usage() {
  echo "Usage: $0 [-w warehouses] [-t threads] [-D duration]" >&2
  exit 1
}

while getopts ":w:t:D:h" opt; do
  case $opt in
    w) WAREHOUSES=$OPTARG ;;
    t) THREADS=$OPTARG   ;;
    D) DURATION=$OPTARG  ;;
    h) usage ;;
    \?) echo "Invalid option: -$OPTARG" >&2; usage ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

# ───────────── Paths & variant table  ──────────────────────────────────────
TARGET_DIR="./target/release"
RESULTS_DIR="./results"
mkdir -p "$RESULTS_DIR"

declare -A VARIANTS=(
  [tpcc_external_vmc_no_hint]="no_tree_hint vmcache  event_tracer"
  [tpcc_external_vmc_tree_hint]="vmcache  event_tracer"
  [tpcc_external_bp_clock_no_hint]="bp_clock no_tree_hint no_bp_hint event_tracer"
  [tpcc_external_bp_clock_tree_hint]="bp_clock no_bp_hint event_tracer"
  [tpcc_external_bp_clock_bp_hint]="bp_clock no_tree_hint event_tracer"
  [tpcc_external_bp_clock_all_hint]="bp_clock event_tracer"
)

# ───────────── Build phase  ────────────────────────────────────────────────
echo "▶ Building tpcc_db_gen…"
cargo build --release --bin tpcc_db_gen

echo "▶ Building tpcc_external variants…"
for BIN in "${!VARIANTS[@]}"; do
  echo "  • $BIN"
  cargo build --release --features "${VARIANTS[$BIN]}" --bin tpcc_external
  mv -f "${TARGET_DIR}/tpcc_external" "${TARGET_DIR}/${BIN}"
done

# ───────────── Benchmark runs  ─────────────────────────────────────────────
SUFFIX=1
for BIN in "${!VARIANTS[@]}"; do
  LOG_FILE="${RESULTS_DIR}/${BIN}_w${WAREHOUSES}_t${THREADS}_D${DURATION}_run${SUFFIX}.log"

  {
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "▶▶ Run #${SUFFIX}: ${BIN}  (w=${WAREHOUSES}, t=${THREADS}, D=${DURATION})"
    echo "═══════════════════════════════════════════════════════════════════════"

    # Fresh DB for each run
    rm -rf "./tpcc_db_w${WAREHOUSES}"
    rm -rf "./eventdb_${BIN}.db"
    rm -rf "./eventdb_${BIN}.db.wal"

    "${TARGET_DIR}/tpcc_db_gen" -w "${WAREHOUSES}"

    DB_PATH="./eventdb_${BIN}.db" \
      "${TARGET_DIR}/${BIN}" \
        -w "${WAREHOUSES}" -t "${THREADS}" -D "${DURATION}" -d 0 -b 32
  } |& tee "${LOG_FILE}"

  ((SUFFIX++))
done
