#!/usr/bin/env bash
# benchmark_tpcc.sh
# ---------------------------------------------------------------------------
# Build tpcc_external variants, then regenerate & benchmark the TPCC database.
# Now accepts -w, -t, -D command-line overrides so you can do quick local runs.
# ---------------------------------------------------------------------------

set -euo pipefail

# ───────────── Defaults (overridable with -w, -t, -D) ───────────────────────
WAREHOUSES=500       # -w
THREADS=40           # -t
DURATION=200         # -D

# ───────────── Parse command-line switches  ────────────────────────────────
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
INFLUX_SETUP="./influxdb_setup.sh"

declare -A VARIANTS=(
  [tpcc_external_vmc]="no_tree_hint no_bp_hint use_vmc_tpcc iouring_async influxdb_trace"
  [tpcc_external_no_hint]="no_tree_hint no_bp_hint iouring_async influxdb_trace"
  [tpcc_external_tree_hint]="no_bp_hint iouring_async influxdb_trace"
  [tpcc_external_bp_hint]="no_tree_hint iouring_async influxdb_trace"
  [tpcc_external_all_hint]="iouring_async influxdb_trace"
  [tpcc_external_all_hint_inmem]="inmem_hint_only iouring_async influxdb_trace"
)

# ───────────── Build phase  ────────────────────────────────────────────────
echo "▶ Building tpcc_db_gen..."
cargo build --release --features "iouring_async" --bin tpcc_db_gen

for BIN in "${!VARIANTS[@]}"; do
  echo "▶ Building ${BIN}..."
  cargo build --release --features "${VARIANTS[$BIN]}" --bin tpcc_external
  mv -f "${TARGET_DIR}/tpcc_external" "${TARGET_DIR}/${BIN}"
done

# ───────────── Ensure InfluxDB is running  ─────────────────────────────────
if ! pgrep -x "influxd" >/dev/null; then
  echo "▶ InfluxDB not running – starting via '${INFLUX_SETUP}'"
  "${INFLUX_SETUP}"
else
  echo "▶ InfluxDB already running."
fi

# ───────────── Benchmark runs  ─────────────────────────────────────────────
SUFFIX=1
for BIN in "${!VARIANTS[@]}"; do
  echo "═══════════════════════════════════════════════════════════════════════"
  echo "▶▶ Run #${SUFFIX}: ${BIN}  (w=${WAREHOUSES}, t=${THREADS}, D=${DURATION})"
  echo "═══════════════════════════════════════════════════════════════════════"

  rm -rf "./tpcc_db_w${WAREHOUSES}"

  INFLUX_DB_SUFFIX=${SUFFIX} \
    "${TARGET_DIR}/tpcc_db_gen" -w "${WAREHOUSES}"

  INFLUX_DB_SUFFIX=${SUFFIX} \
    "${TARGET_DIR}/${BIN}" \
      -w "${WAREHOUSES}" -t "${THREADS}" -D "${DURATION}"

  ((SUFFIX++))
done
