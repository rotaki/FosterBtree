#!/usr/bin/env bash
# benchmark_fbt_lookup.sh
# ---------------------------------------------------------------------------
# Build fbt_lookup_external variants, then regenerate & benchmark the lookup
# database for each variant. Accepts -n, -t, -D overrides so you can do quick
# local runs. Results from each benchmark run are captured under ./results_lookup/ …
# ---------------------------------------------------------------------------

set -euo pipefail

# ───────────── Defaults (overridable with -n, -t, -D) ───────────────────────
NUM_LOOKUPS=300000000    # -n
THREADS=40               # -t
DURATION=200             # -D
DB_BLOCK_SIZE=128        # Block size for DB generation
EXT_BLOCK_SIZE=32        # Block size for external lookup

# ───────────── Parse command-line switches  ────────────────────────────────
usage() {
  echo "Usage: $0 [-n num_lookups] [-t threads] [-D duration]" >&2
  exit 1
}

while getopts ":n:t:D:h" opt; do
  case $opt in
    n) NUM_LOOKUPS=$OPTARG ;;
    t) THREADS=$OPTARG     ;;
    D) DURATION=$OPTARG    ;;
    h) usage ;;
    \?) echo "Invalid option: -$OPTARG" >&2; usage ;;
    :)  echo "Option -$OPTARG requires an argument." >&2; usage ;;
  esac
done
shift $((OPTIND - 1))

# ───────────── Paths & variant table  ──────────────────────────────────────
TARGET_DIR="./target/release"
RESULTS_DIR="./results_lookup"
mkdir -p "$RESULTS_DIR"

# key = final binary name, value = cargo feature list
declare -A VARIANTS=(
  [fbt_lookup_external_bp_clock]="bp_clock event_tracer"
  [fbt_lookup_external_vmcache]="vmcache event_tracer"
  [fbt_lookup_external_no_bp_hint]="bp_clock no_bp_hint event_tracer"
)

# ───────────── Build phase  ────────────────────────────────────────────────
echo "▶ Building fbt_lookup_db_gen…"
cargo build --release --bin fbt_lookup_db_gen

echo "▶ Building fbt_lookup_external variants…"
for BIN in "${!VARIANTS[@]}"; do
  echo "  • $BIN"
  cargo build --release --bin fbt_lookup_external --features "${VARIANTS[$BIN]}"
  mv -f "${TARGET_DIR}/fbt_lookup_external" "${TARGET_DIR}/${BIN}"
  chmod +x "${TARGET_DIR}/${BIN}"
done

# ───────────── Benchmark runs  ─────────────────────────────────────────────
SUFFIX=1
for BIN in "${!VARIANTS[@]}"; do
  DB_FILE="eventdb_${BIN}.db"
  LOG_FILE="${RESULTS_DIR}/${BIN}_n${NUM_LOOKUPS}_t${THREADS}_D${DURATION}_run${SUFFIX}.log"

  # Fresh DB for each variant run
  rm -rf "./fbt_db_n${NUM_LOOKUPS}"
  rm -rf "./${DB_FILE}" "./${DB_FILE}.wal"

  echo "▶ Generating DB for ${BIN} (n=${NUM_LOOKUPS}, block=${DB_BLOCK_SIZE})"
  DB_PATH="${DB_FILE}" "${TARGET_DIR}/fbt_lookup_db_gen" -n "${NUM_LOOKUPS}" -b "${DB_BLOCK_SIZE}"

  {
    echo "═══════════════════════════════════════════════════════════════════════"
    echo "▶▶ Run #${SUFFIX}: ${BIN}  (n=${NUM_LOOKUPS}, t=${THREADS}, D=${DURATION})"
    echo "═══════════════════════════════════════════════════════════════════════"

    DB_PATH="${DB_FILE}" "${TARGET_DIR}/${BIN}" \
      -n "${NUM_LOOKUPS}" -t "${THREADS}" -D "${DURATION}" -d 0 -b "${EXT_BLOCK_SIZE}"
  } |& tee "${LOG_FILE}"

  ((SUFFIX++))
done
