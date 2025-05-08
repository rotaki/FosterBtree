#!/usr/bin/env bash
# ---------------------------------------------------------------------------
#  start_stack.sh  –  launch InfluxDB + Telegraf from manual binary installs
#
#  Requires:
#    • INFLUX_HOME  – path that contains usr/bin/influxd
#    • TELEGRAF_HOME – path that contains usr/bin/telegraf
#  Token is hard-wired to “hoge”.
# ---------------------------------------------------------------------------

set -euo pipefail
trap 'echo -e "\n❌  failed at line $LINENO" 1>&2' ERR

export INFLUX_HOME=./dir_influxdb
export TELEGRAF_HOME=./dir_telegraf

#── sanity checks ─────────────────────────────────────────────────────────────
if [[ -z "${INFLUX_HOME:-}" || -z "${TELEGRAF_HOME:-}" ]]; then
    echo "Please export INFLUX_HOME and TELEGRAF_HOME before running." ; exit 1
fi

command -v influxd  >/dev/null || { echo "influxd not found in \$PATH";  exit 1; }
command -v telegraf >/dev/null || { echo "telegraf not found in \$PATH"; exit 1; }

if lsof -i:8086 -sTCP:LISTEN &>/dev/null; then
    echo "Port 8086 already in use. Stop the other process and retry." ; exit 1
fi

#── start influxd ─────────────────────────────────────────────────────────────
rm -rf "$INFLUX_HOME/data" "$INFLUX_HOME/engine"
mkdir -p "$INFLUX_HOME/data" "$INFLUX_HOME/engine"
INFLUXD_BOLT_PATH="$INFLUX_HOME/data/influxd.bolt" \
INFLUXD_ENGINE_PATH="$INFLUX_HOME/engine" \
    influxd 1>"$INFLUX_HOME/influxd.log" 2>&1 &
INFLUX_PID=$!
echo "▶  influxd started (pid $INFLUX_PID) … waiting for :8086"

for _ in {1..120}; do                    # ≈60 s max
    if kill -0 "$INFLUX_PID" 2>/dev/null; then
        (exec 3<>/dev/tcp/127.0.0.1/8086) &>/dev/null && break
    else
        echo "influxd exited prematurely:" ; tail -n40 "$INFLUX_HOME/influxd.log"
        exit 1
    fi
    sleep 0.5
done || { echo "Timed out waiting for port 8086."; tail -n40 "$INFLUX_HOME/influxd.log"; exit 1; }

#── bootstrap via HTTP (fixed token = hoge) ───────────────────────────────────
ORG="tlb-org"  BUCKET="tlb"  TOKEN="hoge"
curl -sf -X POST http://127.0.0.1:8086/api/v2/setup \
     -H "Content-Type: application/json" \
     -d "{\"username\":\"admin\",\"password\":\"password\",\
\"org\":\"$ORG\",\"bucket\":\"$BUCKET\",\"token\":\"$TOKEN\",\
\"retentionPeriodSeconds\":0,\"force\":true}" >/dev/null \
  || { echo "setup API call failed"; tail -n40 "$INFLUX_HOME/influxd.log"; exit 1; }
echo "✓  InfluxDB initialised (token=hoge)"

#── write Telegraf config ─────────────────────────────────────────────────────
mkdir -p "$TELEGRAF_HOME"                # <-- add this
cat >"$TELEGRAF_HOME/telegraf.conf"<<EOF
[[inputs.socket_listener]]
  service_address            = "udp://:8089"
  data_format                = "influx"
  influx_timestamp_precision = "1ns"
  read_buffer_size           = "4MiB"

[[outputs.influxdb_v2]]
  urls         = ["http://127.0.0.1:8086"]
  token        = "$TOKEN"
  organization = "$ORG"
  bucket       = "$BUCKET"
EOF

#── start Telegraf ────────────────────────────────────────────────────────────
telegraf --config "$TELEGRAF_HOME/telegraf.conf" \
         1>"$TELEGRAF_HOME/telegraf.log" 2>&1 &
echo "✓  Telegraf running (UDP 8089). Stack ready."

# To check if the stack is running:
# lsof -iTCP:8086 -sTCP:LISTEN
# pgrep -fa telegraf
# ss -lpun 'sport = :8089'

# To stop the stack:
# pkill -f influxd
# pkill -f telegraf