#!/usr/bin/env bash
# Drives one scenario through the engine and renders the Grafana dashboard to
# docs/img/. Everything in the README's Grafana section comes from this script.
#
#   docker compose up -d --wait
#   docker compose --profile obs up -d
#   ./scripts/dashboard-demo.sh
#
# The scenario is three phases, because a dashboard that only shows the happy
# path does not say much about a durable execution engine:
#
#   1. steady queued load, with 5% of first step attempts failing, so the
#      retry and queue panels have something in them;
#   2. a batch started and then SIGKILLed mid-flight, leaving executions
#      orphaned with their checkpoints already durable;
#   3. a fresh executor over the same workflow ids, which recovers them and
#      serves their recorded steps from history instead of re-running them.
set -euo pipefail

cd "$(dirname "$0")/.."

DB="${DATABASE_URL:-postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable}"
GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
BIN="$(mktemp -d)/anchora-load"
RUN_ID="demo-$(date +%s)"

echo "building load generator"
go build -o "$BIN" ./cmd/anchora-load

started=$(( $(date +%s) * 1000 ))

echo
echo "phase 1/3: steady queued load with retries"
"$BIN" -database-url "$DB" -workflows 12000 -concurrency 16 -steps 5 \
  -queue -fail-rate 0.05 -error-rate 0.02 -metrics-addr :9095

echo
echo "phase 2/3: batch killed mid-flight"
"$BIN" -database-url "$DB" -workflows 600 -concurrency 64 -steps 5 \
  -step-sleep 300ms -warmup 0 -run-id "$RUN_ID" -metrics-addr :9095 \
  >/dev/null 2>&1 &
victim=$!
sleep 14
kill -9 "$victim" 2>/dev/null || true
wait "$victim" 2>/dev/null || true
echo "  SIGKILLed executor $victim"

echo
echo "phase 3/3: fresh executor recovers and replays"
# The lease has to expire before another executor may claim the orphans, so
# this phase is mostly waiting. -await because most of these ids are already
# terminal: their bodies will not run again, so there is nothing to count, and
# Handle.Result returns their recorded output immediately. -hold keeps
# /metrics up for the final scrapes.
"$BIN" -database-url "$DB" -workflows 600 -concurrency 64 -steps 5 -await \
  -step-sleep 300ms -warmup 0 -run-id "$RUN_ID" -metrics-addr :9095 -hold 50s &
survivor=$!
sleep 55

echo
echo "what the recovering executor did:"
curl -fsS localhost:9095/metrics \
  | grep -E '^anchora_(operations_total|recoveries_total|workflows_completed_total)' \
  | sed 's/^/  /'
wait "$survivor" 2>/dev/null || true

ended=$(( $(date +%s) * 1000 ))

echo
echo "rendering dashboard to docs/img/"
mkdir -p docs/img
shot() {
  local name=$1 path=$2 extra=$3
  local code
  code=$(curl -s -o "docs/img/${name}.png" -w '%{http_code}' --max-time 180 \
    "${GRAFANA}/render/${path}?orgId=1&from=${started}&to=${ended}&theme=dark&${extra}")
  echo "  docs/img/${name}.png  HTTP ${code}"
}
shot anchora-dashboard           "d/anchora-core/anchora-durable-execution"      "width=1500&height=1600&kiosk=true"
shot anchora-throughput          "d-solo/anchora-core/anchora-durable-execution" "panelId=20&width=1100&height=400"
shot anchora-checkpoint-latency  "d-solo/anchora-core/anchora-durable-execution" "panelId=10&width=1100&height=400"
shot anchora-live-vs-replay      "d-solo/anchora-core/anchora-durable-execution" "panelId=21&width=1100&height=400"
shot anchora-step-attempts       "d-solo/anchora-core/anchora-durable-execution" "panelId=31&width=1100&height=400"
shot anchora-recovery            "d-solo/anchora-core/anchora-durable-execution" "panelId=40&width=1100&height=400"
