#!/usr/bin/env bash
# Drives every example in examples/ against a running Anchora so the metrics
# surface, and the dashboards built on it, have something to show: successful
# and failed steps, skipped steps, synchronous and durable runs, queue depth,
# and the 200/202/400/404/502 responses the API can produce.
#
#   scripts/demo-load.sh [seconds]        # default 600, API at :8080
#   API=http://host:8080 scripts/demo-load.sh 1800
#
# Anchora must be running with async enabled (config.local.yaml) and the agent
# sidecar listening, or the durable rounds are skipped.
set -u

API=${API:-http://localhost:8080}
DURATION=${1:-600}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
EXAMPLES=$ROOT/examples

post() { # post <file> <path>
  curl -s -o /dev/null -X POST "$API$2" -H 'Content-Type: application/json' -d @"$EXAMPLES/$1"
}

submit() { # submit <file>; echoes the job ID, empty if async is off
  curl -s -X POST "$API/v1/jobs" -H 'Content-Type: application/json' -d @"$EXAMPLES/$1" |
    python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("id",""))
except Exception: print("")'
}

async_enabled() {
  [ "$(curl -s -o /dev/null -w '%{http_code}' "$API/v1/cluster")" = "200" ]
}

# Successful traffic: a wave of independent steps, a four-step chain, a
# fan-out/fan-in diamond, all three concurrently so the step rate is a curve
# rather than a spike.
sync_round() {
  post parallel.json /v1/workflows/run &
  post chain.json /v1/workflows/run &
  post fanout.json /v1/workflows/run &
  wait
}

# Failure traffic: the unreachable agent exhausts its retries, which fails its
# step, skips the step downstream of it, and answers 502 with partial results.
failure_round() {
  post failure.json /v1/workflows/run
}

# Rejected traffic: a dependency cycle, an agent name that is not configured,
# and a malformed body are all 400; a job ID that was never issued is 404.
reject_round() {
  post cycle.json /v1/workflows/run
  curl -s -o /dev/null -X POST "$API/v1/workflows/run" -H 'Content-Type: application/json' \
    -d '{"steps":[{"id":"a","agent":"nope","prompt":"unknown agent"}]}'
  curl -s -o /dev/null -X POST "$API/v1/workflows/run" -H 'Content-Type: application/json' \
    -d '{"steps":[{"id":"a",'
  curl -s -o /dev/null "$API/v1/jobs/0000000000000000000000000000dead"
}

# Durable traffic: several jobs submitted back to back so the ready list has
# depth before the workers drain it, plus one job that fails for the
# failed-jobs panel. Each job is polled to a terminal state.
job_round() {
  local ids=() id
  for example in batch.json workflow.json fanout.json failure.json; do
    id=$(submit "$example")
    [ -n "$id" ] && ids+=("$id")
  done
  # Watch one job's event stream the way the README does.
  [ ${#ids[@]} -gt 0 ] && curl -s -m 60 -o /dev/null "$API/v1/jobs/${ids[0]}/events"
  for id in "${ids[@]}"; do
    for _ in $(seq 1 60); do
      case "$(curl -s "$API/v1/jobs/$id" |
        python3 -c 'import json,sys; print(json.load(sys.stdin).get("status",""))')" in
        succeeded | failed | dead_lettered) break ;;
      esac
      sleep 2
    done
  done
}

ASYNC=no
async_enabled && ASYNC=yes
echo "driving $API for ${DURATION}s (durable rounds: $ASYNC)"

deadline=$(($(date +%s) + DURATION))
round=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  round=$((round + 1))
  curl -s -o /dev/null "$API/healthz"
  sync_round
  [ $((round % 2)) -eq 0 ] && failure_round
  [ $((round % 3)) -eq 0 ] && reject_round
  if [ "$ASYNC" = yes ] && [ $((round % 2)) -eq 1 ]; then
    job_round
    curl -s -o /dev/null "$API/v1/cluster"
  fi
  curl -s -o /dev/null "$API/metrics"
  echo "round $round done ($(($deadline - $(date +%s)))s left)"
done

echo "--- final metrics ---"
curl -s "$API/metrics" | grep -v '^#'
