#!/usr/bin/env bash
# Measures what the served model and the engine deliver, so a change to either
# can be compared with numbers rather than impressions:
#
#   1. sequential latency of the two-step examples/workflow.json
#   2. a fan-out (examples/batch.json: six parallel steps, a join, a summary)
#   3. CONCURRENCY copies of examples/parallel.json posted at once
#
# and, from the sidecar's own counters, completion tokens per second and the
# mean time a request waited before the model picked it up.
#
#   scripts/bench.sh [runs] [concurrency]       # default 3 runs, 4 concurrent
#   API=http://host:8080 AGENT=http://host:8000 scripts/bench.sh 5 8
set -u

API=${API:-http://localhost:8080}
AGENT=${AGENT:-http://localhost:8000}
RUNS=${1:-3}
CONCURRENCY=${2:-4}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
EXAMPLES=$ROOT/examples

now_ms() { python3 -c 'import time; print(int(time.time()*1000))'; }
stat() { # stat <field>: a sidecar counter, 0 when the sidecar has no /stats
  curl -sf "$AGENT/stats" | python3 -c "import json,sys; print(json.load(sys.stdin).get('$1', 0))" 2>/dev/null || echo 0
}

post() { # post <file>; prints the HTTP status
  curl -s -o /dev/null -w '%{http_code}' -X POST "$API/v1/workflows/run" \
    -H 'Content-Type: application/json' -d @"$EXAMPLES/$1"
}

timed() { # timed <label> <file> [copies]: runs copies concurrently, prints wall ms
  local label=$1 file=$2 copies=${3:-1} start end
  start=$(now_ms)
  for _ in $(seq 1 "$copies"); do post "$file" >/dev/null & done
  wait
  end=$(now_ms)
  echo "$((end - start))"
}

curl -sf "$AGENT/healthz" >/dev/null || { echo "sidecar not reachable at $AGENT"; exit 1; }
curl -sf "$API/healthz" >/dev/null || { echo "anchora not reachable at $API"; exit 1; }

echo "sidecar: $(curl -s "$AGENT/healthz" | python3 -c 'import json,sys; h=json.load(sys.stdin); print(h.get("model","?"), "via", h.get("backend","?"), "max_new_tokens", h.get("max_new_tokens","?"))')"
echo "warming up..."; post workflow.json >/dev/null

tokens0=$(stat completion_tokens); gen0=$(stat generation_seconds); start_all=$(now_ms)

printf '%-44s' "workflow.json x$RUNS (2 steps, sequential)"
total=0; for _ in $(seq 1 "$RUNS"); do ms=$(timed seq workflow.json); total=$((total + ms)); done
echo "mean $((total / RUNS)) ms"

printf '%-44s' "batch.json x$RUNS (6 parallel + join + summary)"
total=0; for _ in $(seq 1 "$RUNS"); do ms=$(timed fan batch.json); total=$((total + ms)); done
echo "mean $((total / RUNS)) ms"

printf '%-44s' "parallel.json x$CONCURRENCY concurrent ($((4 * CONCURRENCY)) steps)"
echo "$(timed par parallel.json "$CONCURRENCY") ms wall"

tokens1=$(stat completion_tokens); gen1=$(stat generation_seconds); end_all=$(now_ms)
python3 - "$tokens0" "$tokens1" "$gen0" "$gen1" "$start_all" "$end_all" <<'PY'
import sys
t0, t1, g0, g1, s, e = (float(x) for x in sys.argv[1:])
tokens, gen, wall = t1 - t0, g1 - g0, (e - s) / 1000
print(f"{'completion tokens':<44}{int(tokens)}")
print(f"{'model decode throughput':<44}{tokens / gen if gen else 0:.1f} tok/s (model busy {gen:.1f}s of {wall:.1f}s)")
PY
echo "mean queue wait: $(stat mean_queue_wait_ms) ms; max batch: $(stat max_batch)"
