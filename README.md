# anchora

<img src="./assets/logo.png" width="200px" height="200px" />

anchora runs small workflows made of dependent agent calls. You describe the steps; Anchora validates that their dependencies form a DAG, runs the steps that are ready at the same time, and makes each successful output available to later steps.

A workflow either runs inside the request that started it, or is submitted as a durable job backed by PostgreSQL and Redis and executed by a pool of workers that survives losing any of its nodes. The only abstraction is a one-method `Agent` interface: the Go server wires named agents over HTTP to a Python sidecar (`agent/`) that serves a Qwen3 checkpoint locally and token-free — through [MLX](https://github.com/ml-explore/mlx-lm) on Apple Silicon or [transformers](https://github.com/huggingface/transformers) anywhere else — with every concurrent step batched into one decode. The default is Qwen/Qwen3-0.6B; the same setup serves the 4B, 8B, and 14B sizes on a 24 GB laptop ([docs/models.md](docs/models.md)), and [docs/performance.md](docs/performance.md) has the measurements and the knobs.

## Architecture

One engine, two entry points:

```mermaid
flowchart TD
    Client([Client])

    subgraph API["HTTP API"]
        Run["POST /v1/workflows/run<br/>synchronous"]
        Jobs["POST /v1/jobs, GET /v1/jobs/{id}<br/>plus /events and /cluster"]
    end

    Engine["Workflow engine<br/>validate DAG, render prompts, run ready waves"]
    Adapter["remoteagent HTTP adapter<br/>POST /run, one JSON body"]
    Sidecar["Agent sidecar<br/>FastAPI; MLX or transformers<br/>one model thread, batched decode"]
    Hub[("Model hub<br/>Qwen3 0.6B – 14B")]

    subgraph Durable["Durable jobs, optional"]
        Store[("PostgreSQL<br/>jobs, steps, events, workers<br/>owner + lease per job")]
        Queue[("Redis<br/>ready list + lease set")]
        Pool["Worker pool"]
        Reaper["Reaper"]
    end

    Client --> Run
    Client --> Jobs
    Run -- inline --> Engine
    Jobs --> Store
    Jobs --> Queue
    Queue --> Pool
    Pool -- same engine --> Engine
    Pool -. heartbeat, renew lease .-> Store
    Pool -. renew lease .-> Queue
    Reaper -- expired leases --> Queue
    Reaper -- re-enqueue orphans --> Store
    Engine --> Adapter --> Sidecar
    Sidecar -- weights --> Hub
```

A client request either runs the engine inside the request, or is recorded in PostgreSQL and queued on Redis for a worker pool that runs the same engine and writes results back. Either way the engine only ever talks to the `Agent` interface, so the sidecar behind it is swappable.

## A workflow

Each step has an `id`, an `agent`, a `prompt`, and optionally `depends_on`. Prompts can include an earlier step's output using `{{steps.<id>.output}}`. This is the two-step workflow checked in at `examples/workflow.json`:

```json
{
  "steps": [
    {
      "id": "research",
      "agent": "research",
      "prompt": "Explain Go's select statement in a few sentences."
    },
    {
      "id": "summary",
      "agent": "research",
      "depends_on": ["research"],
      "prompt": "Summarize this in one sentence:\n{{steps.research.output}}"
    }
  ]
}
```

Before execution, Anchora rejects empty steps, missing fields, duplicate step IDs or dependencies, self-dependencies, unknown dependencies, cycles, and negative retry settings. Execution is dataflow: a step starts the moment its own dependencies have succeeded, concurrently with whatever else is running, so a chain hanging off a fast step never waits for an unrelated slow one. Prompts are rendered and results recorded on a single scheduling goroutine. A failed step skips its whole downstream chain, not just its immediate dependents. A failed workflow returns the results collected up to that point as well as the failing step.

Retries are configured globally. `max_retries` counts attempts after the first call, and the delay is linear: retry number × `retry_delay_ms`. Every step the job store persists is in one of the stored states `pending`, `running`, `succeeded`, `failed`, or `skipped`.

### The examples

`examples/` holds one file per workflow shape, and every one of them runs against the checked-in agents (`research`, `reviewer`, `summarizer`, and `unreachable`, which points at a port nothing listens on):

| File | Shape | What it exercises |
| --- | --- | --- |
| `workflow.json` | two steps in a line | prompt templating from an upstream output |
| `chain.json` | four steps, three agents | a research → review → summarize handoff, with the last step reading two upstreams |
| `fanout.json` | one step, three in parallel, one join | a concurrent wave fanning out and back in |
| `parallel.json` | four independent steps | a single wave, every step running at once |
| `batch.json` | six parallel steps, then a join and a summary | a wider fan-out; submitted as a job it leaves real depth in the queue |
| `failure.json` | three steps, the middle one on `unreachable` | retries, a failed step, the skipped dependent behind it, and the 502 the sync API answers with |
| `cycle.json` | two steps that depend on each other | up-front validation: the API answers 400 and nothing runs |

Any of them can be posted to `/v1/workflows/run` to execute inside the request, or to `/v1/jobs` to run durably. `scripts/demo-load.sh` ([seconds], default 600) drives all of them in a loop — successful runs, failures, rejected submissions, durable jobs polled to completion — which is how the dashboards below are filled:

```sh
task demo-load             # or: DURATION=1800 task demo-load
```

## How it fits together

A claimed job is not removed from Redis. It moves into a lease set scored by a deadline and tagged with the claiming worker, and PostgreSQL records the same ownership on the job row. The owner renews both on a heartbeat; a worker that dies stops renewing, and a reaper on any node returns the job to the ready list.

Deliveries are therefore at-least-once, and a redelivery is cheap. Writes are fenced: everything a worker writes while running a job is conditional on it still holding the PostgreSQL lease, so a partitioned worker that has already been replaced cannot overwrite the new owner's progress. A redelivered job resumes instead of restarting, replaying the steps that already succeeded from the store rather than calling their agents again, so recovering from a crash costs only the work that was actually lost. And a job that keeps coming back is dead-lettered after `async.max_attempts` deliveries instead of looping forever.

Two reapers cover each other. The Redis sweep handles the ordinary case of a worker dying. The PostgreSQL sweep handles jobs whose queue entry vanished entirely (a Redis flush or a failover) and re-enqueues them; it waits one extra lease period so the cheaper sweep goes first.

## Running it

Requirements: Go 1.25 or newer for the server, Python 3.13 for the agent sidecar (`uv` or pip), and Docker for the optional PostgreSQL, Redis, and telemetry services. No API tokens: the sidecar serves a public Hugging Face checkpoint locally, so the first start downloads the weights (~1.5 GB for Qwen3-0.6B, 4.6 GB for the 4-bit Qwen3-8B).

Start the sidecar, which loads `Qwen/Qwen3-0.6B` into memory and listens on `:8000`. On an Apple Silicon Mac it picks the MLX runtime; elsewhere it uses PyTorch:

```sh
task agent-run                                       # creates agent/.venv on first use
MODEL=mlx-community/Qwen3-8B-4bit task agent-run     # a bigger model, same agents

# or by hand:
cd agent && uv venv && uv pip install -r requirements.txt
MODEL_NAME=Qwen/Qwen3-0.6B uvicorn main:app --host 0.0.0.0 --port 8000
```

[docs/models.md](docs/models.md) lists the Qwen3 sizes that fit, what each needs, and the backend to use for it.

The checked-in `config.yaml` starts the synchronous API and points one agent at that sidecar. Satisfy yourself that both are healthy, then start the server:

```sh
curl http://localhost:8000/healthz   # {"status":"ok","model":"Qwen/Qwen3-0.6B","backend":"mlx",...}
go run ./cmd/anchora -config config.yaml
```

The server listens on `:8080` by default. A synchronous request looks like this:

```sh
curl -X POST http://localhost:8080/v1/workflows/run \
  -H 'Content-Type: application/json' \
  -d @examples/workflow.json
```

and returns every step's status and output; **The whole stack in one tour** below shows a full transcript.

### Durable jobs

Create a config with the job API enabled (the checked-in one must always load without services, so async stays off by default):

```sh
cp config.yaml config.local.yaml   # gitignored
# flip async.enabled to true in config.local.yaml, then:

docker compose up -d --wait
export DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable'
export REDIS_URL='redis://localhost:6379/0'
go run ./cmd/anchora -config config.local.yaml
```

With async mode enabled, Anchora creates its tables on startup and starts the configured number of worker goroutines (`workers: 0` is treated as one) plus a reaper. Jobs are shared by every process pointed at the same PostgreSQL database and Redis queue; run as many nodes as you like. Each worker registers itself in `workflow_workers` and heartbeats, so `GET /v1/cluster` shows the live roster and the queue depth.

Startup fails fast if `async.enabled` is set without both backend URLs, and `SIGINT`/`SIGTERM` drains in-flight jobs: a worker shutting down releases its claim and requeues the job, so a rolling restart does not wait out the full visibility timeout.

Queue durability depends on Redis. The included compose file enables append-only persistence with per-second fsync and gives both services a named volume; the PostgreSQL-side reaper covers whatever a Redis restart still loses.

Submit and inspect a job:

```sh
curl -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d @examples/workflow.json

curl http://localhost:8080/v1/jobs/<job-id>
curl -N -H 'Last-Event-ID: 0' \
  http://localhost:8080/v1/jobs/<job-id>/events
```
`Last-Event-ID` is optional; send it to resume a dropped stream from the last event you saw. The endpoint polls PostgreSQL every 500 ms and emits `job.queued`, `job.running`, `job.resumed`, `step.completed`, `job.reclaimed`, `job.abandoned`, `job.dead_lettered`, and `job.completed` events as they are recorded, then closes once the job reaches a terminal state.

Inspect the cluster:

```sh
curl http://localhost:8080/v1/cluster
```

```json
{
  "queue_ready": 0,
  "queue_in_flight": 1,
  "workers": [
    {
      "id": "node-a-4711-1f2e3d4c",
      "hostname": "node-a",
      "pid": 4711,
      "queue": "anchora:jobs",
      "started_at": "2026-09-06T12:00:00Z",
      "last_heartbeat_at": "2026-09-06T12:04:58Z",
      "jobs_claimed": 12
    }
  ]
}
```

### The whole stack in one tour

Everything below is how this repository is demonstrated end to end — one shell sequence, seven checks, no API tokens. Run each command from the repository root in a second terminal after starting the server per **Running it**; steps 6–7 also need the telemetry stack (`task telemetry-up`):

```sh
# 1. Is the API up?
curl http://localhost:8080/healthz
# {"status":"ok"}

# 2. Two-step synchronous workflow through local Qwen3-0.6B (~1.5s)
curl -X POST http://localhost:8080/v1/workflows/run \
  -H 'Content-Type: application/json' -d @examples/workflow.json
# {"steps":[{"id":"research","status":"succeeded","output":"The `select`
#   statement in Go is used to choose one of multiple channels or goroutines...
#   ..."}, {"id":"summary","status":"succeeded","output":"The `select`
#   statement in Go selects one of multiple channels or goroutines to
#   execute."}]}

# 3. Submit the same workflow as a durable job; poll until it succeeds
JOB=$(curl -s -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' -d @examples/workflow.json | \
  python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])')
sleep 2 && curl -s http://localhost:8080/v1/jobs/$JOB | \
  python3 -c 'import json,sys; j=json.load(sys.stdin); print(j["status"], j["attempts"])'
# succeeded 1

# 4. Watch the job's event stream (5 events: queued → running → 2 steps → done)
curl -s "http://localhost:8080/v1/jobs/$JOB/events"
# id: 1  event: job.queued ... event: step.completed ... event: job.completed

# 5. Who is working?
curl http://localhost:8080/v1/cluster
# {"queue_ready":0,"queue_in_flight":0,"workers":[{"id":"…-<pid>-…",
#   "hostname":"musashi.local","jobs_claimed":1,...}]}

# 6. Prometheus-format metrics, scraped in place
curl http://localhost:8080/metrics | head -8
# anchora_http_responses_total{code="200"} 7
# anchora_http_responses_total{code="202"} 1
# anchora_jobs_in_flight 0 ...

# 7. Is the collection stack happy? Prometheus target, Grafana dashboard, Loki logs
curl -s 'http://localhost:9090/api/v1/targets' | \
  python3 -c 'import json,sys; print([(t["labels"]["job"], t["health"])
    for t in json.load(sys.stdin)["data"]["activeTargets"]])'
# [('anchora', 'up'), ('prometheus', 'up')]

curl -s http://localhost:3000/api/search | \
  python3 -c 'import json,sys; print([(d["uid"], d["title"])
    for d in json.load(sys.stdin)])'
# [('anchora-overview', 'Anchora')]

curl -s 'http://localhost:3100/loki/api/v1/query_range?query=%7Bjob%3D%22anchora%22%7D&since=1h&limit=5' \
  | head -20
# streams from ./logs/anchora.log and ./logs/agent.log
```

Then open the dashboard: **http://localhost:3000/d/anchora-overview** —
HTTP rates, step/job outcomes, queue depth, and the Loki log panel fill in
within one 10s refresh.

### In containers

The `Dockerfile` builds a static binary onto a distroless base, so the image carries the binary, CA certificates, and nothing else, and runs as a non-root user:

```sh
docker build -t anchora:local .
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/app/config.yaml:ro" anchora:local
```

`config.yaml` is copied into the image as a fallback and mounted over it by the command above, so a configuration change does not need a rebuild. The compose `app` profile builds and starts the sidecar alongside Anchora, PostgreSQL, and Redis, with the model cache in a named volume so restarts do not re-download weights. Inside the compose network the sidecar answers at `http://agent:8000`, so point a container-run Anchora's agent there; `docker compose --profile app up -d agent` brings up just the sidecar for a host-run Anchora:

```sh
task app-up     # docker compose --profile app up -d --build --wait
```

Set `async.enabled: true` first via `config.local.yaml` (gitignored) and run with `-config config.local.yaml` for the job API; the `anchora` service picks up a local `.env` if one exists. Without a profile, `docker compose up` still starts only PostgreSQL and Redis, which is what the integration tests want.

## Observability

Anchora serves Prometheus-format metrics at `GET /metrics` (always available): `anchora_http_responses_total{code}`, `anchora_steps_total{status}`, `anchora_workflows_total{result}`, `anchora_jobs_submitted_total`, `anchora_jobs_total{status}`, and the queue-depth gauges `anchora_queue_ready`, `anchora_jobs_in_flight`, and `anchora_workers`, refreshed by a poller every 5 s while async mode runs. Logs go to stdout.

The compose `telemetry` profile runs the collection stack:

```sh
task telemetry-up   # docker compose --profile telemetry up -d --wait
```

- **Prometheus** (`:9090`) scrapes the host-run server at
  `host.docker.internal:8080`, so it works whether Anchora runs via docker or
  `go run`.
- **Grafana** (`:3000`, anonymous viewer) provisions the **Anchora** dashboard:
  HTTP response rate by status, step outcomes, durable jobs submitted versus
  terminals, workflow stats, queue depth with the worker roster, and a live log
  panel.
- **Loki** (`:3100`) stores logs. **Promtail** tails `./logs/*.log`, the
  directory the server is expected to write to, so run it with
  `./bin/anchora -config config.local.yaml > logs/anchora.log 2>&1`; the agent
  sidecar can log into `logs/agent.log` the same way. (Sidecar *containers*
  log to `docker compose logs agent` instead.) Everything is scraped
  under `job="anchora"`; filter by `filename` if you need one process.

The captures below are from a live run of `scripts/demo-load.sh`: every example in `examples/` driven in a loop for 25 minutes through the local Qwen3-0.6B sidecar — successful synchronous runs, durable jobs, a workflow whose middle step fails and skips its dependent, and rejected submissions — so each panel has the series it is there to show. Regenerate them by starting the stack, running `task demo-load`, and screenshotting `http://localhost:3000/d/anchora-overview`.

![Anchora dashboard in Grafana](assets/grafana-dashboard.png)

Individual panels in closeup:

| HTTP responses /s | Step outcomes /s |
| --- | --- |
| ![HTTP responses](assets/grafana-http.png) | ![Step outcomes](assets/grafana-steps.png) |
| **Durable jobs /s** | **Queue depth / worker roster** |
| ![Durable jobs](assets/grafana-jobs.png) | ![Queue depth](assets/grafana-queue.png) |
| **Totals for the window** | **Anchora logs (Loki)** |
| ![Totals](assets/grafana-failed.png) | ![Logs panel](assets/grafana-logs.png) |

The same series straight from the Prometheus UI (`:9090/graph`), queried as `sum by (status) (increase(anchora_steps_total[10m]))`:

![Prometheus graph of step outcomes](assets/prometheus-graph.png)

Prometheus may need a first successful scrape before the dashboard shows data; send one request through the API to see every series.

The provisioned **Anchora** dashboard (`http://localhost:3000/d/anchora-overview`, anonymous viewer access, 10s auto-refresh) has one panel per signal. The rate panels use a 2-minute window, which is twelve scrapes wide and smooths the step function a low request rate would otherwise draw; the totals below them are what to read when traffic is sparse:

| # | Panel | Query | What it shows |
| --- | --- | --- | --- |
| 1 | HTTP responses /s by status | `sum by (code) (rate(anchora_http_responses_total[2m]))` | API traffic split by 200/202/400/404/502 |
| 2 | Step outcomes /s by status | `sum by (status) (rate(anchora_steps_total[2m]))` | Steps finishing, stacked: succeeded, failed, skipped |
| 3 | Durable jobs /s | `sum (rate(anchora_jobs_submitted_total[2m]))` and `sum by (status) (rate(anchora_jobs_total[2m]))` | Submissions against terminals; the gap is work still queued |
| 4 | Workflow runs (1h) | `sum (increase(anchora_workflows_total[1h]))` | Synchronous runs in the last hour |
| 5 | Step success rate (1h) | `100 * sum(increase(anchora_steps_total{status="succeeded"}[1h])) / clamp_min(sum(increase(anchora_steps_total[1h])), 1)` | Green above 95%, red below 80% |
| 6 | Failed jobs (1h) | `sum (increase(anchora_jobs_total{status="failed"}[1h]))` | Red stat panel; zero unless jobs fail |
| 7 | Queue depth and worker roster | `anchora_queue_ready`, `anchora_jobs_in_flight`, `anchora_workers` | Backlog, executing jobs, live worker count, drawn as a staircase |
| 8 | Steps by outcome (1h) | `sum by (status) (increase(anchora_steps_total[1h]))` | Bar gauge of step totals for the window |
| 9 | Responses by code (1h) | `sum by (code) (increase(anchora_http_responses_total[1h]))` | Bar gauge of response totals for the window |
| 10 | Anchora logs (Loki) | `{job="anchora"}` | Live log lines from the server and sidecar |

## HTTP API

All JSON request bodies use the workflow shape shown above. Request bodies are
limited to 1 MiB and unknown JSON fields are rejected.

| Method | Path | Available | Response |
| --- | --- | --- | --- |
| GET | `/healthz` | Always | `200` and `{"status":"ok"}` |
| GET | `/metrics` | Always | Prometheus text format |
| POST | `/v1/workflows/run` | Always | `200` with step results; `502` when a workflow step fails; `400` for invalid input; `500` for other errors |
| POST | `/v1/jobs` | Async mode | `202` with the created job; `400` for invalid input; `500` when the backend fails |
| GET | `/v1/jobs/{id}` | Async mode | `200` with the job, `404` if it does not exist, or `500` on a store error |
| GET | `/v1/jobs/{id}/events` | Async mode | `200` SSE, `404` if it does not exist, `400` for an unparseable `Last-Event-ID`, or `500` if streaming is unsupported |
| GET | `/v1/cluster` | Async mode | `200` with queue depth and the live worker roster, or `500` on a backend error |

Unknown agent names and invalid workflow graphs are reported as `400`.

## Agents

The core engine depends only on:

```go
type Agent interface {
    Run(context.Context, string) (string, error)
}
```

The executable wires named agents over HTTP to the Python agent sidecar. The
sidecar's `POST /run` takes `{"prompt", "instruction"}`, applies the model's
chat template, and returns `{"output", "usage"}`, where `usage` carries the
token counts, the finish reason, and the request's latency; an empty
completion, an HTTP error, or an unreachable service is a step failure, which
is what workflow retries are driven by. One thread owns the model and every
request goes through a queue in front of it, so concurrent steps are batched
into one decode rather than contending for the GPU; `GET /stats` on the
sidecar reports the decode rate, the widest batch seen, and the mean queue
wait. The `remoteagent` adapter
(`lib/remoteagent/agent.go`) does the client side: it validates the configured
URL at startup, sets the configured timeout, and reads error bodies up to 1 MiB.

`config.yaml` ships four named agents. `research`, `reviewer`, and `summarizer` all point at the same sidecar and differ only in their instruction, which is the cheap way to get distinct roles out of one served model; `unreachable` points at `http://127.0.0.1:9`, a port nothing listens on, so `examples/failure.json` can exercise retries, a failed step, and the skip cascade without breaking the real sidecar.

Which model is served, and how, is the sidecar's own setting: `MODEL_NAME`
(`Qwen/Qwen3-0.6B` by default), `BACKEND` (`mlx` on Apple Silicon,
`transformers` elsewhere, chosen automatically), `MAX_NEW_TOKENS`, and
`BATCH_SIZE`, set in the environment or the compose service. Every Qwen3 size
from 0.6B to 14B runs behind the same four agents on a 24 GB Mac; see
[docs/models.md](docs/models.md) for the matrix and
[docs/performance.md](docs/performance.md) for the knobs. Adding an agent for
a different routing target is configuration, not code: another entry under
`agents:` pointing at another instance of the sidecar, which is also how to
serve two models side by side.

## Configuration

```yaml
server:
  address: ":8080"
workflow:
  max_retries: 2
  retry_delay_ms: 500
async:
  enabled: false
  database_url_env: DATABASE_URL
  redis_url_env: REDIS_URL
  queue_name: anchora:jobs
  workers: 2
  lease_ms: 30000
  heartbeat_ms: 0
  max_attempts: 3
  reaper_interval_ms: 5000
  worker_ttl_ms: 0
  reclaim_batch: 100
  reaper: true
agents:
  research:
    url: http://localhost:8000
    instruction: You are a concise research assistant.
    timeout_ms: 60000
```

`server.address` defaults to `:8080`. Retry values, worker counts, and agent
`timeout_ms` must be non-negative. Every agent needs a `url` reachable at
startup. Database and Redis environment-variable names default to `DATABASE_URL`
and `REDIS_URL`. An agent's `timeout_ms` of zero leaves the HTTP client without
a timeout.

The distributed settings tune recovery:

| Key | Default | Meaning |
| --- | --- | --- |
| `lease_ms` | `30000` | Visibility timeout. A claimed job is handed to another worker if its owner stops heartbeating for this long. Set it above your slowest step's latency, or a healthy but slow job will be taken away mid-run. |
| `heartbeat_ms` | a third of `lease_ms` | Lease renewal interval. Must be shorter than `lease_ms`; startup rejects it otherwise. |
| `max_attempts` | `3` | Deliveries before a job is dead-lettered. `0` means unlimited. |
| `reaper_interval_ms` | `5000` | How often expired leases are swept and dead workers pruned. |
| `worker_ttl_ms` | four leases | How long a silent worker stays in the registry. |
| `reclaim_batch` | `100` | Jobs recovered per sweep. |
| `reaper` | `true` | Whether this node runs the sweep. Safe to leave on everywhere; the sweeps are atomic. |

Settings are validated at startup: a `heartbeat_ms` at or past `lease_ms` is
rejected outright, because a lease that expires before it is ever renewed would
have every job reclaimed mid-run.

## Development

```sh
task run           # go run ./cmd/anchora -config config.yaml (override with CONFIG=)
task agent-run     # start the sidecar natively (MODEL=, BACKEND=, MAX_NEW_TOKENS=, BATCH_SIZE=)
task bench         # time the example shapes and read the sidecar's decode rate (RUNS=, CONCURRENCY=)
task build         # build ./bin/anchora
task test          # go test -race ./...
task check         # gofmt check, go vet, and tests — what CI runs
task cover         # coverage profile plus an HTML report
task fmt
task vet
task tidy
task image         # docker build -t anchora:local .
task app-up        # docker compose --profile app up -d --build --wait
task app-down
task telemetry-up  # docker compose --profile telemetry up -d --wait
task telemetry-down
task services-up   # docker compose up -d --wait (PostgreSQL and Redis only)
task services-down
task services-logs
```

Unit tests cover the workflow engine (DAG validation, dataflow scheduling,
concurrent prompt rendering, retries, skip cascades, and resume), every HTTP route including the SSE stream,
the agent-service adapter, and configuration. They live in `tests/`, a single
black-box package, and run against in-memory fakes, so they need no services.

The Lua scripts and SQL in `lib/jobs/store.go` and `lib/jobs/queue.go` are the
one part the fakes cannot stand in for, so they are covered separately by tests
that need real servers. Those skip unless both URLs are set; `task
test-integration` starts the compose services and sets them for you:

```sh
task test-integration

# or, against your own instances (the Redis database given is written to):
ANCHORA_TEST_DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable' \
ANCHORA_TEST_REDIS_URL='redis://localhost:6379/1' \
  go test -race -count=1 -run Integration ./tests/
```
