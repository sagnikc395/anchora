# anchora

<img src="./assets/logo.png" width="200px" height="200px" />

anchora runs small workflows made of dependent agent calls. As user describe the steps, Anchora checks that their dependencies form a valid DAG, runs steps that are ready at the same time, and makes successful outputs available to later steps.

Workflows can run in the request that started them, or they can be submitted as jobs backed by PostgreSQL and Redis
and executed by a pool of workers that survives losing any of its nodes.
Agents are the one abstraction that you deal with here : a small `Agent` interface in this repository, wired by the executable over
one HTTP call to a Python agent sidecar (`agent/`) that serves models from the Hugging Face ecosystem —
[transformers](https://github.com/huggingface/transformers) loading any public checkpoint, e.g. Qwen/Qwen3-0.6B, locally and token-free.

## Architecture at a glance

Two entry points, one engine:

```mermaid
flowchart TD
    Client([Client])

    subgraph API["HTTP API"]
        Run["POST /v1/workflows/run<br/>synchronous"]
        Jobs["POST /v1/jobs, GET /v1/jobs/:id<br/>plus /events and /cluster"]
    end

    Engine["Workflow engine<br/>DAG validation, ready-wave execution"]
    Agent["Agent interface<br/>Run(context, prompt) to output, error"]
    Provider["Python agent sidecar<br/>FastAPI + Hugging Face transformers"]

    subgraph Async["Durable jobs, optional"]
        Queue[("Redis<br/>ready list + lease set")]
        Store[("PostgreSQL<br/>jobs, steps, events")]
        Workers["Worker pool + reaper"]
    end

    Client --> Run
    Client --> Jobs
    Run -- inline --> Engine
    Jobs --> Store
    Jobs --> Queue
    Queue --> Workers
    Workers -- same engine --> Engine
    Workers -. heartbeat, renew lease .-> Store
    Engine --> Agent --> Provider
```

Read it top to bottom: a client request either runs the engine in the request
itself, or is stored and queued for a worker pool that runs that same engine and
returns results through PostgreSQL. Either way the engine only ever talks to the
`Agent` interface, so the provider behind it is swappable.

## A workflow

Each step has an `id`, an `agent`, a `prompt`, and optionally `depends_on`.
Prompts can include an earlier step's output using
`{{steps.<id>.output}}`:

```json
{
  "steps": [
    {
      "id": "research",
      "agent": "research",
      "prompt": "Explain Go select in a few sentences."
    },
    {
      "id": "summary",
      "agent": "research",
      "depends_on": ["research"],
      "prompt": "Summarize this:\n{{steps.research.output}}"
    }
  ]
}
```

Before execution, Anchora rejects empty steps, missing fields, duplicate step
IDs or dependencies, self-dependencies, unknown dependencies, cycles, and
negative retry settings. Steps in the same ready wave run concurrently, and
every prompt in a wave is rendered before the wave starts. A failed step skips
its whole downstream chain, not just its immediate dependents. A failed
workflow returns the results collected up to that point as well as the failing
step.

Retries are configured globally. `max_retries` counts attempts after the first
call, and the delay is linear: retry number × `retry_delay_ms`. Persisted step
states are the states represented in the job store (`pending`, `running`,
`succeeded`, `failed`, and `skipped`).

## How it fits together

The synchronous endpoint calls the workflow engine directly. The job endpoint
stores a validated job, places its ID on a Redis list, and returns. A worker
claims the ID from Redis, runs the same workflow engine, writes step and job
updates to PostgreSQL, and records events. The events endpoint polls those
stored events and exposes them as Server-Sent Events.

A claimed job is not removed from Redis. It moves into a lease set scored by a
deadline and tagged with the claiming worker, and PostgreSQL records the same
ownership on the job row. The owner renews both on a heartbeat; a worker that
dies stops renewing, and a reaper on any node returns the job to the ready
list.

Deliveries are therefore at-least-once, and a redelivery is cheap. Writes are
fenced: everything a worker writes while running a job is conditional on it
still holding the PostgreSQL lease, so a partitioned worker that has already
been replaced cannot overwrite the new owner's progress. A redelivered job
resumes instead of restarting, replaying the steps that already succeeded from
the store rather than calling their agents again, so recovering from a crash
costs only the work that was actually lost. And a job that keeps coming back is
dead-lettered after `async.max_attempts` deliveries instead of looping forever.

```mermaid
flowchart LR
    Client[Client]

    subgraph HTTP[Anchora HTTP API]
        Sync["POST /v1/workflows/run"]
        Submit["POST /v1/jobs"]
        Get["GET /v1/jobs/{id}"]
        Events["GET /v1/jobs/{id}/events"]
    end

    Engine["Workflow engine<br/>validate DAG, render prompts, run ready steps"]
    Adapter["remoteagent HTTP adapter<br/>POST /run, one JSON body"]
    Sidecar["Python agent sidecar<br/>FastAPI + Hugging Face transformers"]
    DB[("PostgreSQL<br/>jobs, steps, events, workers<br/>owner + lease per job")]
    Ready[("Redis ready list")]
    Leases[("Redis lease set<br/>job to deadline + owner")]
    Worker["Worker<br/>claim, heartbeat, run"]
    Reaper["Reaper<br/>sweep expired leases"]
    Hub["Hugging Face model hub<br/>Qwen/Qwen3-0.6B by default"]

    Client --> Sync
    Sync --> Engine
    Engine --> Adapter
    Adapter --> Sidecar
    Sidecar -- weights --> Hub
    Client --> Submit
    Submit --> DB
    Submit --> Ready
    Ready --> Worker
    Worker -- claim + renew --> Leases
    Worker -- claim + renew --> DB
    Worker --> Engine
    Reaper -- expired --> Leases
    Reaper -- requeue --> Ready
    Reaper -- orphans --> DB
    Client --> Get
    Get --> DB
    Client --> Events
    Events --> DB
```

Two reapers cover each other. The Redis sweep handles the ordinary case of a
worker dying. The PostgreSQL sweep handles jobs whose queue entry vanished
entirely (a Redis flush or a failover) and re-enqueues them; it waits one extra
lease period so the cheaper sweep goes first.

## Running it

Requirements: Go 1.25 or newer for the server, Python 3.13 for the agent
sidecar (any `uv` or pip), and Docker for the optional PostgreSQL and Redis
services. No API tokens: the sidecar serves a public Hugging Face checkpoint
locally, so the first start downloads the weights (~1.5 GB for Qwen3-0.6B).

Start the sidecar, which loads `Qwen/Qwen3-0.6B` into memory and listens on
`:8000`:

```sh
cd agent
uv venv && uv pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

The checked-in `config.yaml` starts the synchronous API and points one agent at
that sidecar. Satisfy yourself that both are healthy, then start the server:

```sh
curl http://localhost:8000/healthz   # {"status":"ok"}
go run ./cmd/anchora -config config.yaml
```

The server listens on `:8080` by default. A simple synchronous request looks
like this:

```sh
curl -X POST http://localhost:8080/v1/workflows/run \
  -H 'Content-Type: application/json' \
  -d @examples/workflow.json
```

`examples/workflow.json` holds the two-step workflow shown above. A real run
against local Qwen3-0.6B looks like this (trimmed):

```json
{
  "steps": [
    { "id": "research", "status": "succeeded",
      "output": "The `select` statement in Go is used to choose one of multiple channels or goroutines to execute. It allows for selecting a target from a set of possible options, which is particularly useful in concurrent programming scenarios." },
    { "id": "summary", "status": "succeeded",
      "output": "The `select` statement in Go selects one of multiple channels or goroutines to execute." }
  ]
}
```

### Durable jobs

Create a config with the job API enabled (the checked-in one must always load
without services, so async stays off by default):

```sh
cp config.yaml config.local.yaml   # gitignored
# flip async.enabled to true in config.local.yaml, then:

docker compose up -d --wait
export DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable'
export REDIS_URL='redis://localhost:6379/0'
go run ./cmd/anchora -config config.local.yaml
```

With async mode enabled, Anchora creates its tables on startup and starts the
configured number of worker goroutines (`workers: 0` is treated as one) plus a
reaper. Jobs are shared by every process pointed at the same PostgreSQL
database and Redis queue; run as many nodes as you like. Each worker registers
itself in `workflow_workers` and heartbeats, so `GET /v1/cluster` shows the
live roster and the queue depth.

Startup fails fast if `async.enabled` is set without both backend URLs, and
`SIGINT`/`SIGTERM` drains in-flight jobs: a worker shutting down releases its
claim and requeues the job, so a rolling restart does not wait out the full
visibility timeout.

Queue durability depends on Redis. The included compose file enables
append-only persistence with per-second fsync and gives both services a named
volume; the PostgreSQL-side reaper covers whatever a Redis restart still loses.

Submit and inspect a job:

```sh
curl -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d @examples/workflow.json

curl http://localhost:8080/v1/jobs/<job-id>
curl -N -H 'Last-Event-ID: 0' \
  http://localhost:8080/v1/jobs/<job-id>/events
```

`Last-Event-ID` is optional; send it to resume a dropped stream from the last
event you saw. The endpoint polls PostgreSQL every 500 ms and emits
`job.queued`, `job.running`, `job.resumed`, `step.completed`, `job.reclaimed`,
`job.abandoned`, `job.dead_lettered`, and `job.completed` events as they are
recorded, then closes once the job reaches a terminal state.

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

### In containers

The `Dockerfile` builds a static binary and copies it onto a distroless base, so
the image carries the binary, CA certificates, and nothing else, and runs as a
non-root user:

```sh
docker build -t anchora:local .
docker run --rm -p 8080:8080 \
  -v "$PWD/config.yaml:/app/config.yaml:ro" anchora:local
```

`config.yaml` is mounted rather than baked in, so a configuration change does
not need a rebuild. Point the agent at the sidecar container when Anchora runs
in a container (`url: http://agent:8000`); the compose `app` profile builds and
starts the sidecar alongside Anchora, PostgreSQL, and Redis, with the model
cache kept in a named volume so restarts do not re-download weights:

```sh
task app-up     # docker compose --profile app up -d --build --wait
```

Set `async.enabled: true` first via `config.local.yaml` (gitignored) and run
with `-config config.local.yaml` for the job API. In the `app` profile the
compose network renames to the service host, so point the agent at
`http://agent:8000`: `docker compose --profile app up -d agent` brings just the
sidecar up for a host-run Anchora, and `task app-up` runs the whole stack. The
service picks up a local `.env` if one exists. Without the profile,
`docker compose up` still starts only PostgreSQL and Redis, which is
what the integration tests want.

## Observability

Anchora serves Prometheus-format metrics at `GET /metrics` (always available):
responses by status, terminal step outcomes, workflow run results, jobs
submitted and jobs that reached a terminal state, and the queue depth gauges a
poller refreshes every 5 seconds when async mode is on. Logs go to stdout.

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
  sidecar can log into `logs/agent.log` the same way. Everything is scraped
  under `job="anchora"`; filter by `filename` if you need one process.

Metrics names are `anchora_*`: `anchora_http_responses_total{code}`,
`anchora_steps_total{status}`, `anchora_workflows_total{result}`,
`anchora_jobs_submitted_total`, `anchora_jobs_total{status}`,
`anchora_queue_ready`, `anchora_jobs_in_flight`, and `anchora_workers`.
Prometheus may need a first successful scrape before the dashboard shows data;
send one request through the API to see every series.

## HTTP API

All JSON request bodies use the workflow shape shown above. Request bodies are
limited to 1 MiB and unknown JSON fields are rejected.

| Method | Path | Available | Response |
| --- | --- | --- | --- |
| GET | `/healthz` | Always | `200` and `{"status":"ok"}` |
| GET | `/metrics` | Always | Prometheus text format |
| POST | `/v1/workflows/run` | Always | `200` with step results; `502` when a workflow step fails; `400` for invalid input; `500` for other errors |
| POST | `/v1/jobs` | Async mode | `202` with the created job; `400` for invalid input |
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
sidecar's `POST /run` takes `{"prompt", "instruction"}`, applies the chat
template with a Hugging Face transformers model, and returns `{"output"}`; an
empty completion, an HTTP error, or an unreachable service is a step failure,
which is what workflow retries are driven by. The `remoteagent` adapter
(`lib/remoteagent/agent.go`) does the client side: it validates the configured
URL at startup, sets the configured timeout, and reads error bodies up to 1 MiB.

Which model is served is the sidecar's own setting: `MODEL_NAME`
(`Qwen/Qwen3-0.6B` by default) and `MAX_NEW_TOKENS`, set in the compose service
or the environment. Adding an agent for a different routing target is
configuration, not code: another entry under `agents:` pointing at another
instance of the sidecar.

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

Unit tests cover the workflow engine (DAG validation, concurrent waves,
retries, skip cascades, and resume), every HTTP route including the SSE stream,
the agent-service adapter, and configuration. They live in `tests/`, a single
black-box package, and run against in-memory fakes, so they need no services.

The Lua scripts and SQL in `lib/jobs/store.go` and `lib/jobs/queue.go`
are the one part
the fakes cannot stand in for, so they are covered separately by tests that
need real servers. Those skip unless both URLs are set; `task test-integration`
starts the compose services and sets them for you:

```sh
task test-integration

# or, against your own instances (the Redis database given is written to):
ANCHORA_TEST_DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable' \
ANCHORA_TEST_REDIS_URL='redis://localhost:6379/1' \
  go test -race -count=1 -run Integration ./tests/
```


