# anchora

[![CI](https://github.com/sagnikc395/anchora/actions/workflows/ci.yml/badge.svg)](https://github.com/sagnikc395/anchora/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/go-1.25-00ADD8.svg)](go.mod)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Anchora runs small workflows made of dependent agent calls. You describe the
steps, Anchora checks that their dependencies form a valid DAG, runs steps that
are ready at the same time, and makes successful outputs available to later
steps.

It is a Go service with an HTTP API. Workflows can run in the request that
started them, or they can be submitted as jobs backed by PostgreSQL and Redis
and executed by a pool of workers that survives losing any of its nodes.
There is no UI, authentication, or scheduler. Agents are the one abstraction:
a small `Agent` interface in this repository, wired by the executable to an
Eino chat model that speaks to any OpenAI-compatible provider.

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
    Provider["Eino chat model<br/>OpenAI-compatible provider"]

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
call, and the delay is linear: retry number × `retry_delay_ms`. `Retrying` is an
in-memory execution state; persisted step states are the states represented in
the job store (`pending`, `running`, `succeeded`, `failed`, and `skipped`).

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
    HF["Eino chat model adapter<br/>OpenAI-compatible provider"]
    DB[("PostgreSQL<br/>jobs, steps, events, workers<br/>owner + lease per job")]
    Ready[("Redis ready list")]
    Leases[("Redis lease set<br/>job to deadline + owner")]
    Worker["Worker<br/>claim, heartbeat, run"]
    Reaper["Reaper<br/>sweep expired leases"]
    HFRouter["Provider endpoint<br/>router.huggingface.co by default"]

    Client --> Sync
    Sync --> Engine
    Engine --> HF
    HF --> HFRouter
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

Requirements: Go 1.25 or newer, or just Docker if you would rather not build
locally. Docker is otherwise only needed for the optional PostgreSQL and Redis
services. Real requests also need an API token for the provider you point an
agent at; Hugging Face's router is the default.

The checked-in `config.yaml` starts the synchronous API and has no agents
configured. Add at least one named agent before sending a workflow:

```yaml
agents:
  research:
    model_id: HuggingFaceTB/SmolLM3-3B:fastest
    base_url: https://router.huggingface.co/v1
    token_env: HF_TOKEN
    instruction: You are a concise research assistant.
    max_tokens: 1024
    timeout_ms: 30000
```

`base_url` is optional and defaults to Hugging Face's router. Point it at any
other OpenAI-compatible endpoint, with `token_env` naming that provider's key.

Then set the token and start the server. `.env.example` lists every variable
Anchora reads:

```sh
export HF_TOKEN=hf_replace_me
go run ./cmd/anchora -config config.yaml
```

The server listens on `:8080` by default. A simple synchronous request looks
like this:

```sh
curl -X POST http://localhost:8080/v1/workflows/run \
  -H 'Content-Type: application/json' \
  -d @examples/workflow.json
```

`examples/workflow.json` holds the two-step workflow shown above.

### Durable jobs

Set `async.enabled: true` and provide PostgreSQL and Redis URLs through the
configured environment variables. The included services can be started with:

```sh
docker compose up -d --wait
export DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable'
export REDIS_URL='redis://localhost:6379/0'
export HF_TOKEN=hf_replace_me
go run ./cmd/anchora -config config.yaml
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
docker run --rm -p 8080:8080 -e HF_TOKEN=hf_replace_me \
  -v "$PWD/config.yaml:/app/config.yaml:ro" anchora:local
```

`config.yaml` is mounted rather than baked in, so a configuration change does
not need a rebuild. To bring the whole stack up instead, use the compose `app`
profile, which starts Anchora with PostgreSQL and Redis and points it at both:

```sh
task app-up     # docker compose --profile app up -d --build --wait
```

Set `async.enabled: true` in `config.yaml` first if you want the job API; the
service picks up `HF_TOKEN` from a local `.env` if one exists. Without the
profile, `docker compose up` still starts only PostgreSQL and Redis, which is
what the integration tests want.

## HTTP API

All JSON request bodies use the workflow shape shown above. Request bodies are
limited to 1 MiB and unknown JSON fields are rejected.

| Method | Path | Available | Response |
| --- | --- | --- | --- |
| GET | `/healthz` | Always | `200` and `{"status":"ok"}` |
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

The executable wires named agents to an [Eino](https://github.com/cloudwego/eino)
OpenAI-compatible chat model. `base_url` defaults to Hugging Face's Inference
Providers router (`https://router.huggingface.co/v1`), so requests land on
`/chat/completions` there unless configured otherwise. Each generation sends the
configured system instruction, when present, followed by the rendered prompt as
a user message, and fails if the model returns no text. `model_id`,
`max_tokens`, `timeout_ms`, and the bearer token come from configuration and
the named environment variable.

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
agents: {}
```

`server.address` defaults to `:8080`. Retry values, worker counts, and agent
`max_tokens`/`timeout_ms` must be non-negative. Every agent needs a `model_id`.
`token_env` defaults to `HF_TOKEN` and `base_url` to
`https://router.huggingface.co/v1`; database and Redis environment-variable
names default to `DATABASE_URL` and `REDIS_URL`. An agent's `timeout_ms` of zero
leaves the model's HTTP client without a timeout, and `max_tokens` of zero omits
that field from the provider request.

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
task services-up   # docker compose up -d --wait (PostgreSQL and Redis only)
task services-down
task services-logs
```

Unit tests cover the workflow engine (DAG validation, concurrent waves,
retries, skip cascades, and resume), every HTTP route including the SSE stream,
the Eino model adapter, configuration, and the worker lifecycle (claim, resume,
duplicate delivery, lease loss, shutdown requeue, dead-lettering, and reaping).
They run against in-memory fakes, so they need no services.

The Lua scripts and SQL in `internal/jobs/store.go` and `internal/jobs/queue.go`
are the one part
the fakes cannot stand in for, so they are covered separately by tests that
need real servers. Those skip unless both URLs are set; `task test-integration`
starts the compose services and sets them for you:

```sh
task test-integration

# or, against your own instances (the Redis database given is written to):
ANCHORA_TEST_DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable' \
ANCHORA_TEST_REDIS_URL='redis://localhost:6379/1' \
  go test -race -count=1 -run Integration ./internal/jobs/
```

## License

MIT. See [LICENSE](LICENSE).

## Repository map

```text
cmd/anchora/main.go             executable entrypoint
internal/workflow/workflow.go   DAG validation and execution
internal/httpapi/router.go      HTTP routes and SSE polling
internal/jobs/jobs.go           submission, worker lifecycle, reaper
internal/jobs/store.go          PostgreSQL schema, leases, worker registry
internal/jobs/queue.go          Redis queue: atomic claim, lease, reclaim
internal/einoagent/agent.go     Eino OpenAI-compatible model adapter (Hugging Face by default)
internal/config/config.go       YAML and environment configuration
config.yaml                     the configuration the server starts with
Dockerfile                      static build on distroless, non-root
docker-compose.yml              PostgreSQL, Redis, and the app under the `app` profile
examples/workflow.json          a two-step request body for the curl examples
.github/workflows/ci.yml        build, vet, gofmt, tests, and the integration suite
```

Anchora ships as a binary, not as a library: everything but `cmd/anchora` lives
under `internal/`, so the HTTP API and `config.yaml` are the whole compatibility
surface and the packages behind them stay free to change.

