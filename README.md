# Anchora

Anchora runs small workflows made of dependent agent calls. You describe the
steps, Anchora checks that their dependencies form a valid DAG, runs steps that
are ready at the same time, and makes successful outputs available to later
steps.

It is a Go service with an HTTP API. Workflows can run in the request that
started them, or they can be submitted as jobs backed by PostgreSQL and Redis.
There is no UI, authentication, scheduler, or model abstraction beyond the
small `Agent` interface in this repository.

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
negative retry settings. Steps in the same ready wave run concurrently. A
failed dependency causes downstream steps to be skipped. A failed workflow
returns the results collected up to that point as well as the failing step.

Retries are configured globally. `max_retries` counts attempts after the first
call, and the delay is linear: retry number × `retry_delay_ms`. `Retrying` is an
in-memory execution state; persisted step states are the states represented in
the job store (`pending`, `running`, `succeeded`, `failed`, and `skipped`).

## How it fits together

The synchronous endpoint calls the workflow engine directly. The job endpoint
stores a validated job, places its ID on a Redis list, and returns. A worker
takes the ID from Redis, runs the same workflow engine, writes step and job
updates to PostgreSQL, and records events. The events endpoint polls those
stored events and exposes them as Server-Sent Events.

```mermaid
flowchart LR
    Client[Client]

    subgraph HTTP[Anchora HTTP API]
        Sync[POST /v1/workflows/run]
        Submit[POST /v1/jobs]
        Get[GET /v1/jobs/{id}]
        Events[GET /v1/jobs/{id}/events]
    end

    Engine[Workflow engine\nvalidate DAG, render prompts, run ready steps]
    HF[Hugging Face agent\nchat completions]
    DB[(PostgreSQL\njobs, steps, events)]
    Ready[(Redis ready list)]
    Processing[(Redis processing list)]
    Worker[Worker goroutine]
    HFRouter[router.huggingface.co]

    Client --> Sync
    Sync --> Engine
    Engine --> HF
    HF --> HFRouter
    Client --> Submit
    Submit --> DB
    Submit --> Ready
    Ready --> Worker
    Worker --> Processing
    Worker --> Engine
    Worker --> DB
    Client --> Get
    Get --> DB
    Client --> Events
    Events --> DB
```

## Running it

Requirements: Go 1.25 or newer. Docker is only needed for the optional
PostgreSQL and Redis services. Real requests also need a Hugging Face token.

The checked-in `config.yaml` starts the synchronous API and has no agents
configured. Add at least one named agent before sending a workflow:

```yaml
agents:
  research:
    model_id: HuggingFaceTB/SmolLM3-3B:fastest
    token_env: HF_TOKEN
    instruction: You are a concise research assistant.
    max_tokens: 1024
    timeout_ms: 30000
```

Then set the token and start the server:

```sh
export HF_TOKEN=hf_replace_me
go run ./cmd/anchora -config config.yaml
```

The server listens on `:8080` by default. A simple synchronous request looks
like this:

```sh
curl -X POST http://localhost:8080/v1/workflows/run \
  -H 'Content-Type: application/json' \
  -d @workflow.json
```

### Durable jobs

Set `async.enabled: true` and provide PostgreSQL and Redis URLs through the
configured environment variables. The included services can be started with:

```sh
docker compose up -d
export DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable'
export REDIS_URL='redis://localhost:6379/0'
export HF_TOKEN=hf_replace_me
go run ./cmd/anchora -config config.yaml
```

With async mode enabled, Anchora creates its three tables on startup and starts
the configured number of worker goroutines (`workers: 0` is treated as one).
Jobs are shared by processes using the same PostgreSQL database and Redis
queue.

Submit and inspect a job:

```sh
curl -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d @workflow.json

curl http://localhost:8080/v1/jobs/<job-id>
curl -N -H 'Last-Event-ID: 0' \
  http://localhost:8080/v1/jobs/<job-id>/events
```

The current events endpoint requires a parseable `Last-Event-ID` header,
including on the first request. It polls PostgreSQL every 500 ms and emits
`job.queued`, `job.running`, `step.completed`, `job.completed`, or
`job.failed` events as they are recorded. A job moved into Redis's processing
list is not requeued automatically if its worker stops.

## HTTP API

All JSON request bodies use the workflow shape shown above. Request bodies are
limited to 1 MiB and unknown JSON fields are rejected.

| Method | Path | Available | Response |
| --- | --- | --- | --- |
| GET | `/healthz` | Always | `200` and `{"status":"ok"}` |
| POST | `/v1/workflows/run` | Always | `200` with step results; `502` when a workflow step fails; `400` for invalid input; `500` for other errors |
| POST | `/v1/jobs` | Async mode | `202` with the created job; `400` for invalid input |
| GET | `/v1/jobs/{id}` | Async mode | `200` with the job, `404` if it does not exist, or `500` on a store error |
| GET | `/v1/jobs/{id}/events` | Async mode | `200` SSE, `404` if it does not exist, `400` for an invalid `Last-Event-ID`, or `500` if streaming is unsupported |

Unknown agent names and invalid workflow graphs are reported as `400`.

## Agents

The core engine depends only on:

```go
type Agent interface {
    Run(context.Context, string) (string, error)
}
```

The executable currently wires named agents to the Hugging Face Inference
Providers endpoint at `https://router.huggingface.co/v1/chat/completions`.
Each request contains the configured system instruction, when present, and
the rendered prompt as a user message. `model_id`, `max_tokens`, and the bearer
token are taken from configuration and the configured environment variable.

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
agents: {}
```

`server.address` defaults to `:8080`. Retry values and agent
`max_tokens`/`timeout_ms` must be non-negative. `token_env` defaults to
`HF_TOKEN`; database and Redis environment-variable names default to
`DATABASE_URL` and `REDIS_URL`. An agent's `timeout_ms` of zero leaves the
underlying HTTP client without a timeout, and `max_tokens` of zero omits that
field from the provider request.

## Development

```sh
go test ./...
task test
task check
task fmt
```

The tests cover the workflow engine, the synchronous router, and the Hugging
Face transport with mocks. PostgreSQL/Redis storage and queue behavior are not
covered by integration tests in this repository.

## Repository map

```text
workflow.go                DAG validation and execution
httpapi/router.go          HTTP routes and SSE polling
jobs/jobs.go               job submission and workers
jobs/store.go              PostgreSQL schema and persistence
jobs/queue.go              Redis ready/processing lists
huggingfaceagent/agent.go  Hugging Face adapter
config/config.go           YAML and environment configuration
cmd/anchora/main.go        executable entrypoint
```
