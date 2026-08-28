# Anchora

Anchora is a Go service for durable, dependency-aware AI workflows. It executes Hugging Face Inference Provider agents as DAG nodes, persists job state in PostgreSQL, dispatches work through Redis, and exposes status plus server-sent events.

## Capabilities

- DAG validation and concurrent execution of dependency-ready steps.
- Upstream output passing with `{{steps.<step-id>.output}}` prompt references.
- Linear retries with request-context propagation.
- PostgreSQL persistence for jobs, steps, results, and replayable events.
- Redis queue workers that can be scaled by running additional service instances.
- `GET /v1/jobs/{id}` live status and `GET /v1/jobs/{id}/events` SSE updates.
- Hugging Face model IDs, including routing or provider suffixes such as `:fastest`.

## Architecture

```mermaid
flowchart LR
    Client -->|POST /v1/jobs| API[Chi API]
    API --> PG[(PostgreSQL)]
    API --> Redis[(Redis queue)]
    Redis --> Worker[Workflow worker]
    Worker --> DAG[Anchora DAG runner]
    DAG --> HF[Hugging Face Inference Providers]
    Worker --> PG
    PG -->|events| SSE[SSE client]
```

## Run locally

Prerequisites: Go 1.24+, a Hugging Face `HF_TOKEN`, Docker, PostgreSQL, and Redis.

```sh
docker compose up -d
export HF_TOKEN=hf_replace_me
export DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable'
export REDIS_URL='redis://localhost:6379/0'
go run ./cmd/anchora -config config.yaml
```

Enable async mode and configure agents in `config.yaml`:

```yaml
async:
  enabled: true
  database_url_env: DATABASE_URL
  redis_url_env: REDIS_URL
  queue_name: anchora:jobs
  workers: 2

agents:
  research:
    model_id: HuggingFaceTB/SmolLM3-3B:fastest
    token_env: HF_TOKEN
    instruction: You are a concise research assistant.
    max_tokens: 1024
    timeout_ms: 30000
```

PostgreSQL tables are created on service startup. The checked-in configuration deliberately has no agents, so add one before submitting workflows.

## API

`POST /v1/workflows/run` runs a workflow synchronously. `POST /v1/jobs` submits the same request body for durable asynchronous execution and returns `202 Accepted`.

```json
{
  "steps": [
    {"id":"research","agent":"research","prompt":"Explain Go select."},
    {"id":"summary","agent":"research","depends_on":["research"],"prompt":"Summarize: {{steps.research.output}}"}
  ]
}
```

```sh
curl -X POST http://localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d @workflow.json
curl http://localhost:8080/v1/jobs/<job-id>
curl -N http://localhost:8080/v1/jobs/<job-id>/events
```

The SSE stream emits `job.queued`, `job.running`, `step.completed`, and `job.completed` events. It supports reconnection through the `Last-Event-ID` header.

## Configuration

| Field | Meaning |
| --- | --- |
| `workflow.max_retries` | Retry count after the initial model call. |
| `workflow.retry_delay_ms` | Base linear retry delay. |
| `async.enabled` | Enables Postgres, Redis workers, and job endpoints. |
| `async.workers` | Worker goroutines in this server process. |
| `agents.<name>.model_id` | Required Hugging Face model ID. |
| `agents.<name>.token_env` | Token variable; defaults to `HF_TOKEN`. |

## Test

```sh
go test ./...
```
