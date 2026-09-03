# Anchora

A small, durable orchestrator for AI workflows — think "make several LLM calls that depend on each other, run them in the right order, pass outputs forward, and don't lose the job if the server restarts."

Anchora is a single Go binary. You describe a workflow as a list of steps with dependencies. Anchora validates it as a DAG, runs everything that is ready in parallel, lets later steps reference earlier outputs with `{{steps.<id>.output}}`, retries transient failures, and — when durable mode is on — persists the whole thing to PostgreSQL, queues it through Redis, and streams live progress over SSE. The actual model calls go through the Hugging Face Inference Providers router (`huggingfaceagent/agent.go:17`).

No UI, no scheduler, no auth layer. Just an HTTP API with two flavours: run now and get the answer back, or submit a job and poll/stream it.

## What Anchora actually does — in plain language

**The problem it solves:** LLM workflows are rarely one prompt. A typical flow is "research a topic → summarize it → turn it into a tweet → check tone". Each step depends on the last, some steps are independent and could run together, prompts need to carry forward prior outputs, providers fail intermittently, and long workflows shouldn't block an HTTP request or vanish on restart.

**What you give it:** A JSON array of steps. Each step has an `id`, an `agent` name (which maps to a configured Hugging Face model), a `prompt`, and an optional `depends_on`.

```json
{
  "steps": [
    {"id":"research","agent":"research","prompt":"Explain Go select."},
    {"id":"summary","agent":"research","depends_on":["research"],"prompt":"Summarize: {{steps.research.output}}"}
  ]
}
```

**What it does with it:**
1. Validates the graph — every dependency exists, no self-references, no duplicates, no cycles (`workflow.go:53`, `workflow.go:196`).
2. Runs in waves — every step whose dependencies have succeeded runs together (`workflow.go:93`, `workflow.go:180`), sorted by ID within each wave (`workflow.go:109`). Results are collected on a channel.
3. Renders prompts — `{{steps.research.output}}` is replaced with the real upstream output before the call (`workflow.go:228`, pattern at `workflow.go:226`). If the upstream hasn't succeeded, the step fails instead of hallucinating.
4. Retries sensibly — linear backoff `attempt * RetryDelay` (`workflow.go:170`), `MaxRetries` means retries *after* the first try (`workflow.go:154`). Transient `Retrying` state is in-memory only; what gets persisted is `pending`/`running`/`succeeded`/`failed`/`skipped` (`workflow.go:16`).
5. Skips what can't run — if a dependency fails, downstream steps are marked `Skipped` (`workflow.go:188`) rather than hanging. Orphaned steps end as `Skipped: "workflow did not run"`.
6. Optionally makes it durable — `POST /v1/jobs` validates the same way, writes the job to Postgres, appends a `job.queued` event, and pushes the ID to Redis. Background workers pick it up, run the same DAG, update Postgres after each step, and emit `job.running` / `step.completed` / `job.completed` events you can stream.

**Two modes, same workflow shape:**
- **Sync** (`POST /v1/workflows/run`) — blocks, returns `200` or `502` with partial results. Good for short, interactive calls.
- **Durable** (`POST /v1/jobs` → `202`, then `GET /v1/jobs/{id}` + `GET /v1/jobs/{id}/events` SSE) — survives restarts, can be scaled by running more Anchora instances against the same Postgres/Redis, streams progress every 500 ms (`httpapi/router.go:158`).

## Scope and boundaries

This helps decide quickly if Anchora fits:

**In scope:**
- DAG orchestration for LLM steps with concurrency and dependency-aware skipping.
- Prompt templating that passes prior outputs forward.
- Pluggable `Agent` interface (`workflow.go:27`) — today only the Hugging Face agent is wired, but any `Run(ctx, prompt) (string, error)` can be adapted.
- Sync and durable execution behind one binary and one config file.
- Postgres for jobs/steps/events, Redis for queueing, SSE for live updates.

**Out of scope (by design, today):**
- No authentication, rate limiting, CORS, or pagination — just `chi/middleware.RequestID` + `Recoverer` (`httpapi/router.go:36`).
- No visual editor, no cron, no complex branching/conditionals — only `depends_on`.
- No requeue/TTL or dead-letter queue — a crashed worker leaves its ID in the Redis `processing` list (`jobs/queue.go:14`) until manually cleared.
- Only the Hugging Face chat-completions router is implemented. No local models, no tool calling.
- No integration/E2E test harness — coverage is unit-level for workflow, router, and HF transport; `jobs.Store`/`jobs.Queue` need live infra.

## Architecture

```mermaid
flowchart LR
    Client -->|POST /v1/workflows/run or /v1/jobs| API[Chi API httpapi/router.go]
    API --> PG[(PostgreSQL workflow_* tables)]
    API --> Redis[(Redis ready/processing lists)]
    Redis --> Worker[RunWorker goroutines jobs/jobs.go:79]
    Worker --> DAG[Workflow.Run workflow.go:93]
    DAG --> HF[Hugging Face router.huggingface.co]
    Worker --> PG
    PG -->|Events poll 500ms| SSE[GET /v1/jobs/{id}/events]
```

## Repository layout

```
workflow.go              # DAG validation, concurrent ready-step execution, {{steps.<id>.output}} rendering, linear retries
workflow_test.go         # DAG, cycle, retry tests
httpapi/router.go        # Chi router: /healthz, /v1/workflows/run, and conditional /v1/jobs* routes
httpapi/router_test.go   # Single sync route test
jobs/jobs.go             # Service.Submit / Get / Events / RunWorker / runJob
jobs/store.go            # Postgres: workflow_jobs / workflow_steps / workflow_events + migrate
jobs/queue.go            # Redis: LPUSH / BRPopLPush (1s) / LRem with ready + processing lists
config/config.go         # YAML load, defaults, validation, env lookups
huggingfaceagent/agent.go # HF Inference Providers agent (router.huggingface.co/v1/chat/completions)
huggingfaceagent/agent_test.go # Router/transport mocks
cmd/anchora/main.go      # Entrypoint: build agents, optionally start store/queue/workers, serve Chi
config.yaml              # Checked-in config: async.enabled=false, agents={}
docker-compose.yml       # postgres:17-alpine + redis:7-alpine
Taskfile.yml             # task / task run / task test / task fmt / task check / services-up|down|logs
agenthttp/ einoagent/ src/anchora/ tests/  # empty placeholder directories, not wired into the build
```

## What is actually implemented (with pointers)

### 1. Workflow engine — `workflow.go`
Think of this as the core that doesn't know about HTTP or databases. It just knows how to run a DAG correctly.

- `NewWorkflow` (`workflow.go:53`) enforces the rules: at least one step, each step needs `id`/`prompt`/`Agent`, no duplicate IDs, every `depends_on` is real and not self-referential, and the graph has no cycles (Kahn's algorithm in `hasCycle` at `workflow.go:196`).
- `Run` (`workflow.go:93`) works in rounds: find all steps whose dependencies have succeeded (`workflow.go:180`), run that ready set concurrently via goroutines + buffered channel, call `Options.OnStepState` for each result, then mark anything whose dependency failed as `Skipped` (`workflow.go:188`). Results come back ordered as you defined them, with a `*WorkflowError` pointing at the first failure.
- Prompt rendering (`workflow.go:228`) swaps `{{steps.<id>.output}}` (`workflow.go:226`) for the actual upstream text (trimmed). If the upstream isn't available, the step errors cleanly.
- Retries (`workflow.go:154`) are deliberately simple: try, and on error wait `1*delay`, `2*delay`, ... (`workflow.go:170`) up to `MaxRetries`, respecting `ctx.Done()`. `Attempts` counts retries, not total tries.

### 2. HTTP API — `httpapi/router.go`
A thin, honest HTTP layer. It validates early and maps outcomes to status codes you'd expect.

- `NewRouter` (`httpapi/router.go:31`) always serves `GET /healthz` and `POST /v1/workflows/run`. `NewRouterWithJobs` (`httpapi/router.go:34`) adds `POST /v1/jobs`, `GET /v1/jobs/{id}`, `GET /v1/jobs/{id}/events` only when a `jobs.Service` is provided (i.e. `async.enabled: true`).
- Requests are capped at 1 MiB and `DisallowUnknownFields` (`httpapi/router.go:66`) — extra JSON keys are a `400`, not silently ignored. Unknown `agent` → `400`, bad DAG → `400`, workflow failure → `502` with partial `steps` so you can see what succeeded, other errors → `500`.
- Durable submit (`httpapi/router.go:99`) does the same validation then `202 Accepted` with the full `Job` body.
- Events (`httpapi/router.go:135`) verify the job exists, set `text/event-stream`, require `http.Flusher`, then poll `Service.Events(id, after)` every 500 ms (`httpapi/router.go:158`) and flush `id: <n>\nevent: <type>\ndata: <json>\n\n` frames. Note the current quirk: `Last-Event-ID` is parsed with `fmt.Sscan` (`httpapi/router.go:154`), so **you must send `Last-Event-ID: 0` on the first request** — an absent header returns `400 invalid Last-Event-ID`.

### 3. Durable jobs — `jobs/jobs.go`, `jobs/store.go`, `jobs/queue.go`
The "don't lose my work" part. Same DAG, but with persistence and a queue in between.

- `Submit` (`jobs/jobs.go:48`) resolves agent names, dry-runs `NewWorkflow` so bad graphs never get stored, mints a random 16-byte hex ID (`jobs/jobs.go:153`), writes `pending` rows for the job and each step in a Postgres transaction, emits `job.queued`, and `LPUSH`es the ID to Redis.
- `RunWorker` (`jobs/jobs.go:79`) is a tight loop: `BRPopLPush` with 1s timeout (`jobs/queue.go:29`) from `ready` to `processing`, `redis.Nil` means "nothing, try again", otherwise `runJob` then `LRem` Ack (`jobs/queue.go:31`).
- `runJob` (`jobs/jobs.go:100`) marks `running` + `job.running`, resolves agents again, hooks `OnStepState` to `UpdateStep` + `step.completed` events (via `context.Background()` at `jobs/jobs.go:115`), runs the workflow, bulk-updates final step states, then `succeeded`/`failed` + `job.completed`. The `fail` path emits `job.failed`.
- `Store` (`jobs/store.go:13`) via `pgxpool` auto-migrates (`jobs/store.go:28`) three tables: `workflow_jobs`, `workflow_steps` (with `ordinal` to preserve order, `depends_on JSONB`), `workflow_events` (`BIGSERIAL` + index on `(job_id, id)`). `SetStatus` stamps `started_at` for `running` and `finished_at` otherwise; `Events` is `id > after` ordered by `id`.
- `Queue` (`jobs/queue.go:14`) defaults `ready` to `anchora:jobs` and `processing` to `anchora:jobs:processing` when no name is given.

### 4. Config — `config/config.go`
YAML plus env indirection, with sensible defaults.

- `Load` (`config/config.go:38`) defaults `server.address` to `:8080` (`config/config.go:48`), validates that retries/workers/agent `model_id`/`max_tokens`/`timeout_ms` are non-negative.
- `DatabaseURL()`/`RedisURL()` (`config/config.go:66`) respect custom env var names, falling back to `DATABASE_URL`/`REDIS_URL`. `WorkerCount()` (`config/config.go:78`) turns `0` into `1`. Millisecond fields convert via `RetryDelay()` / `Timeout()`.

### 5. Hugging Face agent — `huggingfaceagent/agent.go`
A small adapter that speaks the HF Inference Providers chat API so the workflow engine doesn't have to.

- `New` (`huggingfaceagent/agent.go:36`) needs `Name` + `ModelID`, defaults `TokenEnv` to `HF_TOKEN`, errors if the env is empty, and reuses a supplied `HTTPClient` or builds one with the configured timeout. The `model_id` suffix like `:fastest` is passed through verbatim as `model` in JSON.
- `Run` (`huggingfaceagent/agent.go:74`) sends `[{system: instruction if set}, {user: prompt}]` (`huggingfaceagent/agent.go:60`) to `https://router.huggingface.co/v1/chat/completions` (`huggingfaceagent/agent.go:17`) with `Bearer` token, caps the response at 10 MiB (`huggingfaceagent/agent.go:98`), checks for 2xx, and returns `choices[0].message.content` or a provider `error.message`.

### 6. Entrypoint — `cmd/anchora/main.go`
Glue that reads the config and decides whether to run as a simple API or as a durable worker.

- Reads `-config` (default `config.yaml` at `cmd/anchora/main.go:16`), builds `httpapi.AgentRegistry` (fatal if any agent misconfigures), derives `httpapi.Options` from workflow retries.
- If `async.enabled`, connects Postgres and Redis, creates the `jobs.Service`, spawns `WorkerCount()` goroutines running `RunWorker(context.Background())` (`cmd/anchora/main.go:44`), and serves `NewRouterWithJobs`. Otherwise just serves the sync router.

## Run locally

Requires Go 1.25+, Docker, and a `HF_TOKEN` if you want real model calls.

```sh
docker compose up -d
export HF_TOKEN=hf_replace_me
export DATABASE_URL='postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable'
export REDIS_URL='redis://localhost:6379/0'
go run ./cmd/anchora -config config.yaml
```

The checked-in `config.yaml` ships with `async.enabled: false` and `agents: {}` so `go run` works without infra, but it can't execute anything until you add an agent.

To enable durable mode (`config/config.go:16`):

```yaml
server:
  address: ":8080"
workflow:
  max_retries: 2
  retry_delay_ms: 500
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

`workers: 0` becomes `1` (`config/config.go:78`). Tables are created automatically on first `NewStore` (`jobs/store.go:28`). Scale by running more Anchora processes pointed at the same Postgres and Redis — they share the same queue.

## Development commands

```sh
task                 # task test
task run             # go run ./cmd/anchora -config config.yaml
task run CONFIG=dev.yaml
task fmt             # gofmt -w
task check           # gofmt -l check + go test ./...
task services-up     # docker compose up -d
task services-down   # docker compose down
task services-logs   # docker compose logs -f
task test            # go test ./...
```

## API

All workflows use the same step shape:

```json
{
  "steps": [
    {"id":"research","agent":"research","prompt":"Explain Go select."},
    {"id":"summary","agent":"research","depends_on":["research"],"prompt":"Summarize: {{steps.research.output}}"}
  ]
}
```

| Method | Path | Status | Body |
|---|---|---|---|
| `GET` | `/healthz` | `200` | `{"status":"ok"}` |
| `POST` | `/v1/workflows/run` | `200` success, `502` workflow failed (with `steps`), `400` validation/unknown agent/bad JSON, `500` internal | `{"steps": StepResult[]}` |
| `POST` | `/v1/jobs` | `202` with `Job`, `400` on validation | `Job{ id, status, created_at, started_at?, finished_at?, steps, results }` — only when durable mode is on, otherwise `404` |
| `GET` | `/v1/jobs/{id}` | `200`, `404`, `500` | `Job` |
| `GET` | `/v1/jobs/{id}/events` | `200` SSE, `400` bad `Last-Event-ID`, `404`, `500` | `id/type/data` frames; poll 500 ms; events: `job.queued`, `job.running`, `step.completed`, `job.completed` (and `job.failed` on resolve failure) |

```sh
# Sync — wait for the answer
curl -X POST http://localhost:8080/v1/workflows/run -H 'Content-Type: application/json' -d @workflow.json

# Durable — submit, then follow along
curl -X POST http://localhost:8080/v1/jobs -H 'Content-Type: application/json' -d @workflow.json
curl http://localhost:8080/v1/jobs/<job-id>
curl -N -H 'Last-Event-ID: 0' http://localhost:8080/v1/jobs/<job-id>/events
```

## Configuration

| Field | Meaning | Default |
|---|---|---|
| `server.address` | Listen address | `:8080` |
| `workflow.max_retries` | Retries after first attempt | `0` |
| `workflow.retry_delay_ms` | Linear delay base (`attempt * delay`) | `0` |
| `async.enabled` | Mount `/v1/jobs*`, connect PG/Redis, start workers | `false` |
| `async.database_url_env` | Env var for Postgres URL | `DATABASE_URL` |
| `async.redis_url_env` | Env var for Redis URL | `REDIS_URL` |
| `async.queue_name` | Redis list name | `anchora:jobs` |
| `async.workers` | Goroutines per process (`0` → `1`) | `1` |
| `agents.<name>.model_id` | Required HF model ID (suffixes like `:fastest` preserved) | — |
| `agents.<name>.token_env` | Env var for HF token | `HF_TOKEN` |
| `agents.<name>.instruction` | System prompt | `""` |
| `agents.<name>.max_tokens` | `max_tokens` in HF request | `0` (omit) |
| `agents.<name>.timeout_ms` | HTTP client timeout | `0` (no timeout) |

## Tests

```sh
go test ./...
```

Today there are unit tests for the things that can be tested without infra: `workflow_test.go:24` (DAG, templating, cycle detection, retries) and `httpapi/router_test.go:17` + `huggingfaceagent/agent_test.go:15` (happy-path routing and HF transport mocked via `http.RoundTripper`). There are no integration tests for `jobs.Store`/`jobs.Queue` (they need a live Postgres/Redis) and no SSE/E2E test.

## Known gaps and next steps

- **SSE reconnection** — `GET /v1/jobs/{id}/events` returns `400` if `Last-Event-ID` is missing (`httpapi/router.go:154`). Send `Last-Event-ID: 0` on the first call; a browser `EventSource` that omits the header will not work as-is.
- **Ops** — no auth, CORS, rate limiting, pagination, or structured logging beyond `RequestID`/`Recoverer`. No requeue/TTL or dead-letter handling for the Redis `processing` list; a crashed worker orphans its job ID.
- **Execution details** — `OnStepState` DB writes use `context.Background()` (`jobs/jobs.go:115`), so they outlive cancellation. Ready-batch sorting (`workflow.go:109`) is per-wave by ID, not a global topological sort.
- **Housekeeping** — `.gitignore` is Python-oriented; Go artifacts aren't ignored. `agenthttp/`, `einoagent/`, `src/anchora/`, `tests/` are empty placeholders.

These are the obvious next improvements if you're taking Anchora further: fix the SSE header default, add requeue/timeout for the processing list, and add `store`/`queue` integration tests with `docker compose`.
