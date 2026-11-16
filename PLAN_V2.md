# Plan: A Postgres-Native Durable Execution Engine in Go for AI Agents

Working name: `anchora` (rename freely). Goal: a Go library, modeled on DBOS Transact, that embeds durable execution directly into an application process using Postgres as the only infrastructure dependency. No separate orchestrator server, no message broker.

---

## 1. Design Principles

1. **Library, not a service.** Workflows run inside the user's Go binary. Postgres is the source of truth for all execution state.
2. **The database row is the workflow.** Every workflow is a row in `workflow_status`; every completed step is a row in `operation_outputs`. Anything you can query, you can debug, recover, fork or cancel.
3. **Checkpoint before proceeding.** A step's output is durably committed before the workflow is allowed to observe it. This is the write-ahead property.
4. **Replay is cheap and exact.** Re-running a workflow from the top is a sequence of primary-key lookups until the first missing checkpoint.
5. **Determinism is enforced, not hoped for.** The engine detects when replayed code diverges from the recorded history and fails loudly.
6. **At-least-once side effects, exactly-once observation.** External calls (LLMs, APIs) may run more than once around a crash; the workflow will only ever see one recorded result. Postgres transaction steps get true exactly-once.

---

## 2. Core Concepts Mapped to Implementation

| Primitive | Implementation in this engine |
|---|---|
| Event log / WAL | `operation_outputs` table keyed by `(workflow_id, function_id)`; inserted before control returns to workflow code |
| Workflow identity | `workflow_status` row with id, name, inputs, status, executor, version |
| Deterministic replay | Monotonic `function_id` counter per workflow execution; each step call looks up its slot first |
| Workflows | Plain Go functions `func(ctx *wf.Context, in I) (O, error)` that may only do deterministic work plus engine calls |
| Activities / steps | `wf.Step(ctx, name, fn, opts...)` wrapping arbitrary code with retries, timeouts, heartbeats |
| Recovery | On startup and periodically, claim PENDING workflows whose lease expired and re-execute them |

---

## 3. System Schema (Postgres)

All tables live in a dedicated schema (`anchora`). Migrations are embedded in the binary (`embed.FS` + a tiny migration runner, or `golang-migrate`).

```sql
CREATE SCHEMA IF NOT EXISTS anchora;

-- One row per workflow execution
CREATE TABLE anchora.workflow_status (
  workflow_id        TEXT PRIMARY KEY,           -- user-supplied (idempotency) or UUIDv7
  workflow_name      TEXT NOT NULL,              -- registered function name
  status             TEXT NOT NULL,              -- ENQUEUED | PENDING | SUCCESS | ERROR | CANCELLED | RETRIES_EXCEEDED
  app_version        TEXT NOT NULL,              -- hash of registered workflow code / build id
  executor_id        TEXT,                       -- worker currently owning it
  lease_expires_at   TIMESTAMPTZ,                -- ownership lease, renewed by heartbeat
  epoch              BIGINT NOT NULL DEFAULT 0,  -- fencing token, bumped on every claim
  recovery_attempts  INT NOT NULL DEFAULT 0,
  queue_name         TEXT,
  priority           INT NOT NULL DEFAULT 0,
  parent_workflow_id TEXT,
  parent_function_id INT,
  forked_from        TEXT,
  deadline_at        TIMESTAMPTZ,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at         TIMESTAMPTZ
);
CREATE INDEX ON anchora.workflow_status (status, lease_expires_at);
CREATE INDEX ON anchora.workflow_status (queue_name, status, priority, created_at)
  WHERE status = 'ENQUEUED';

-- Inputs/outputs separated so frequent status updates do not rewrite large blobs
CREATE TABLE anchora.workflow_io (
  workflow_id TEXT PRIMARY KEY REFERENCES anchora.workflow_status ON DELETE CASCADE,
  inputs      BYTEA NOT NULL,
  output      BYTEA,
  error       JSONB
);

-- The event log: one row per completed step / durable operation
CREATE TABLE anchora.operation_outputs (
  workflow_id    TEXT NOT NULL REFERENCES anchora.workflow_status ON DELETE CASCADE,
  function_id    INT  NOT NULL,                -- position in the workflow's step sequence
  function_name  TEXT NOT NULL,                -- used for nondeterminism detection
  kind           TEXT NOT NULL,                -- STEP | TX | SLEEP | RECV | SEND | CHILD | NOW | RAND | SET_EVENT
  output         BYTEA,
  error          JSONB,                        -- serialized terminal error (also replayed)
  attempts       INT NOT NULL DEFAULT 1,
  started_at     TIMESTAMPTZ,
  completed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  child_workflow_id TEXT,
  PRIMARY KEY (workflow_id, function_id)
);

-- In-flight step tracking (observability + heartbeats), deleted/ignored once checkpointed
CREATE TABLE anchora.step_attempts (
  workflow_id   TEXT NOT NULL,
  function_id   INT  NOT NULL,
  attempt       INT  NOT NULL,
  executor_id   TEXT NOT NULL,
  heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  heartbeat_details BYTEA,
  PRIMARY KEY (workflow_id, function_id, attempt)
);

-- Durable messaging (signals into a workflow, e.g. human approval)
CREATE TABLE anchora.notifications (
  message_id     TEXT PRIMARY KEY,             -- dedupe key for exactly-once send
  destination_id TEXT NOT NULL,
  topic          TEXT NOT NULL DEFAULT '',
  payload        BYTEA NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  consumed_at    TIMESTAMPTZ
);
CREATE INDEX ON anchora.notifications (destination_id, topic, created_at) WHERE consumed_at IS NULL;

-- Key/value events published by a workflow (progress, partial results)
CREATE TABLE anchora.workflow_events (
  workflow_id TEXT NOT NULL,
  key         TEXT NOT NULL,
  value       BYTEA NOT NULL,
  PRIMARY KEY (workflow_id, key)
);

-- Append-only streams (LLM token streams, agent traces) readable by clients
CREATE TABLE anchora.streams (
  workflow_id TEXT NOT NULL,
  stream_key  TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  chunk       BYTEA NOT NULL,
  PRIMARY KEY (workflow_id, stream_key, seq)
);

-- Queue configuration
CREATE TABLE anchora.queues (
  name             TEXT PRIMARY KEY,
  global_concurrency INT,
  worker_concurrency INT,
  rate_limit_count INT,
  rate_limit_period INTERVAL
);
```

Postgres triggers on `notifications` and `workflow_events` call `pg_notify` so waiters wake via `LISTEN` instead of polling (polling remains as a fallback).

---

## 4. Public Go API (target shape)

```go
eng, _ := anchora.New(ctx, anchora.Config{
    DatabaseURL: os.Getenv("DATABASE_URL"),
    AppVersion:  "", // default: hash of registered workflow names + build info
    ExecutorID:  "", // default: hostname-pid-random
})

// Registration happens before Launch so recovery can find functions by name
var ResearchAgent = anchora.RegisterWorkflow(eng, "research_agent", researchAgent)

eng.Launch() // runs migrations, starts recovery loop, queue pollers, listeners

h, _ := ResearchAgent.Start(ctx, "Summarize X", anchora.WithWorkflowID("req-123"))
out, err := h.Result(ctx)
```

Workflow code:

```go
func researchAgent(ctx *anchora.Context, task string) (Report, error) {
    plan, err := anchora.Step(ctx, "plan", func(sc anchora.StepCtx) (Plan, error) {
        return llm.Plan(sc, task) // non-deterministic: runs once, output checkpointed
    }, anchora.Retry(5, time.Second, 2.0), anchora.Timeout(60*time.Second))
    if err != nil { return Report{}, err }

    var notes []Note
    for _, q := range plan.Queries {            // deterministic loop over recorded data
        n, err := anchora.Step(ctx, "search", func(sc anchora.StepCtx) (Note, error) {
            return tools.WebSearch(sc, q)
        })
        if err != nil { return Report{}, err }
        notes = append(notes, n)
    }

    // Human-in-the-loop: durable wait, survives restarts
    ok, err := anchora.Recv[bool](ctx, "approval", 24*time.Hour)
    if err != nil || !ok { return Report{}, ErrRejected }

    anchora.Sleep(ctx, 10*time.Minute) // durable timer
    now := anchora.Now(ctx)            // recorded clock read
    return anchora.Step(ctx, "write", func(sc anchora.StepCtx) (Report, error) {
        return llm.Write(sc, notes, now)
    })
}
```

Generics give type-safe steps (`Step[T]`), messages (`Recv[T]`), and events (`GetEvent[T]`).

---

## 5. The Execution Algorithm

### 5.1 Starting a workflow
1. Generate or accept `workflow_id`. The id is the idempotency key.
2. `INSERT ... ON CONFLICT (workflow_id) DO NOTHING RETURNING` into `workflow_status` (status PENDING, executor_id, lease, epoch+1) and `workflow_io` in one transaction.
3. If the row already existed: if terminal, return its stored result; if pending and owned by a live executor, return a handle that polls/listens for completion.
4. Run the function in a goroutine with a fresh `Context{workflowID, epoch, nextFunctionID: 0}`.

### 5.2 Executing a step (the write-ahead core)
```
fid := ctx.nextFunctionID; ctx.nextFunctionID++

row := SELECT function_name, output, error FROM operation_outputs
       WHERE workflow_id=$1 AND function_id=$2

if row exists:
    if row.function_name != name -> return NondeterminismError (fatal)
    return decode(row.output), decode(row.error)        // REPLAY: no side effect

// LIVE execution
insert step_attempts row (for heartbeat visibility)
result, err := runWithRetries(fn, policy, timeout)       // side effect happens here

// CHECKPOINT (fenced)
INSERT INTO operation_outputs (...) VALUES (...)
  ON CONFLICT (workflow_id, function_id) DO NOTHING
  WHERE EXISTS (SELECT 1 FROM workflow_status
                WHERE workflow_id=$1 AND epoch=$ctx.epoch)   // implement via CTE
RETURNING ...

if conflict: another executor already recorded this slot -> read and use THEIR result
if epoch check failed: we are a zombie -> abort this goroutine (ErrFenced)
return result
```

Key points:
- The workflow never sees a value that is not already durable.
- First writer wins on `(workflow_id, function_id)`, so two racing executors converge on one history.
- Optimization: batch the "read history" step by loading all `operation_outputs` for the workflow once at the start of recovery into an in-memory map, instead of one SELECT per step.

### 5.3 Transaction steps (exactly-once)
`anchora.Tx(ctx, name, func(tx pgx.Tx) (T, error))` runs the user's SQL and the `operation_outputs` insert in the same Postgres transaction. Either both commit or neither does. This is the transactional outbox pattern and is the one place you get true exactly-once semantics.

### 5.4 Completing a workflow
Single transaction: update `workflow_status.status` to SUCCESS/ERROR, write `workflow_io.output`, guarded by the epoch fence. Notify waiters with `pg_notify`.

### 5.5 Recovery
- **Startup recovery:** select PENDING workflows where `executor_id = me` (restart of same identity) or `lease_expires_at < now()`.
- **Continuous recovery (multi-worker):** a loop claims expired leases:
  ```sql
  UPDATE anchora.workflow_status SET executor_id=$me, lease_expires_at=now()+$lease,
         epoch=epoch+1, recovery_attempts=recovery_attempts+1
  WHERE workflow_id IN (
    SELECT workflow_id FROM anchora.workflow_status
    WHERE status='PENDING' AND lease_expires_at < now() AND app_version=$myVersion
    ORDER BY lease_expires_at LIMIT $n FOR UPDATE SKIP LOCKED)
  RETURNING workflow_id, workflow_name, epoch;
  ```
- Each live executor renews leases for its running workflows in one batched UPDATE every `lease/3`.
- `recovery_attempts > maxRecoveryAttempts` moves the workflow to RETRIES_EXCEEDED (poison-pill protection: a workflow that crashes the process every time should not crash-loop the fleet).
- Only recover workflows whose `app_version` matches the running binary (see versioning).

---

## 6. Determinism Enforcement

Workflow code must produce the same sequence of engine calls on every replay. Provide replacements for every common source of nondeterminism, each implemented as a recorded operation:

| Forbidden in workflow code | Engine replacement |
|---|---|
| `time.Now()` | `anchora.Now(ctx)` (recorded) |
| `rand.*`, `uuid.New()` | `anchora.Random(ctx)`, `anchora.UUID(ctx)` (recorded) |
| `time.Sleep` | `anchora.Sleep(ctx, d)`: records the absolute wake time on first execution; on replay sleeps only the remaining duration |
| network / DB calls | `anchora.Step` / `anchora.Tx` |
| reading env/config that may change | wrap in a step |
| bare `go func()` | `anchora.Go(ctx, fn)` returning a future; function ids are reserved at spawn time in program order so concurrent steps get deterministic slots |
| map iteration order feeding step order | lint rule; sort keys |

Detection layers:
1. **Runtime:** `function_name` mismatch on replay returns `NondeterminismError`, marks workflow ERROR with a clear diff (expected vs actual step at index N).
2. **Static:** a `go/analysis` linter (`anchoravet`) that flags `time.Now`, `math/rand`, `net/http`, `os.Getenv`, and raw `go` statements inside functions registered as workflows. This is a strong resume differentiator.
3. **Replay tests:** `anchoratest.Replay(t, workflowFn, historyFromProdDB)` re-runs a recorded history against new code and reports divergence before deploy.

---

## 7. Activities: Retries, Timeouts, Heartbeats

```go
type StepOptions struct {
    MaxAttempts     int
    InitialBackoff  time.Duration
    BackoffFactor   float64
    MaxBackoff      time.Duration
    Jitter          bool
    NonRetryable    func(error) bool    // e.g. 4xx from LLM provider, schema validation errors
    StartToClose    time.Duration       // per-attempt timeout via context.WithTimeout
    ScheduleToClose time.Duration       // total budget across attempts
    HeartbeatTimeout time.Duration
}
```

- **Retries** run inside the live executor; only the final outcome (success or terminal error) is checkpointed. Attempt count is stored for observability.
- **Timeouts** are enforced with `context.WithTimeout`; step functions receive a `StepCtx` that embeds `context.Context`, so well-behaved code (HTTP clients, LLM SDKs) cancels cleanly.
- **Heartbeats:** `sc.Heartbeat(details)` updates `step_attempts.heartbeat_at` (debounced, e.g. at most once per second). If heartbeats go stale beyond `HeartbeatTimeout`, the step's context is cancelled and the attempt counts as failed. Heartbeat details are passed to the next attempt so long-running activities (e.g. a 20-minute sandbox job or a batch embedding run) can resume from progress.
- **Idempotency keys:** each step exposes `sc.IdempotencyKey()` = `workflowID:functionID`. Pass it to providers that support it (payments, email APIs) to upgrade at-least-once to effectively-once.
- **Error serialization:** terminal errors are stored as `{type, message, retryable, details}` and rehydrated on replay so `errors.Is/As` keep working for registered error types.

---

## 8. Queues and Concurrency Control

Queues are just `workflow_status` rows with `status='ENQUEUED'` and a `queue_name`.

Dequeue loop per queue:
```sql
WITH candidates AS (
  SELECT workflow_id FROM anchora.workflow_status
  WHERE queue_name=$q AND status='ENQUEUED' AND app_version=$v
  ORDER BY priority DESC, created_at
  LIMIT $freeSlots
  FOR UPDATE SKIP LOCKED)
UPDATE anchora.workflow_status w SET status='PENDING', executor_id=$me,
       started_at=now(), lease_expires_at=now()+$lease, epoch=epoch+1
FROM candidates c WHERE w.workflow_id=c.workflow_id
RETURNING w.workflow_id, w.workflow_name, w.epoch;
```

- **Worker concurrency:** local semaphore.
- **Global concurrency:** count PENDING rows for the queue inside the same transaction (take an advisory lock on the queue name to avoid races).
- **Rate limiting:** count rows with `started_at > now() - period`. Critical for LLM provider RPM/TPM limits.
- **Partitioned queues:** add a `partition_key` (e.g. per tenant or per user) with per-partition concurrency so one tenant's agent swarm cannot starve others.
- Enqueueing from inside a workflow is itself a recorded operation, so the child is enqueued exactly once.

---

## 9. Composition Primitives

- **Child workflows:** `anchora.Child(ctx, wf, input)`; child id is deterministic (`parentID/functionID`) so replay reattaches to the same child instead of spawning a duplicate.
- **Send / Recv:** `Send(ctx, destID, topic, msg)` uses `message_id = senderWorkflowID:functionID` for exactly-once delivery. `Recv` consumes the oldest unconsumed message in a transaction that also writes the checkpoint, so a message is never lost or double-consumed.
- **Events:** `SetEvent(ctx, key, v)` / `GetEvent[T](id, key, timeout)` for publishing status and partial results to external callers.
- **Streams:** `WriteStream(ctx, key, chunk)` appends; clients tail with `LISTEN`. Used for LLM token streaming while the durable checkpoint stores only the final assembled response.
- **Cancellation:** set status CANCELLED; the executor's lease renewal sees it and cancels the workflow's root context; the next engine call returns `ErrCancelled`.
- **Fork / time travel:** `Fork(workflowID, fromFunctionID)` creates a new workflow copying `operation_outputs` with `function_id < N`, then runs live from N. For agents this means: rerun an agent from step 7 with a fixed prompt without repaying for steps 1 to 6.

---

## 10. Versioning (the hard problem)

Replaying old histories against changed code is the main source of production bugs in durable execution systems.

1. **Version pinning (default):** every workflow row stores `app_version`. Executors only recover workflows matching their own version. During deploys, old and new binaries run side by side until old workflows drain.
2. **Patching API (advanced):** `if anchora.Patched(ctx, "use-new-planner") { ... } else { ... }`. The first live execution records the patch marker; replays of histories without the marker take the old branch.
3. **Admin tooling:** list pending workflows per version; force-migrate or cancel stragglers.

---

## 11. AI-Agent Layer (built on top of the core)

Keep this as a separate package (`anchora/agent`) so the core stays generic.

- **`agent.LLMCall`**: a step wrapper that records model, prompt hash, token usage, latency, cost, and the full response. Provider adapters for OpenAI, Anthropic, and a local model (Ollama) behind one interface.
- **Agent loop as a workflow:** `for !done { resp := LLMCall; for each tool_call: Step(tool) }`. Every iteration is replay-safe; a crash mid-loop resumes at the exact tool call.
- **Tool registry:** tools declared with JSON schema; each tool invocation is a step with its own retry policy (search tools retry, code execution maybe not).
- **Sandboxed code execution activity:** Docker or Firecracker runner with heartbeats and hard timeouts.
- **Budgets and guardrails:** per-workflow token and dollar budget tracked in a recorded counter; exceeding it fails the workflow deterministically.
- **Human-in-the-loop:** approval gates built on `Recv` with timeouts and escalation.
- **Multi-agent:** supervisor workflow spawning child agent workflows on a partitioned queue, aggregating results.
- **Context window management:** large tool outputs stored once in a blob table and referenced by hash in later prompts, keeping `operation_outputs` rows small.

---

## 12. Observability and Tooling

- OpenTelemetry: one span per workflow, child spans per step with attributes (function_id, attempt, replayed=true/false).
- Prometheus metrics: steps/sec, checkpoint latency p50/p99, replay length, recovery count, queue depth, lease expirations.
- `anchora` CLI: `list`, `get <id>`, `steps <id>`, `cancel`, `resume`, `fork <id> --from N`, `gc --older-than 30d`.
- Optional small web UI (read-only) rendering a workflow timeline from `operation_outputs`.
- Garbage collection: retention policy deleting terminal workflows older than N days, in batches.

---

## 13. Testing Strategy (this is where the project proves itself)

1. **Unit tests** for replay logic with an in-memory store implementing the same `Store` interface.
2. **Integration tests** against real Postgres via `testcontainers-go`.
3. **Crash injection:** an env-driven fault hook `anchora_CRASH_AFTER=<step_name>:<before|after_checkpoint>` that calls `os.Exit(1)`. A harness starts the process, kills it at random points, restarts it, and asserts:
   - every workflow eventually reaches a terminal state,
   - final outputs equal those of an uninterrupted run,
   - each `Tx` step's business write happened exactly once,
   - each external step ran at most `k` times (count with a counter server).
4. **Concurrency tests:** two executors racing on the same workflow; zombie executor resuming after its lease was stolen (fencing must reject its writes). Run everything under `-race`.
5. **Property-based tests** (`rapid` or `gopter`): generate random workflow shapes (loops, branches, children, sleeps) and random crash schedules.
6. **Determinism tests:** intentionally modify workflow code and assert a `NondeterminismError` with a useful diff.
7. **Benchmarks:** steps/sec per worker, checkpoint overhead vs. a plain function call, queue throughput with N workers, recovery time for 10k pending workflows.

---

## 14. Repository Layout

```
anchora/
  cmd/anchora/            CLI
  engine.go              New, Launch, Shutdown, registry
  context.go             workflow Context, function id counter, fencing epoch
  step.go                Step, Tx, retries, timeouts, heartbeats
  primitives.go          Now, Random, UUID, Sleep, Go, Patched
  messaging.go           Send, Recv, SetEvent, GetEvent, streams
  queue.go               queues, dequeue loop, rate limits
  recovery.go            lease renewal, claim loop
  store/
    store.go             Store interface
    postgres/            pgx implementation, migrations (embed.FS)
    memory/              in-memory store for unit tests
  serde/                 codec interface (JSON default, gob/protobuf optional)
  agent/                 LLM adapters, tool registry, agent loop, budgets
  anchoravet/             go/analysis determinism linter
  anchoratest/            replay test helpers, crash harness
  examples/
    research-agent/
    coding-agent-sandbox/
    human-approval/
  bench/
```

Dependencies: `jackc/pgx/v5` (pool, LISTEN/NOTIFY, COPY), `testcontainers-go`, `go.opentelemetry.io/otel`, `prometheus/client_golang`, `golang.org/x/tools/go/analysis`.

---

## 15. Milestones

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 (week 1) | Schema, migrations, Store interface, pgx pool, registry | `Start` persists a workflow; tables inspectable in psql |
| 1 (weeks 2 to 3) | Step checkpointing, replay, function-id counter, nondeterminism detection, workflow completion | Crash-injection test passes for linear workflows |
| 2 (week 4) | Recovery with leases + epoch fencing, multi-executor, `Tx` steps | Two-executor race and zombie tests pass under `-race` |
| 3 (week 5) | Retries, timeouts, heartbeats, error serialization, durable Sleep, Now/Random/UUID | Heartbeat-timeout and retry tests pass |
| 4 (week 6) | Child workflows, Send/Recv, events, streams, cancellation, `anchora.Go` | Human-approval example survives restarts mid-wait |
| 5 (week 7) | Queues, concurrency limits, rate limits, partitions | Benchmark: N workers draining 100k jobs, no double execution |
| 6 (week 8) | Agent package: LLM adapters, tools, budgets, sandbox activity, fork | Research agent demo: kill -9 mid-run, resumes without repeating paid LLM calls |
| 7 (week 9) | Versioning (pinning + Patched), linter, CLI, OTel, metrics, GC | Linter catches seeded violations; dashboards show step timelines |
| 8 (week 10) | Benchmarks, docs, comparison write-up vs DBOS/Temporal | Published README with numbers and architecture diagram |

---

## 16. Known Trade-offs to Document Honestly

- External steps are at-least-once: a crash after an LLM call returns but before the checkpoint commits will repeat that call. Mitigation: idempotency keys where supported; accept the cost for LLMs.
- Postgres is the throughput ceiling: every step is at least one write. Mitigations: batch history reads, use a separate system database, keep payloads small (blob table), consider `synchronous_commit` tuning only with clear warnings.
- In-process execution means a workflow and the process share fate; recovery latency is bounded by lease duration.
- Long histories make replay slow. Provide a `ContinueAsNew` primitive that ends the current workflow and starts a fresh one with carried-over state, for long-running agent loops.

---

## 17. Resume Framing (fill with real measured numbers)

- Built a Postgres-native durable execution engine in Go providing crash-safe workflows via step-level checkpointing, deterministic replay, and lease-based recovery with epoch fencing.
- Implemented exactly-once transactional steps (outbox pattern) and at-least-once activities with retries, timeouts and heartbeats; validated with randomized crash-injection and multi-executor race tests.
- Shipped a static analyzer (`go/analysis`) that detects nondeterministic calls in workflow code, plus replay testing against production histories.
- Built an AI agent runtime on top: resumable LLM tool loops, durable human-approval gates, token budgets, and fork-from-step debugging; benchmarked X steps/sec and Y ms p99 checkpoint latency.

---

## References (verified)

- DBOS system tables documentation: https://docs.dbos.dev/explanations/system-tables
- DBOS Go programming guide: https://docs.dbos.dev/golang/programming-guide
- DBOS Go source: https://github.com/dbos-inc/dbos-transact-golang
- "Why Your Workflows Should Be Postgres Rows" (DBOS blog): https://www.dbos.dev/blog/why-workflows-should-be-postgres-rows
- Supabase on running DBOS workflows in Postgres: https://supabase.com/blog/durable-workflows-in-postgres-dbos
- Original DBOS research paper (CIDR 2022, peer reviewed): https://stanford.edu/~askiad/files/dbos-cidr.pdf
