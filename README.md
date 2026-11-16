# pgflow

A Postgres-native durable execution engine for Go. A workflow is a plain Go
function; every engine call it makes is checkpointed in Postgres before its
result is returned. If the process dies, the workflow is re-executed from the
top: recorded calls replay from the database without repeating side effects,
and execution resumes at the first call with no recorded result.

Postgres is the only infrastructure dependency. There is no coordinator
process, no message broker, and no external queue. This repository implements
[PLAN_V2.md](PLAN_V2.md): the durable-execution core plus queues, events,
streams, fork, the agent layer, a determinism linter, a CLI, telemetry, and a
completeness test suite. What is deliberately left out is listed under
[Scope](#scope).

Requires Go 1.25+ and Postgres 12+.

## Guarantees

| Property | How |
| --- | --- |
| The workflow is a row | `pgflow.workflow_status` plus its input/output in `pgflow.workflow_io` |
| A result is observed only after it is durable | every step writes `pgflow.operation_outputs` before returning |
| Replay is exact | one monotonic function-id counter per execution; each call reads its recorded slot first |
| Side effects are at-least-once | a crash between a live call and its checkpoint repeats that call |
| Transactional steps are exactly-once | the business writes and the checkpoint commit in one Postgres transaction |
| Two executors never both win | checkpoint inserts are fenced on the execution's epoch; first writer wins |
| A dead executor is recovered | leases expire, another executor claims with a higher epoch and replays |
| Divergent code fails loudly | recorded kind/name at a function slot must match what the code calls |

## Quick start

```go
eng, err := pgflow.New(ctx, pgflow.Config{
    DatabaseURL: os.Getenv("DATABASE_URL"),
})
if err != nil {
    log.Fatal(err)
}
defer eng.Shutdown(ctx)

// Register before Launch (Start launches the engine on first use).
var Research = pgflow.RegisterWorkflow(eng, "research_agent", researchAgent)

h, err := Research.Start(ctx, "Summarize X", pgflow.WithWorkflowID("req-123"))
if err != nil {
    log.Fatal(err)
}
report, err := h.Result(ctx)
```

The workflow id is the idempotency key: starting the same id again returns a
handle to the existing execution instead of running a duplicate.

## A workflow

```go
func researchAgent(ctx *pgflow.Context, task string) (Report, error) {
    plan, err := pgflow.Step(ctx, "plan", func(sc pgflow.StepCtx) (Plan, error) {
        return llm.Plan(sc, task) // runs once; the result is checkpointed
    }, pgflow.Retry(5, time.Second, 2), pgflow.Timeout(60*time.Second))
    if err != nil {
        return Report{}, err
    }

    notes, err := pgflow.Step(ctx, "search", func(sc pgflow.StepCtx) ([]Note, error) {
        return tools.Search(sc, plan.Queries)
    })
    if err != nil {
        return Report{}, err
    }

    approved, err := pgflow.Recv[bool](ctx, "approval", 24*time.Hour) // durable wait
    if err != nil {
        return Report{}, err
    }
    if !approved {
        return Report{}, ErrRejected
    }

    if err := pgflow.Sleep(ctx, 10*time.Minute); err != nil { // durable timer
        return Report{}, err
    }
    now, err := pgflow.Now(ctx) // recorded clock read
    if err != nil {
        return Report{}, err
    }
    return pgflow.Tx(ctx, "store", func(sc pgflow.StepCtx, tx pgflow.TxConn) (Report, error) {
        report := buildReport(notes, now)
        _, err := tx.Exec(sc, `INSERT INTO reports (id, body) VALUES ($1, $2)`, ctx.WorkflowID(), report.Body)
        return report, err
    })
}
```

## API

| Call | Purpose |
| --- | --- |
| `Step(ctx, name, fn, opts...)` | run an arbitrary side effect once and checkpoint its output |
| `Tx(ctx, name, fn, opts...)` | run business SQL and the checkpoint in one transaction |
| `Go(ctx, name, fn)` → `Future[T]` | start a step in parallel; deterministic slots in program order, `Wait` for the result |
| `Now` / `Random` / `UUID` | recorded replacements for nondeterministic reads |
| `Sleep(ctx, d)` | durable timer; replays sleep only the remaining duration |
| `Send(ctx, destID, topic, msg)` | send a durable message exactly once per step slot |
| `Recv[T](ctx, topic, timeout)` | consume a message and checkpoint it atomically |
| `SetEvent(ctx, key, v)` / `GetEvent[T](id, key, timeout)` | publish/observe key events; reads are recorded |
| `WriteStream(sc, key, chunk)` / `Engine.Stream` | append-only side channel for token streams and traces |
| `Child(ctx, workflow, input)` | start a child workflow with a deterministic id and await it |
| `Enqueue(ctx, workflow, input, opts...)` | enqueue a child onto a queue as a recorded operation |
| `ContinueAsNew(ctx, workflow, input)` | end this execution and carry state into a fresh one |
| `Patched(ctx, "name")` | branching into new behavior for new histories only |
| `pgflow.Fork(ctx, workflow, id, fromSlot)` | rerun a recorded workflow from a slot without repaying earlier steps |
| `Workflow.Start` / `Handle.Result` | start an execution and wait for its output |
| `Engine.RegisterQueue` | drain a named queue with concurrency, rate, and partition limits |
| `Engine.Cancel` | cancel a pending workflow anywhere in the fleet |
| `Engine.Workflow` / `Engine.ListWorkflows` / `Engine.WorkflowEvent` / `Engine.Stream` | inspect state for operators and UIs |

Step options: `Retry(maxAttempts, initialBackoff, factor)`, `Timeout(d)`,
`NonRetryable(func(error) bool)`, `HeartbeatTimeout(d)`, plus `StepOptions` for
`ScheduleToClose`, `MaxBackoff`, and jitter. `StepCtx` embeds
`context.Context`, exposes `Attempt()`, `IdempotencyKey()`, and
`Heartbeat(details)`. The heartbeat timeout cancels an attempt that stops
heartbeating, which catches a wedged activity rather than waiting for the
start-to-close timeout.

`TxConn` is the subset of `pgx.Tx` a transaction step receives (`Exec`,
`Query`, `QueryRow`), so the transaction is the real Postgres transaction that
receives the checkpoint.

## How a step executes

1. Reserve `function_id` from the execution's counter.
2. Look it up in the preloaded history. If present, verify the recorded kind and
   name, then return the recorded value or error. `fn` does not run.
3. Run `fn` with retries, timeouts, and heartbeats. Attempt rows go to
   `pgflow.step_attempts`.
4. Insert the checkpoint with the execution's epoch. The insert only happens if
   the workflow row still has that epoch, and `ON CONFLICT DO NOTHING` makes the
   first writer win.
5. If the insert lost, use the winner's recorded value. If the epoch was stale,
   abort with `ErrFenced`.

A cancelled or shut-down execution writes no terminal step checkpoint, so the
workflow stays resumable.

## Schema

All state lives in the `pgflow` schema, created by embedded migrations on
`Launch`:

| Table | Contents |
| --- | --- |
| `workflow_status` | one row per execution: name, status, app version, executor, lease, epoch, recovery attempts, queue and priority, partition key, deadline, parent, forked-from, continuation |
| `workflow_io` | inputs, output, error (split out so status updates do not rewrite payloads) |
| `operation_outputs` | the event log: one row per completed operation, keyed by `(workflow_id, function_id)` |
| `step_attempts` | live attempts, heartbeat time, and heartbeat details |
| `notifications` | durable messages between workflows, consumed transactionally |
| `workflow_events` | published key/value events (`SetEvent` / `GetEvent`) |
| `streams` | append-only chunked streams (`WriteStream`), tailed by `Engine.Stream` |
| `queues` | queue configuration: global and worker concurrency, rate limit, partition concurrency |

## Queues

A queue is a name over `ENQUEUED` rows. `WithQueue`, `WithPriority`, and
`WithPartition` tag the row; `Engine.RegisterQueue` starts draining it on this
executor:

```go
eng.RegisterQueue(ctx, pgflow.Queue{
    Name:                  "llm",
    LocalConcurrency:      4,               // per executor
    GlobalConcurrency:     20,              // across the fleet
    PartitionConcurrency:  2,               // per tenant, so one cannot starve the rest
    RateLimitCount:        100,
    RateLimitPeriod:       time.Minute,     // provider RPM budgets
})
```

The dequeue sweep claims rows oldest-first by partition and highest-priority
first, bumps the epoch, and dispatches them. Local enqueues kick the sweep
immediately; a poll tick covers enqueues from other processes. An enqueue made
from inside a workflow is a recorded operation with the child id derived from
its slot, so a replaying parent reattaches to the same queue row instead of
enqueueing a duplicate.

## Fork, patches, and continuations

- `pgflow.Fork(ctx, workflow, sourceID, fromSlot)` copies the source's history
  below the slot and runs live from there: rerun an agent from step 7 without
  repaying steps 1–6.
- `pgflow.Patched(ctx, "use-new-planner")` returns true for fresh executions
  (recording a marker) and false for replays of older history, letting you add
  behavior while old executions replay their old branch.
- `pgflow.ContinueAsNew(ctx, workflow, state)` ends the current execution and
  starts a fresh one with a fresh history; awaited handles follow the chain.
  Long-running agent loops call it before their history becomes unbounded.

## Testing

```sh
task check                 # gofmt check, go vet, go test -race ./...
go test -race ./...

# Postgres integration tests (they skip without a URL):
task test-integration      # starts docker compose, then runs everything
```

Unit tests run entirely on `pgflow/memory`, an in-memory `pgflow.Store` with
the same fencing and first-writer-wins rules. They cover replay and crash
recovery (including mid-`Sleep`, mid-`Recv`, and mid-parallel-`Go` crashes),
nondeterminism detection, zombie-epoch fencing, retries/timeouts/heartbeats,
children, messaging, events, streams, fork, patching, continuations, queue
concurrency caps and rate limits, enqueue idempotence across a restart, and
cancellation. The `agent` package is tested with a scripted provider, its
actual `Provider` interface.

The integration tests run the same scenarios against real Postgres —
transactional-step exactly-once, recovery after executor loss, epoch fencing,
Send/Recv, queues, fork, events and streams, continuations, migration
idempotency, enqueue idempotence across a restart, and error replay. They run
only when `PGFLOW_TEST_DATABASE_URL` is set.

## Replay testing

`pgflow/pgflowtest` replays a recorded history against changed code:

```go
history, _ := pgflowtest.History(ctx, store, "req-123")
if _, err := pgflowtest.Replay(t, newCode, "research_agent", history, input); err != nil {
    // ErrNondeterminism names the first slot where new code diverges.
}
```

Recorded steps return their stored values without running; the first slot
without a recording runs live. A history produced by the old code validates a
deploy before it ships.

## Benchmarks

Apple M4, in-memory store (the Postgres path adds a row write per step):

```
BenchmarkCheckpointOverhead/plain-function    0.24 ns/op      (baseline function call)
BenchmarkCheckpointOverhead/pgflow-step       1.8 µs/op       (engine + checkpoint write)
BenchmarkQueuedWorkflows                      ~2,100 jobs/s    local concurrency 8
```

Run with `go test -bench . ./pgflow/`.

## Determinism rules

Workflow bodies must make the same sequence of engine calls on every replay:

- use `pgflow.Now`, `pgflow.Random`, and `pgflow.UUID` instead of `time.Now`,
  `math/rand`, and direct id generation;
- use `pgflow.Sleep` instead of `time.Sleep`;
- wrap every network or database call in `Step` or `Tx`;
- do not spawn bare goroutines that make engine calls — engine calls are
  positional, so start them in program order.

A recorded call is matched by position, kind, and name. Running different code
against an existing history returns `ErrNondeterminism` naming both operations,
and fails the workflow instead of producing a wrong result.

Errors rehydrated from history keep the engine's sentinels matchable
(`errors.Is(err, pgflow.ErrTimeout)`, `ErrCancelled`, `ErrFenced`,
`ErrNondeterminism`). Other errors come back as `*pgflow.StepError` with the
original type name and message.

## Recovery and versioning

`Launch` runs a startup sweep, then a periodic sweep (`RecoveryInterval`) claims
pending workflows whose lease expired, bumps their epoch, and re-executes them
from history. Each executor renews its leases every lease/3; a workflow missing
from the renewal result was cancelled or taken over, so its run is cancelled.
`MaxRecoveryAttempts` moves a crash-looping workflow to `RETRIES_EXCEEDED`.

Recovery is gated on `AppVersion`: an executor only picks up workflows stamped
with its own version, so old and new binaries can run side by side during a
deploy. The default version hashes the registered workflow names and Go runtime
version; set `Config.AppVersion` explicitly to pin a build. The
[patching API](#fork-patches-and-continuations) covers code changes that must
apply to in-flight histories.

`Engine.Shutdown` cancels in-flight workflows and leaves them `PENDING`;
another executor takes them over when their lease expires, replaying every
completed step.

## Tooling

- `cmd/pgflow` — operator CLI: `list`, `get <id>`, `steps <id>`, `cancel`,
  `resume`, `fork <id> --from <n>`, `gc --older-than 30d`. Reads
  `DATABASE_URL`.
- `pgflow/pgflowvet` — a `go/analysis` linter that fails workflow functions
  that call `time.Now`, `math/rand`, `net/http`, `os` env lookups, direct UUID
  generation, or spawn bare goroutines. Step bodies (`StepCtx` first parameter)
  are exempt. Run: `go vet -vettool=$(which pgflowvet) ./...`.
- OpenTelemetry: a span per workflow execution and per step attempt with
  workflow id, function id, and attempt attributes; wire any `otel.Tracer`.
- Prometheus: checkpoint writes and durations, recoveries, dequeued counts,
  registered on the default registerer.

## AI agent layer

`pgflow/agent` builds durable agents on the engine: provider adapters for
OpenAI-compatible APIs (including Ollama) and Anthropic, a JSON-schema tool
registry, and a docker-backed `Sandbox` tool. `agent.Run` drives the
replay-safe loop — every model call and every tool invocation is a named
step — so a crash mid-loop resumes at the exact call without repeating paid
work:

```go
messages := []agent.Message{{Role: agent.RoleUser, Content: task}}
out, err := agent.Run(ctx, "agent", agent.RunConfig{
    Call:    agent.CallConfig{Provider: openai.New(os.Getenv("OPENAI_API_KEY"))},
    Model:   "gpt-4o-mini",
    System:  sys,
    Tools:   registry,
    Budget:  &agent.Budget{MaxTotalTokens: 200_000},
    MaxTurns: 8,
}, messages)
```

`agent.Call` wraps a single completion; `agent.Budget` fails the run
deterministically when tokens or cost exceed the cap, using recorded usage so
replays agree. `examples/research-agent` is a complete runnable example with
parallel search legs, an approval gate, and provider selection.

## Layout

```
pgflow/
  pgflow.go        Engine, Config, registry, execution, shutdown
  context.go       workflow context, function-id counter, checkpoint persistence
  step.go          Step, StepCtx, retries, timeouts, heartbeats
  tx.go            Tx steps
  primitives.go    Now, Random, UUID, Sleep
  concurrency.go   Go, Future
  queue.go         queues, Enqueue, dequeue sweep
  events.go        SetEvent, GetEvent
  stream.go        WriteStream, Engine.Stream
  fork.go          Fork
  patch.go         Patched
  continue.go      ContinueAsNew
  crash.go         PGFLOW_CRASH_AFTER fault hook
  metrics.go       Prometheus counters
  messaging.go     Send, Recv
  child.go         Workflow handles, Handle.Result, Child
  recovery.go      recovery sweep and lease renewal
  store.go         Store interface and persisted records
  store_postgres.go pgx implementation
  migrate.go       embedded migration runner
  migrations/      schema
  memory/          in-memory Store for tests
  agent/           provider adapters, tool registry, sandbox, agent loop, budgets
  pgflowvet/       determinism linter (go/analysis) + cmd/pgflowvet
  pgflowtest/      replay testing helpers
  serde/           codec interface (JSON default)
cmd/pgflow/        operator CLI
examples/          order (approval + timer), research-agent (durable agent)
```

## Scope

Everything in the plan is implemented except the items below:

- `pg_notify` triggers plus `LISTEN` waiters for messages and events —
  `Recv`/`GetEvent` poll instead; the plan lists polling as the accepted
  fallback.
- A read-only web UI — the CLI and `Engine.ListWorkflows` cover the same
  reads.
- Property-based crash-schedule testing (`rapid`/`gopter`) and a subprocess
  crash-fuzz harness. Crash injection exists (`PGFLOW_CRASH_AFTER` kills the
  process at a chosen checkpoint), and crash-restart behavior is covered by the
  shutdown-and-recover tests; the scaffold is in `pgflow/pgflowtest`. A
  Firecracker sandbox — `agent.Sandbox` runs Docker only.
- Deadline enforcement on queued rows (`deadline_at` is recorded; nothing
  acts on it yet).

Known trade-offs from the plan's §16 apply: external steps are
at-least-once (idempotency keys upgrade them where providers allow), in-process
execution means the workflow shares the process's fate with recovery bounded by
the lease, and every step costs at least one Postgres write.
