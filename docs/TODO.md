# TODO — what stands between Anchora and a full product

Grounded in the current codebase (`main` @ 9f83879). Items are ordered by priority
within each section. "P0 = blocks shipping to other users; P1 = expected by any
production adopter; P2 = nice to have." By-design non-goals are listed at the bottom so
they don't get re-litigated.

## 1. Security (P0 — the API is wide open)

- [ ] **Authentication/authorization on every route.** No route checks anything; anyone
  who can reach the port can submit jobs that spend HF tokens. Minimum: bearer-token
  middleware on `/v1/*`, ideally per-agent-key quotas later.
- [ ] **CORS policy.** Not configurable, not even `*`; browsers can't talk to it today.
- [ ] **TLS / behind-proxy guidance.** Server is plain HTTP; document or add TLS
  options. SSE + `Last-Event-ID` reconnect behavior through proxies is untested ground.
- [ ] **Rate limiting / admission control.** Nothing bounds submissions; a client can
  fill the Redis queue and the DB with failed jobs at request rate.
- [ ] **Prompt/output privacy.** Prompts and outputs (possibly user data) are stored
  raw in `workflow_steps` forever; no redaction, no retention policy, no download/wipe
  path. Merge with §5 retention below.

## 2. Job lifecycle API (P0 — the API is write/read-only)

- [ ] **List jobs** — `GET /v1/jobs` with status filter + pagination. Only fetch-by-ID
  exists; there is no way to see what's in the system.
- [ ] **Cancel a running job** — no `DELETE /v1/jobs/{id}` or equivalent. A runaway job
  can only be stopped by killing workers or exhausting `max_attempts`.
- [ ] **Retry/re-drive a failed or dead-lettered job** — terminal jobs are refused by
  `ClaimJob` by design, so recovery means submitting a new identical job manually.
- [ ] **Idempotent submit** — retries of a failed HTTP request create duplicate jobs;
  accept an `Idempotency-Key` (or caller-supplied job ID) before this matters.
- [ ] **Dead-letter queue surface** — dead-lettering exists (`max_attempts`), but
  there's no dedicated view of stuck/failed jobs beyond status filtering (needs the
  list endpoint above).

## 3. Observability (P0 → P1)

- [ ] **Metrics.** No `/metrics`. Priorities: queue depth (`ready`, `in_flight`),
  claims/reclaims/dead-letters counters, step durations, job durations, lease-loss
  count, provider call latency/error rate per agent, worker roster gauge.
- [ ] **Structured logs.** Everything is `log.Printf`; no levels, no JSON, no
  request-ID propagation (chi `RequestID` middleware is mounted but the ID is never
  logged or echoed).
- [ ] **Readiness vs liveness.** `/healthz` is static. A readiness probe that fails
  when the store/queue is down would let a load balancer pick a broken node.
- [ ] **Version/build endpoint** (`/v1/version`) for deployment tracking.
- [ ] SSE keep-alive: the stream writes nothing between events, so intermediate
  proxies/lbs may time out idle connections; add periodic `: ping` comments.

## 4. Schema management (P1 — blocks safe upgrades)

- [ ] **Versioned migrations.** Schema is additive in-code
  (`CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`). Fine until a rename or
  backfill is needed; adopt a migration tool before then.
- [ ] **Retention / GC.** Terminal jobs, steps, and events accumulate forever. Need a
  TTL sweep (e.g., delete events older than N days; jobs older than M) plus an index
  on `workflow_events.created_at` for the sweep.
- [ ] **Run under `go vet`/lint statically-fenced** — there is no CI: add a GitHub
  Actions workflow running `task check` (it's described as "what CI runs" but no
  workflow file is checked in).

## 5. Engine capabilities (P1 → P2, product-facing)

- [ ] **Per-step retry/timeout overrides.** Retry policy is global today; long steps
  and short steps cannot coexist with one `lease_ms` either (a slow step forces an
  inflated lease for everyone).
- [ ] **Workflow deadline.** A sync `/v1/workflows/run` is bounded only by per-agent
  timeout × retries; set a wall-clock budget per workflow.
- [ ] **Richer templating.** Only `{{steps.<id>.output}}` exists. Expected next:
  JSON field extraction (`{{steps.a.output.summary}}`), default values, escaping.
- [ ] **Inputs/metadata.** No way to inject submit-time variables or label workflows
  (working with the list endpoint, this enables searching).
- [ ] **Structured outputs.** `Agent.Run` returns one string; JSON mode/tool calls
  would need an `Agent` interface change (see non-goals note: deliberate v1 scope).
- [ ] **More agent kinds.** The interface can already express HTTP-call or shell agents;
  the wiring and config only support Eino chat models.

## 6. Distributed behaviour (P1)

- [ ] **Delayed retries.** An abandoned job is requeued immediately, so a temporarily broken backend burns `max_attempts` fast; consider a small delay or exponential
  backoff on redelivery.
- [ ] **PRIORITY/FAIRNESS.** Single FIFO queue; a chatty tenant can starve everyone.
- [ ] **Redis Cluster compatibility.** Lua scripts touch queue keys across 4 keys;
  under Cluster they must hash-tag to one slot (document or implement).
- [ ] **LISTEN/NOTIFY option for SSE** if many watchers become the norm (current
  500 ms polling per connection scales with viewers × DB).
- [ ] **Backpressure signal.** `/v1/cluster` exposes depth but nothing rejects work
  when the queue is full/failing.

## 7. Docs & hygiene (P2)

- [ ] CHANGELOG, CONTRIBUTING, release/versioning (`go build` has no version flags; a
  `/v1/version` endpoint needs them).
- [ ] More examples beyond `workflow.json` (fan-out/fan-in, retry demo, async client
  with SSE consumption).
- [ ] golangci-lint beyond `gofmt + go vet`, pre-commit equivalents.
- [ ] Kubernetes manifest/Helm chart — multi-node is the advertisement; deployment
  tooling for it is absent.
- [ ] Otel traces (P2) — span per step would answer "where did my tokens go" with
  existing `OnStepState`.

## Explicit non-goals (do not file as bugs)

- **Business-failure retries at the job level.** A workflow whose steps exhaust
  retries is recorded `failed` and stays failed; only *infrastructure* failures
  redeliver. Deliberate: retrying a definitive answer wastes money.
- **Per-tenant isolation of every kind** (separate DB schemas, per-tenant queues) —
  out of scope until §1 ships.
- **Exactly-once delivery.** At-least-once + at-most-once per step is the model
  (see `docs/ANCHORA.md` §8).
- **Streaming model responses.** The `Agent` contract is a single prompt in, single
  text out; changing it is a v2 decision, not a gap in v1.
