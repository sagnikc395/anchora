# Performance

Where the time goes in an Anchora workflow, what was done about it, and how
to measure it. Numbers in this page come from `task bench` on an Apple M4 with
24 GB, greedy decoding, a 256-token cap, and the checked-in examples.

## Where the time goes

A workflow step is one round trip to the sidecar, and the sidecar's time is
almost entirely the model decoding tokens one at a time. Decoding is
memory-bound: each token streams the whole weight set through the GPU, so a
single sequence uses a fraction of the hardware, and a batch of sequences
decoding together costs about the same per step as one. Everything below
follows from that:

1. **Never decode one sequence when several are waiting.** The engine
   starts every ready step at once, so concurrency is the normal case.
2. **Fewer tokens is the biggest lever.** Output length, not model size, sets
   the latency of a step. A 256-token answer from a 4B model takes longer
   than a 60-token answer from a 14B one.
3. **Keep the model busy and nothing else in the way.** Serialising the
   HTTP layer, the Go side, or the connection setup in front of a GPU that
   is waiting is pure loss.

## What changed

Three layers, in order of effect.

### The sidecar batches

The original sidecar ran `model.generate` inside a synchronous FastAPI
handler. Uvicorn ran concurrent requests on a thread pool, so a wave of six
steps meant six generations contending for one GPU, each at single-sequence
speed, with no batching at all.

Now exactly one thread owns the model and the HTTP handlers are async and
only wait. Requests go onto a queue and are generated together:

- **MLX (Apple Silicon)** uses mlx-lm's `BatchGenerator`, which is continuous
  batching: a request arriving while others are mid-decode is prefilled and
  joins the running batch at the next step, up to `BATCH_SIZE` sequences. No
  request waits for an unrelated one to finish.
- **transformers (CUDA, CPU, MPS)** gathers whatever arrives within
  `BATCH_WINDOW_MS` of the first request, up to `BATCH_SIZE`, left-pads, and
  generates one batch. The engine launches a wave within a millisecond, so
  10 ms catches a whole wave without adding visible latency to a lone step.

The MLX backend is also simply faster per token than PyTorch on MPS, and it
is the path that fits the 8B to 30B checkpoints into 24 GB through 4-bit
quantization ([models.md](models.md)).

### The engine is dataflow, not waves

The engine used to run in waves: collect every step whose dependencies are
done, run them all, wait for the slowest, then compute the next wave. A
chain hanging off a fast step waited for an unrelated slow sibling to finish.
`workflow.Run` now starts a step the moment its own dependencies have
succeeded and re-evaluates on every completion. Results, prompt rendering,
and the skip cascade still happen on the single scheduling goroutine, so the
race-freedom guarantee is unchanged, and `tests/workflow_test.go` pins the
new behaviour.

### The HTTP client keeps connections warm

Every step hits the same sidecar. Go's default transport keeps two idle
connections per host, so a fan-out of six opened four fresh TCP connections
per wave and closed them afterwards. The `remoteagent` client now keeps a
pool sized for a wide wave with a 90 s idle timeout.

## Measured

Same machine, same model (Qwen3-0.6B), same examples, three runs each. "Old"
is the sidecar and engine at commit `8b35169`; the two "new" rows differ only
in the backend.

| Workload | Old (transformers, unbatched) | New, `transformers` on MPS | New, `mlx` |
| --- | --- | --- | --- |
| `workflow.json`, 2 sequential steps | 1049 ms | 1069 ms | 981 ms |
| `batch.json`, 6 parallel + join + summary | 1985 ms | 1343 ms | 980 ms |
| 4 × `parallel.json` at once, 16 steps | 10415 ms | 1683 ms | 1219 ms |
| decode throughput during the run | n/a | 86 tok/s | 109 tok/s |

Sequential latency is the same everywhere because a single short step is
dominated by the model's per-token time, which nothing above changes. The
concurrent rows are the point: sixteen steps that used to queue up for ten
seconds now finish in just over one, and a fan-out costs the same as a
two-step chain because its six steps decode as one batch.

Larger models on MLX, same examples:

| Checkpoint | `workflow.json` | `batch.json` | 16 concurrent steps | decode throughput |
| --- | --- | --- | --- | --- |
| `Qwen/Qwen3-0.6B` | 981 ms | 980 ms | 1219 ms | 109 tok/s |
| `mlx-community/Qwen3-4B-4bit` | _pending_ | _pending_ | _pending_ | _pending_ |
| `mlx-community/Qwen3-8B-4bit` | _pending_ | _pending_ | _pending_ | _pending_ |

## Measuring it yourself

```sh
task agent-run                    # or MODEL=mlx-community/Qwen3-8B-4bit task agent-run
task run                          # in another shell
task bench                        # RUNS=5 CONCURRENCY=8 task bench for a longer run
```

`scripts/bench.sh` times the sequential, fan-out, and concurrent shapes and
reads the sidecar's counters before and after, so the last lines are the
model's own decode rate and how long requests waited in the queue. The
counters are at `GET /stats` on the sidecar:

| Field | Meaning |
| --- | --- |
| `completion_tokens_per_second` | lifetime decode rate across all batches |
| `mean_queue_wait_ms` | time from arrival to the model starting on it; rising means the engine is sending more than the model absorbs |
| `max_batch` | the widest batch seen; if it is stuck at 1 the engine is not sending concurrent work |
| `failures` | generation errors (out of memory is the usual one) |

Every `/run` response also carries `usage` with the prompt and completion
token counts, the finish reason (`stop` or `length`), and the request's
latency, so a single step can be inspected with `curl`.

## Knobs

Sidecar environment variables. The defaults are what the numbers above used.

| Variable | Default | Effect |
| --- | --- | --- |
| `MODEL_NAME` | `Qwen/Qwen3-0.6B` | checkpoint, by hub name or local path ([models.md](models.md)) |
| `BACKEND` | `auto` | `mlx`, `transformers`, or pick by platform |
| `MAX_NEW_TOKENS` | `256` | the cap on a step's output; the single biggest latency lever. A per-request `max_new_tokens` in the body can only lower it |
| `BATCH_SIZE` | `16` | sequences decoding together. Raise it for wide workflows or many durable workers; lower it when a large model is near the memory limit |
| `BATCH_WINDOW_MS` | `10` | transformers only: how long to wait for siblings after the first request |
| `ENABLE_THINKING` | `false` | Qwen3 reasoning block; costs two to three times the tokens |
| `TEMPERATURE`, `TOP_P` | `0`, `0.95` | greedy by default, which is also the fastest sampler |
| `PREFILL_STEP_SIZE` | `2048` | mlx only: prompt tokens per prefill step; larger is faster on long prompts, at the cost of peak memory |
| `PREFILL_BATCH_SIZE` | `8` | mlx only: prompts prefilled together |
| `MAX_KV_SIZE` | unbounded | mlx only: cap on KV entries per sequence, for large models with long prompts |
| `OMP_NUM_THREADS` | | transformers on CPU: set it to the physical core count |

Anchora-side settings that interact with them:

- `agents.<name>.timeout_ms` has to cover `MAX_NEW_TOKENS` at the batch's
  per-sequence rate. See [models.md](models.md#timeouts).
- `async.workers` is how many durable jobs one node runs at once. Each job's
  wave lands on the sidecar together, so `BATCH_SIZE` should be at least
  `workers` times the widest wave you run, or jobs will queue at the model.
- `workflow.max_retries` and `retry_delay_ms` only matter once a step has
  already failed; they add nothing to the happy path.

## Tuning by situation

**A lone step is slow.** Shorten the output: tighten the instruction
("answer in one sentence"), lower `MAX_NEW_TOKENS`, make sure thinking is
off. Then pick a smaller or more quantized checkpoint. The Mac-side ceiling
is the memory bandwidth, so 4-bit weights decode faster than 8-bit, which
decode faster than bf16.

**Concurrent steps are slow but a lone step is fine.** Check `max_batch` in
`/stats`. If it is 1 the requests are not reaching the sidecar together: the
engine is not being given independent steps, or (transformers) the window is
too short. If it is high and `mean_queue_wait_ms` is climbing, the batch is
full; raise `BATCH_SIZE` if memory allows, or add a second sidecar on another
port and point a second agent name at it.

**Out of memory.** The sidecar logs `generation failed` and the batch fails.
Lower `BATCH_SIZE`, set `MAX_KV_SIZE`, lower `PREFILL_STEP_SIZE`, or move
down a quantization level. On macOS also close other GPU users; the limit is
shared.

**Running in Docker on a Mac.** The container is CPU-only and an order of
magnitude slower than the native MLX sidecar. Use `task agent-run` for the
sidecar and the containers for everything else; the compose `agent` service
is for Linux hosts, where it can take a GPU through the commented `deploy`
block.

## Headroom not yet taken

- **A vLLM or SGLang backend** for Linux GPUs. The transformers path batches
  within a window; a real serving engine does continuous batching with paged
  KV memory and would roughly double throughput on CUDA. The sidecar's
  backend interface (`submit`, `device_info`) is where it would plug in.
- **Prompt caching of the system instruction.** Every step from the same
  agent shares a prefix; mlx-lm can reuse a prefilled cache for it, which
  saves the prefill on each call. Prefill is a small fraction of a 256-token
  step today, so this matters more for long instructions.
- **Speculative decoding** with a draft model (`Qwen3-0.6B` drafting for
  `Qwen3-14B`). mlx-lm supports it for single-sequence generation but not in
  the batch generator yet.
- **Quantized KV cache** (`kv_bits`) to fit longer prompts for the 30B-A3B
  size.
- **Streaming step output** to the client. The engine needs the full output
  before a dependent step can render its prompt, so this only improves
  perceived latency for the synchronous API.
