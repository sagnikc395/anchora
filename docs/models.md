# Serving Qwen models

The sidecar (`agent/main.py`) serves one checkpoint. Which one is its own
setting, `MODEL_NAME`; nothing in Anchora's configuration names a model, so
swapping Qwen3-0.6B for Qwen3-14B is a sidecar restart, not a config change.
Every agent in `config.yaml` keeps working, because they only differ in their
system instruction.

```sh
MODEL=mlx-community/Qwen3-8B-4bit task agent-run        # Apple Silicon
MODEL_NAME=Qwen/Qwen3-8B BACKEND=transformers uvicorn main:app --port 8000   # CUDA
```

`GET /healthz` on the sidecar reports what is loaded: model, backend, device,
the memory it can use, and the generation limits in force.

## Backends

| `BACKEND` | Runtime | Where | Batching | Quantized checkpoints |
| --- | --- | --- | --- | --- |
| `mlx` | [mlx-lm](https://github.com/ml-explore/mlx-lm) on Metal | macOS on Apple Silicon | continuous: a request joins the running batch | `mlx-community/*-4bit`, `*-6bit`, `*-8bit` |
| `transformers` | PyTorch | CUDA, CPU, MPS, and inside containers | windowed: requests arriving within `BATCH_WINDOW_MS` form one padded batch | bf16 by default; AWQ/GPTQ/FP8 need their kernels installed |

`BACKEND=auto` (the default) picks `mlx` when `mlx-lm` imports on an arm64 Mac
and `transformers` everywhere else. The MLX backend is the one to use on a
Mac: it runs on the GPU with unified memory, it is faster than PyTorch's MPS
path at every size we measured, and 4-bit checkpoints are what make the 8B
and larger models fit. A Linux container on a Mac cannot reach the GPU at
all, so run the sidecar natively there (`task agent-run`) and use the compose
`agent` service only on Linux hosts.

Both backends load any Hugging Face checkpoint by repo name or local path.
MLX reads `Qwen/Qwen3-*` bf16 safetensors directly, so a quantized
`mlx-community` repo is a size choice, not a requirement.

## Which checkpoint

Rule of thumb for weights alone: bf16 takes 2 bytes per parameter, 8-bit
about 1.1, 4-bit about 0.6. Add the KV cache on top: for Qwen3-8B at 4-bit,
every 1,000 tokens in flight across the batch costs roughly 140 MB. macOS
lets the GPU use about three quarters of unified memory (`/healthz` reports
the limit as `max_recommended_working_set_gb`; it is 17.8 GB on a 24 GB M4),
and that is the number to fit under.

Qwen3 dense and MoE sizes, with what they need on a 24 GB Apple Silicon
machine. Decode rates are from `task bench` on an M4 with 24 GB, greedy,
256-token cap, and are what a single workflow step sees; batched throughput is
higher (see [performance.md](performance.md)).

| Checkpoint | Params | Weights | Fits 24 GB? | Single-step decode | Notes |
| --- | --- | --- | --- | --- | --- |
| `Qwen/Qwen3-0.6B` | 0.6B | 1.2 GB bf16 | yes | ~110 tok/s | the default; fast enough to run every example in about a second |
| `Qwen/Qwen3-1.7B` | 1.7B | 3.4 GB bf16 | yes | | noticeably better summaries than 0.6B for little cost |
| `mlx-community/Qwen3-4B-4bit` | 4B | 2.3 GB | yes | see performance.md | the sweet spot for a laptop: quality of a real assistant at 0.6B-like memory |
| `Qwen/Qwen3-4B` | 4B | 8 GB bf16 | yes | | same model unquantized; prefer it only if you have the memory to spare |
| `mlx-community/Qwen3-8B-4bit` | 8B | 4.6 GB | yes | see performance.md | the recommended "bigger" model for this project |
| `mlx-community/Qwen3-8B-8bit` | 8B | 8.7 GB | yes | | closer to bf16 quality, roughly half the speed of 4-bit |
| `mlx-community/Qwen3-14B-4bit` | 14B | 8.3 GB | yes | | largest dense size that leaves comfortable room for KV cache and batching |
| `mlx-community/Qwen3-30B-A3B-4bit` | 30B MoE, 3B active | 17 GB | barely | | fast for its size because only 3B parameters run per token, but the weights alone sit at the GPU limit; set `BATCH_SIZE=2` and `MAX_KV_SIZE` |
| `mlx-community/Qwen3-32B-4bit` | 32B | 18.5 GB | no (swaps) | | needs 32 GB or more of unified memory |

For CUDA through the `transformers` backend the same table applies with the
bf16 `Qwen/Qwen3-*` names and the bf16 column: an 8B model wants a 24 GB GPU,
14B a 40 GB one, 32B an 80 GB one. `Qwen/Qwen3-*-AWQ` and `-FP8` repos cut
that in half but need `autoawq` or a Hopper/Ada GPU respectively; they are not
pinned in `requirements.txt`.

### The 2507 instruct variants

`Qwen/Qwen3-4B-Instruct-2507` and `Qwen/Qwen3-30B-A3B-Instruct-2507` (and the
`mlx-community` 4-bit conversions of them) are non-thinking models with a
longer context. They are a drop-in for the workflow use case and never spend
tokens on a reasoning block. The sidecar passes `enable_thinking=False` to
the chat template, which these templates simply ignore.

### Newer Qwen families

`Qwen/Qwen3.5-*`, `Qwen3.6-*`, and `Qwen3.8-*` checkpoints are on the hub and
load through the same two code paths, but they have not been run through this
project. Check that the `transformers` or `mlx-lm` version pinned in
`agent/requirements.txt` supports the architecture before relying on one, and
check that its chat template honours the thinking switch (or that it has no
thinking mode) by looking at the first few outputs for a `<think>` block.

## Thinking

Qwen3 is a hybrid reasoning model. With thinking on it writes a `<think>`
block before the answer, which doubles or triples the tokens a step emits
and lands in the step's output, where the next step's prompt would quote it.
The sidecar therefore disables it by default. `ENABLE_THINKING=true` turns it
on; raise `MAX_NEW_TOKENS` at the same time or answers will be cut off inside
the reasoning.

## Timeouts

An agent's `timeout_ms` in `config.yaml` bounds one attempt, including queue
wait and prefill. Budget it as `MAX_NEW_TOKENS / decode rate` for the slowest
model you serve, with a margin for batching (every sequence in a batch
decodes at the batch's rate, which is lower per sequence than the single-step
rate). The checked-in 120 s covers 256 tokens for everything up to 14B on an
M4. If a step times out it fails and retries; `anchora_steps_total{status="failed"}`
and the `error` on the step result are where that shows.

## Local checkpoints

`MODEL_NAME` can be a directory. That is useful when the hub is slow (a
parallel downloader beats `hf download` by a wide margin on a throttled
link) or when you quantize your own copy:

```sh
cd agent && .venv/bin/python -m mlx_lm convert --hf-path Qwen/Qwen3-8B -q --q-bits 4 --mlx-path ./models/Qwen3-8B-4bit
MODEL=./models/Qwen3-8B-4bit task agent-run
```
