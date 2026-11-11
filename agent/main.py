# Agent sidecar: serves workflow.Agent over HTTP (POST /run) backed by a local
# Qwen checkpoint. The Go executable calls it; which model to serve, and which
# runtime serves it, is decided here, not in the Anchora configuration.
#
# Two runtimes ("backends") are built in and chosen with BACKEND=auto|mlx|transformers:
#
#   mlx           Apple Silicon. Weights live in unified memory and run on the
#                 GPU through MLX; mlx-lm's BatchGenerator gives continuous
#                 batching, so requests that arrive while others are decoding
#                 join the running batch instead of waiting for it. This is
#                 the fastest way to serve Qwen3 on a Mac and the only one that
#                 fits the 8B-32B sizes in 24 GB (through 4-bit checkpoints).
#   transformers  Everything else: CUDA, CPU, or MPS through PyTorch. Requests
#                 are gathered for a short window and generated as one padded
#                 batch, which is what the engine's concurrent waves produce.
#
# Either way exactly one thread touches the model. Generation is serialized
# behind a queue, so concurrent HTTP requests never contend for the GPU; they
# are batched instead. The HTTP layer is async and only ever waits.
import logging
import os
import platform
import queue
import threading
import time
from concurrent.futures import Future
from dataclasses import dataclass, field
from typing import Optional

import asyncio

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

log = logging.getLogger("anchora-agent")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


def env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen3-0.6B")
BACKEND = os.getenv("BACKEND", "auto").strip().lower()
MAX_NEW_TOKENS = env_int("MAX_NEW_TOKENS", 256)
# How many sequences decode together. MLX admits new requests into a running
# batch up to this many; transformers gathers up to this many per generate().
BATCH_SIZE = env_int("BATCH_SIZE", 16)
# transformers only: how long the collector waits for siblings after the first
# request of a batch arrives. The engine starts a whole wave within a few
# hundred microseconds, so a short window catches it without adding latency.
BATCH_WINDOW_MS = env_int("BATCH_WINDOW_MS", 10)
# Qwen3 is a hybrid reasoning model; thinking roughly doubles the tokens a step
# emits and the engine only wants the answer, so it is off unless asked for.
ENABLE_THINKING = env_bool("ENABLE_THINKING", False)
# Sampling. Greedy by default: workflow steps want deterministic, repeatable
# output. TEMPERATURE > 0 switches to nucleus sampling.
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))
TOP_P = float(os.getenv("TOP_P", "0.95"))
# mlx only: prompt tokens processed per prefill step, and how many prompts are
# prefilled together. Larger steps are faster on big prompts, at the cost of
# peak memory.
PREFILL_STEP_SIZE = env_int("PREFILL_STEP_SIZE", 2048)
PREFILL_BATCH_SIZE = env_int("PREFILL_BATCH_SIZE", 8)
# mlx only: cap on KV cache entries per sequence (0 = unbounded). Set it when
# serving a large model near the memory limit with long prompts.
MAX_KV_SIZE = env_int("MAX_KV_SIZE", 0)


def choose_backend() -> str:
    if BACKEND in {"mlx", "transformers"}:
        return BACKEND
    if BACKEND != "auto":
        raise SystemExit(f"BACKEND={BACKEND!r} is not one of auto, mlx, transformers")
    if platform.system() == "Darwin" and platform.machine() == "arm64":
        try:
            import mlx_lm  # noqa: F401

            return "mlx"
        except ImportError:
            log.warning("mlx-lm is not installed; falling back to transformers (install mlx-lm for Apple Silicon)")
    return "transformers"


@dataclass
class Request:
    prompt: str
    instruction: str
    max_new_tokens: int
    future: Future = field(default_factory=Future)
    enqueued_at: float = field(default_factory=time.perf_counter)


@dataclass
class Completion:
    output: str
    prompt_tokens: int
    completion_tokens: int
    finish_reason: str


class Stats:
    """Lifetime counters, reported at GET /stats so a load run can be read
    straight off the sidecar: tokens, requests, and the queue wait that tells
    you whether the engine is sending more than the model can absorb."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = time.time()
        self.requests = 0
        self.failures = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.generation_seconds = 0.0
        self.queue_wait_seconds = 0.0
        self.batches = 0
        self.max_batch = 0

    def record(self, completions: list[Completion], waited: list[float], seconds: float) -> None:
        with self.lock:
            self.requests += len(completions)
            self.prompt_tokens += sum(c.prompt_tokens for c in completions)
            self.completion_tokens += sum(c.completion_tokens for c in completions)
            self.generation_seconds += seconds
            self.queue_wait_seconds += sum(waited)
            self.batches += 1
            self.max_batch = max(self.max_batch, len(completions))

    def snapshot(self) -> dict:
        with self.lock:
            tps = self.completion_tokens / self.generation_seconds if self.generation_seconds else 0.0
            return {
                "uptime_seconds": round(time.time() - self.started_at, 1),
                "requests": self.requests,
                "failures": self.failures,
                "batches": self.batches,
                "max_batch": self.max_batch,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
                "generation_seconds": round(self.generation_seconds, 3),
                "completion_tokens_per_second": round(tps, 1),
                "mean_queue_wait_ms": round(1000 * self.queue_wait_seconds / self.requests, 1) if self.requests else 0.0,
            }


stats = Stats()


def render_messages(request: Request) -> list[dict]:
    messages = [{"role": "user", "content": request.prompt}]
    if request.instruction:
        messages.insert(0, {"role": "system", "content": request.instruction})
    return messages


# --------------------------------------------------------------------------
# MLX backend: continuous batching on Apple Silicon.
# --------------------------------------------------------------------------
class MLXBackend:
    name = "mlx"

    def __init__(self) -> None:
        import mlx.core as mx
        from mlx_lm import load
        from mlx_lm.generate import BatchGenerator
        from mlx_lm.sample_utils import make_sampler

        self.mx = mx
        self.model, self.tokenizer = load(MODEL_NAME)
        sampler = make_sampler(temp=TEMPERATURE, top_p=TOP_P) if TEMPERATURE > 0 else None
        self.generator = BatchGenerator(
            self.model,
            max_tokens=MAX_NEW_TOKENS,
            stop_tokens=[[t] for t in self.tokenizer.eos_token_ids],
            sampler=sampler,
            completion_batch_size=BATCH_SIZE,
            prefill_batch_size=PREFILL_BATCH_SIZE,
            prefill_step_size=PREFILL_STEP_SIZE,
            max_kv_size=MAX_KV_SIZE or None,
        )
        self.queue: "queue.Queue[Request]" = queue.Queue()
        self.in_flight: dict[int, tuple[Request, list[int], int, float]] = {}
        threading.Thread(target=self.loop, name="mlx-generate", daemon=True).start()

    def device_info(self) -> dict:
        info = self.mx.device_info()
        return {
            "device": "metal",
            "memory_gb": round(info.get("memory_size", 0) / 2**30, 1),
            "max_recommended_working_set_gb": round(info.get("max_recommended_working_set_size", 0) / 2**30, 1),
        }

    def encode(self, request: Request) -> list[int]:
        text = self.tokenizer.apply_chat_template(
            render_messages(request),
            tokenize=False,
            add_generation_prompt=True,
            enable_thinking=ENABLE_THINKING,
        )
        return self.tokenizer.encode(text, add_special_tokens=False)

    def submit(self, request: Request) -> None:
        self.queue.put(request)

    def admit(self, block: bool) -> None:
        """Move waiting requests into the generator: all of them, blocking for
        the first one when the generator is otherwise idle."""
        pending: list[Request] = []
        if block:
            pending.append(self.queue.get())
        while len(self.in_flight) + len(pending) < BATCH_SIZE:
            try:
                pending.append(self.queue.get_nowait())
            except queue.Empty:
                break
        if not pending:
            return
        prompts = [self.encode(r) for r in pending]
        uids = self.generator.insert(prompts, max_tokens=[r.max_new_tokens for r in pending])
        now = time.perf_counter()
        for uid, request, prompt in zip(uids, pending, prompts):
            self.in_flight[uid] = (request, [], len(prompt), now)
        with stats.lock:
            stats.batches += 1
            stats.max_batch = max(stats.max_batch, len(self.in_flight))

    def loop(self) -> None:
        while True:
            try:
                self.admit(block=not self.in_flight)
                started = time.perf_counter()
                responses = self.generator.next_generated()
                step_seconds = time.perf_counter() - started
                finished: list[Completion] = []
                waited: list[float] = []
                for r in responses:
                    request, tokens, prompt_len, admitted = self.in_flight[r.uid]
                    if r.finish_reason != "stop":
                        tokens.append(r.token)
                    if r.finish_reason is None:
                        continue
                    del self.in_flight[r.uid]
                    text = self.tokenizer.decode(tokens).strip()
                    completion = Completion(text, prompt_len, len(tokens), r.finish_reason)
                    request.future.set_result(completion)
                    finished.append(completion)
                    waited.append(admitted - request.enqueued_at)
                # Batches and the widest batch are counted at admission; the
                # whole decode step is attributed to generation time, since the
                # per-request split is not observable from here.
                with stats.lock:
                    stats.generation_seconds += step_seconds
                    stats.requests += len(finished)
                    stats.prompt_tokens += sum(c.prompt_tokens for c in finished)
                    stats.completion_tokens += sum(c.completion_tokens for c in finished)
                    stats.queue_wait_seconds += sum(waited)
            except Exception as exc:  # noqa: BLE001 - one bad batch must not kill the server
                log.exception("generation failed")
                for uid, (request, *_rest) in list(self.in_flight.items()):
                    request.future.set_exception(exc)
                self.in_flight.clear()
                with stats.lock:
                    stats.failures += 1


# --------------------------------------------------------------------------
# transformers backend: windowed batching on CUDA, CPU, or MPS.
# --------------------------------------------------------------------------
class TransformersBackend:
    name = "transformers"

    def __init__(self) -> None:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        self.torch = torch
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, padding_side="left")
        if self.tokenizer.pad_token_id is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, dtype="auto", device_map="auto")
        self.model.eval()
        if torch.cuda.is_available():
            # Lets matmuls use TF32 on Ampere and newer; harmless elsewhere.
            torch.backends.cuda.matmul.allow_tf32 = True
        self.queue: "queue.Queue[Request]" = queue.Queue()
        threading.Thread(target=self.loop, name="transformers-generate", daemon=True).start()

    def device_info(self) -> dict:
        device = str(self.model.device)
        info = {"device": device, "dtype": str(self.model.dtype)}
        if self.torch.cuda.is_available():
            info["gpu"] = self.torch.cuda.get_device_name(0)
            info["memory_gb"] = round(self.torch.cuda.get_device_properties(0).total_memory / 2**30, 1)
        return info

    def submit(self, request: Request) -> None:
        self.queue.put(request)

    def gather(self) -> list[Request]:
        batch = [self.queue.get()]
        deadline = time.perf_counter() + BATCH_WINDOW_MS / 1000
        while len(batch) < BATCH_SIZE:
            remaining = deadline - time.perf_counter()
            if remaining <= 0:
                break
            try:
                batch.append(self.queue.get(timeout=remaining))
            except queue.Empty:
                break
        return batch

    def generate(self, batch: list[Request]) -> list[Completion]:
        texts = [
            self.tokenizer.apply_chat_template(
                render_messages(r), tokenize=False, add_generation_prompt=True, enable_thinking=ENABLE_THINKING
            )
            for r in batch
        ]
        inputs = self.tokenizer(texts, return_tensors="pt", padding=True).to(self.model.device)
        prompt_len = inputs["input_ids"].shape[1]
        kwargs = dict(max_new_tokens=max(r.max_new_tokens for r in batch), pad_token_id=self.tokenizer.pad_token_id)
        if TEMPERATURE > 0:
            kwargs.update(do_sample=True, temperature=TEMPERATURE, top_p=TOP_P)
        else:
            kwargs.update(do_sample=False)
        with self.torch.inference_mode():
            generated = self.model.generate(**inputs, **kwargs)
        completions = []
        attention = inputs["attention_mask"]
        for i, request in enumerate(batch):
            new_tokens = generated[i][prompt_len:]
            # Trim at the first end-of-sequence token so padding past it is not
            # counted; decode skips the specials either way.
            eos_hits = (new_tokens == self.tokenizer.eos_token_id).nonzero()
            if self.tokenizer.pad_token_id != self.tokenizer.eos_token_id:
                pad_hits = (new_tokens == self.tokenizer.pad_token_id).nonzero()
                eos_hits = self.torch.cat([eos_hits, pad_hits]) if len(pad_hits) else eos_hits
            cut = int(eos_hits.min()) if len(eos_hits) else len(new_tokens)
            text = self.tokenizer.decode(new_tokens[:cut], skip_special_tokens=True).strip()
            reason = "stop" if len(eos_hits) else "length"
            completions.append(Completion(text, int(attention[i].sum()), min(cut, request.max_new_tokens), reason))
        return completions

    def loop(self) -> None:
        while True:
            batch = self.gather()
            started = time.perf_counter()
            try:
                completions = self.generate(batch)
            except Exception as exc:  # noqa: BLE001
                log.exception("generation failed")
                for request in batch:
                    request.future.set_exception(exc)
                with stats.lock:
                    stats.failures += 1
                continue
            seconds = time.perf_counter() - started
            for request, completion in zip(batch, completions):
                request.future.set_result(completion)
            stats.record(completions, [started - r.enqueued_at for r in batch], seconds)


# --------------------------------------------------------------------------
# HTTP surface.
# --------------------------------------------------------------------------
backend_name = choose_backend()
log.info("loading %s with the %s backend", MODEL_NAME, backend_name)
load_started = time.perf_counter()
backend = MLXBackend() if backend_name == "mlx" else TransformersBackend()
log.info("model ready in %.1fs: %s", time.perf_counter() - load_started, backend.device_info())

app = FastAPI(title="anchora-agent")


class RunRequest(BaseModel):
    prompt: str
    instruction: str = ""
    # Per-request cap, bounded by MAX_NEW_TOKENS. Anchora does not send it;
    # it is here for direct callers and benchmarks.
    max_new_tokens: Optional[int] = None


class Usage(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    finish_reason: str
    latency_ms: float


class RunResponse(BaseModel):
    output: str
    usage: Usage


@app.get("/healthz")
def healthz() -> dict:
    return {
        "status": "ok",
        "model": MODEL_NAME,
        "backend": backend.name,
        "max_new_tokens": MAX_NEW_TOKENS,
        "batch_size": BATCH_SIZE,
        "thinking": ENABLE_THINKING,
        **backend.device_info(),
    }


@app.get("/stats")
def stats_endpoint() -> dict:
    return {"model": MODEL_NAME, "backend": backend.name, **stats.snapshot()}


@app.post("/run")
async def run(request: RunRequest) -> RunResponse:
    limit = MAX_NEW_TOKENS
    if request.max_new_tokens is not None and request.max_new_tokens > 0:
        limit = min(limit, request.max_new_tokens)
    pending = Request(prompt=request.prompt, instruction=request.instruction, max_new_tokens=limit)
    backend.submit(pending)
    try:
        completion: Completion = await asyncio.wrap_future(pending.future)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"generation failed: {exc}") from exc
    # A step that produced no text has nothing to pass downstream, so an empty
    # completion is an error rather than an empty success.
    if not completion.output:
        raise HTTPException(status_code=500, detail="model returned no text")
    return RunResponse(
        output=completion.output,
        usage=Usage(
            prompt_tokens=completion.prompt_tokens,
            completion_tokens=completion.completion_tokens,
            finish_reason=completion.finish_reason,
            latency_ms=round(1000 * (time.perf_counter() - pending.enqueued_at), 1),
        ),
    )
