# Agent sidecar: serves workflow.Agent over HTTP (POST /run) backed by a
# Hugging Face transformers text-generation model. The Go executable calls it;
# which model to serve is decided here, not in the Anchora configuration.
import os

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from transformers import AutoModelForCausalLM, AutoTokenizer

MODEL_NAME = os.getenv("MODEL_NAME", "Qwen/Qwen3-0.6B")
MAX_NEW_TOKENS = int(os.getenv("MAX_NEW_TOKENS", "256"))

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, dtype="auto", device_map="auto")

app = FastAPI(title="anchora-agent")


class RunRequest(BaseModel):
    prompt: str
    instruction: str = ""


class RunResponse(BaseModel):
    output: str


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/run")
def run(request: RunRequest) -> RunResponse:
    messages = [{"role": "user", "content": request.prompt}]
    if request.instruction:
        messages.insert(0, {"role": "system", "content": request.instruction})
    # Qwen3 is a reasoning model that wraps its answer in </think><think> unless told otherwise; the template's switch keeps outputs direct.
    prompt = tokenizer.apply_chat_template(
        messages, tokenize=False, add_generation_prompt=True, enable_thinking=False
    )
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    generated = model.generate(**inputs, max_new_tokens=MAX_NEW_TOKENS, do_sample=False)
    output = tokenizer.decode(
        generated[0][inputs["input_ids"].shape[1] :], skip_special_tokens=True
    ).strip()
    # A step that produced no text has nothing to pass downstream, so an empty
    # completion is an error rather than an empty success.
    if not output:
        raise HTTPException(status_code=500, detail="model returned no text")
    return RunResponse(output=output)
