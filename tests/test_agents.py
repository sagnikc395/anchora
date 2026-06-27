from __future__ import annotations

from smolagents import InferenceClientModel

from anchora.agents import build_model
from anchora.config import Config, ProviderConfig, WorkflowConfig


def test_build_model_uses_huggingface_inference_client(monkeypatch):
    monkeypatch.setenv("HF_TOKEN", "secret")

    model = build_model(
        Config(
            provider=ProviderConfig(
                name="huggingface",
                model="Qwen/Qwen3-Next-80B-A3B-Thinking",
                inference_provider="auto",
                max_tokens=128,
            ),
            workflow=WorkflowConfig(max_retries=0, retry_delay_ms=0),
        )
    )

    assert isinstance(model, InferenceClientModel)
    assert model.client_kwargs["provider"] == "auto"
    assert model.client_kwargs["token"] == "secret"
