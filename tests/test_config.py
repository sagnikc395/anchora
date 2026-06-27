from __future__ import annotations

import pytest

from anchora.config import ProviderConfig, load_config


def test_load_config_reads_provider_and_workflow(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        """
provider:
  name: huggingface
  model: Qwen/Qwen3-Next-80B-A3B-Thinking
  inference_provider: auto
  max_tokens: 1024

workflow:
  max_retries: 2
  retry_delay_ms: 500
"""
    )

    config = load_config(path)

    assert config.provider.name == "huggingface"
    assert config.provider.model == "Qwen/Qwen3-Next-80B-A3B-Thinking"
    assert config.provider.inference_provider == "auto"
    assert config.workflow.max_retries == 2
    assert config.workflow.retry_delay_ms == 500


def test_provider_api_key_uses_default_env_name(monkeypatch):
    monkeypatch.setenv("HF_TOKEN", "secret")

    provider = ProviderConfig(
        name="huggingface",
        model="Qwen/Qwen3-Next-80B-A3B-Thinking",
        max_tokens=1024,
    )

    assert provider.api_key() == "secret"


def test_unknown_provider_requires_explicit_api_key_env():
    provider = ProviderConfig(
        name="custom",
        base_url="https://example.com/openai/v1",
        model="custom-model",
        max_tokens=1024,
    )

    with pytest.raises(ValueError, match="provider.api_key_env"):
        _ = provider.resolved_api_key_env
