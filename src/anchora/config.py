from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


_DEFAULT_API_KEY_ENV = {
    "groq": "GROQ_API_KEY",
    "huggingface": "HF_TOKEN",
    "openai": "OPENAI_API_KEY",
}


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    model: str
    max_tokens: int
    base_url: str | None = None
    inference_provider: str | None = None
    api_key_env: str | None = None

    @property
    def resolved_api_key_env(self) -> str:
        if self.api_key_env:
            return self.api_key_env

        try:
            return _DEFAULT_API_KEY_ENV[self.name]
        except KeyError as exc:
            raise ValueError(
                f'unknown provider "{self.name}" -- set provider.api_key_env in config.yaml'
            ) from exc

    def api_key(self) -> str:
        env_name = self.resolved_api_key_env
        value = os.getenv(env_name)
        if not value:
            raise ValueError(f'env var "{env_name}" is not set')
        return value

    @property
    def litellm_model_id(self) -> str:
        if "/" in self.model:
            return self.model
        return f"{self.name}/{self.model}"


@dataclass(frozen=True)
class WorkflowConfig:
    max_retries: int
    retry_delay_ms: int


@dataclass(frozen=True)
class Config:
    provider: ProviderConfig
    workflow: WorkflowConfig


def load_config(path: str | Path) -> Config:
    config_path = Path(path)
    try:
        raw = yaml.safe_load(config_path.read_text()) or {}
    except OSError as exc:
        raise ValueError(f"open config: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"decode config: {exc}") from exc

    provider_raw = _mapping(raw.get("provider"), "provider")
    workflow_raw = _mapping(raw.get("workflow"), "workflow")

    provider = ProviderConfig(
        name=_required_str(provider_raw, "provider.name"),
        model=_required_str(provider_raw, "provider.model"),
        max_tokens=_required_int(provider_raw, "provider.max_tokens", default=1024),
        base_url=_optional_str(provider_raw, "provider.base_url"),
        inference_provider=_optional_str(provider_raw, "provider.inference_provider"),
        api_key_env=_optional_str(provider_raw, "provider.api_key_env"),
    )
    workflow = WorkflowConfig(
        max_retries=_required_int(workflow_raw, "workflow.max_retries", default=2),
        retry_delay_ms=_required_int(workflow_raw, "workflow.retry_delay_ms", default=500),
    )

    return Config(provider=provider, workflow=workflow)


def _mapping(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"config: {name} is required")
    return value


def _required_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key.rsplit(".", 1)[-1])
    if not isinstance(value, str) or not value:
        raise ValueError(f"config: {key} is required")
    return value


def _optional_str(data: dict[str, Any], key: str) -> str | None:
    value = data.get(key.rsplit(".", 1)[-1])
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"config: {key} must be a non-empty string")
    return value


def _required_int(data: dict[str, Any], key: str, *, default: int) -> int:
    value = data.get(key.rsplit(".", 1)[-1], default)
    if not isinstance(value, int):
        raise ValueError(f"config: {key} must be an integer")
    if value < 0:
        raise ValueError(f"config: {key} must be non-negative")
    return value
