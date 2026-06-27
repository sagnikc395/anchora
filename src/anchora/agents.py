from __future__ import annotations

from smolagents import InferenceClientModel, LiteLLMModel, ToolCallingAgent, tool

from anchora.config import Config
from anchora.workflow import Step, Workflow


@tool
def summarize(text: str) -> str:
    """Summarize a given block of text.

    Args:
        text: Text to summarize.
    """
    return f"Summary: {text[:100]} [truncated]"


def build_model(config: Config) -> InferenceClientModel | LiteLLMModel:
    if config.provider.name == "huggingface":
        return InferenceClientModel(
            model_id=config.provider.model,
            provider=config.provider.inference_provider,
            token=config.provider.api_key(),
            base_url=config.provider.base_url,
            max_tokens=config.provider.max_tokens,
        )

    return LiteLLMModel(
        model_id=config.provider.litellm_model_id,
        api_base=config.provider.base_url,
        api_key=config.provider.api_key(),
        max_tokens=config.provider.max_tokens,
    )


def build_workflow(config: Config) -> Workflow:
    model = build_model(config)

    research_agent = ToolCallingAgent(
        tools=[],
        model=model,
        instructions="You are a research assistant. Be concise.",
        max_steps=3,
    )
    summary_agent = ToolCallingAgent(
        tools=[summarize],
        model=model,
        instructions="You are a summarization assistant.",
        max_steps=3,
    )

    return Workflow(
        max_retries=config.workflow.max_retries,
        retry_delay_ms=config.workflow.retry_delay_ms,
        steps=[
            Step(
                id="research",
                agent=research_agent,
                prompt="What are the three main uses of Go's select statement?",
            ),
            Step(
                id="summarize",
                agent=summary_agent,
                prompt="Summarize the key points of Go concurrency primitives in two sentences.",
            ),
        ],
    )
