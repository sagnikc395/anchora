from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Protocol

from anchora.state_machine import Event, Status, transition


class Agent(Protocol):
    def run(self, task: str, *, reset: bool = True) -> object:
        """Run an agent task and return the final answer."""


@dataclass
class Step:
    id: str
    agent: Agent
    prompt: str
    status: Status = Status.PENDING
    attempts: int = 0
    output: str = ""
    error: Exception | None = field(default=None, repr=False)


@dataclass
class Workflow:
    steps: list[Step]
    max_retries: int
    retry_delay_ms: int

    def run(self) -> None:
        for step in self.steps:
            try:
                self._run_step(step)
            except Exception as exc:
                raise RuntimeError(f'workflow failed at step "{step.id}": {exc}') from exc
            logging.info("[%s] output: %s", step.id, step.output)

    def _run_step(self, step: Step) -> None:
        self._apply(step, Event.START)

        while True:
            try:
                step.output = _as_text(step.agent.run(step.prompt, reset=True))
                self._apply(step, Event.SUCCEED)
                return
            except Exception as exc:
                step.error = exc

                if step.attempts >= self.max_retries:
                    self._apply(step, Event.FAIL)
                    raise RuntimeError(f'step "{step.id}" exhausted retries: {exc}') from exc

                self._apply(step, Event.FAIL)
                step.attempts += 1
                self._apply(step, Event.RETRY)
                self._apply(step, Event.START)

                delay = (step.attempts * self.retry_delay_ms) / 1000
                logging.info(
                    "[%s] retrying in %.3fs (attempt %d/%d)",
                    step.id,
                    delay,
                    step.attempts,
                    self.max_retries,
                )
                time.sleep(delay)

    @staticmethod
    def _apply(step: Step, event: Event) -> None:
        next_status = transition(step.status, event)
        logging.info("[%s] %s --> %s", step.id, step.status, next_status)
        step.status = next_status


def _as_text(value: object) -> str:
    if value is None:
        return ""
    return value if isinstance(value, str) else str(value)
