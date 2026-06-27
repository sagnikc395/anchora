from __future__ import annotations

import pytest

from anchora.state_machine import Status
from anchora.workflow import Step, Workflow


class FakeAgent:
    def __init__(self, failures: int = 0):
        self.failures = failures
        self.calls = 0

    def run(self, task: str, *, reset: bool = True) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise RuntimeError("temporary failure")
        return f"done: {task}"


def test_workflow_runs_steps_in_order():
    step = Step(id="example", agent=FakeAgent(), prompt="work")
    workflow = Workflow(steps=[step], max_retries=0, retry_delay_ms=0)

    workflow.run()

    assert step.status == Status.SUCCEEDED
    assert step.output == "done: work"


def test_workflow_retries_before_succeeding():
    agent = FakeAgent(failures=2)
    step = Step(id="flaky", agent=agent, prompt="work")
    workflow = Workflow(steps=[step], max_retries=2, retry_delay_ms=0)

    workflow.run()

    assert agent.calls == 3
    assert step.attempts == 2
    assert step.status == Status.SUCCEEDED


def test_workflow_fails_after_retries_are_exhausted():
    step = Step(id="broken", agent=FakeAgent(failures=2), prompt="work")
    workflow = Workflow(steps=[step], max_retries=1, retry_delay_ms=0)

    with pytest.raises(RuntimeError, match="workflow failed"):
        workflow.run()

    assert step.status == Status.FAILED
