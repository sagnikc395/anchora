from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from temporalio import workflow

from anchora.agents import agent_for_step
from anchora.models import WorkflowState


@dataclass
class CompensationAction:
    activity_name: str
    args: Sequence[object] = field(default_factory=tuple)


class SagaCompensator:
    def __init__(self) -> None:
        self._actions: list[CompensationAction] = []

    def add(self, activity_name: str, *args: object) -> None:
        self._actions.append(CompensationAction(activity_name=activity_name, args=args))

    async def compensate(self, state: WorkflowState | None = None) -> None:
        for action in reversed(self._actions):
            agent_id = agent_for_step(action.activity_name)
            if state is not None:
                state.record_event(
                    action.activity_name,
                    "compensating",
                    f"Running compensation step {action.activity_name}",
                    agent_id,
                )
            await workflow.execute_activity(
                action.activity_name,
                args=list(action.args),
                schedule_to_close_timeout=timedelta(seconds=10),
            )
            if state is not None:
                state.record_event(
                    action.activity_name,
                    "compensated",
                    f"Compensation step {action.activity_name} completed",
                    agent_id,
                )
