from __future__ import annotations

from enum import StrEnum


class Status(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    RETRYING = "retrying"


class Event(StrEnum):
    START = "start"
    SUCCEED = "succeed"
    FAIL = "fail"
    RETRY = "retry"


_TRANSITIONS: dict[Status, dict[Event, Status]] = {
    Status.PENDING: {
        Event.START: Status.RUNNING,
    },
    Status.RUNNING: {
        Event.SUCCEED: Status.SUCCEEDED,
        Event.FAIL: Status.FAILED,
    },
    Status.FAILED: {
        Event.RETRY: Status.RETRYING,
    },
    Status.RETRYING: {
        Event.START: Status.RUNNING,
    },
}


def transition(current: Status, event: Event) -> Status:
    events = _TRANSITIONS.get(current)
    if events is None:
        raise ValueError(f'no transitions from "{current}"')

    next_status = events.get(event)
    if next_status is None:
        raise ValueError(f'event "{event}" invalid from "{current}"')

    return next_status
