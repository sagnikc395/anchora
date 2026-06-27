from __future__ import annotations

import pytest

from anchora.state_machine import Event, Status, transition


def test_transition_allows_valid_step_lifecycle():
    status = transition(Status.PENDING, Event.START)
    status = transition(status, Event.FAIL)
    status = transition(status, Event.RETRY)
    status = transition(status, Event.START)
    status = transition(status, Event.SUCCEED)

    assert status == Status.SUCCEEDED


def test_transition_rejects_invalid_event():
    with pytest.raises(ValueError, match="invalid"):
        transition(Status.PENDING, Event.SUCCEED)
