from anchora.models import WorkflowState
from anchora.workflows.compensation import SagaCompensator


def test_compensator_registers_actions() -> None:
    compensator = SagaCompensator()
    compensator.add("release_inventory", "SKU-001", 2)

    assert len(compensator._actions) == 1
    assert compensator._actions[0].activity_name == "release_inventory"


def test_workflow_state_supports_compensation_events() -> None:
    state = WorkflowState(order_id="ord_123", status="compensating")
    state.record_event("refund_payment", "compensating", "Running compensation step")
    state.record_event("refund_payment", "compensated", "Compensation completed")

    assert [event.status for event in state.events] == ["compensating", "compensated"]
