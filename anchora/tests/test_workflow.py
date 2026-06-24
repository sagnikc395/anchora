import asyncio

from anchora.models import OrderRequest, WorkflowState
from anchora.workflows.workflows import FulfillmentWorkflow


def test_workflow_state_records_events_and_current_step() -> None:
    state = WorkflowState(order_id="ord_123", status="running")

    state.record_event(
        "check_inventory", "started", "checking inventory", "inventory-agent"
    )
    state.record_event(
        "check_inventory", "completed", "inventory ok", "inventory-agent"
    )

    assert state.current_step == "check_inventory"
    assert len(state.events) == 2
    assert state.events[0].status == "started"
    assert state.events[0].agent_id == "inventory-agent"
    assert state.events[1].status == "completed"
    assert state.steps_completed == []


def test_workflow_compensates_side_effects_after_activity_failure(monkeypatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    async def fake_execute_activity(name, args, **_kwargs):
        calls.append((name, args))
        if name == "process_payment":
            raise RuntimeError("Injected failure after payment")
        return None

    monkeypatch.setattr(
        "anchora.workflows.workflows.workflow.execute_activity",
        fake_execute_activity,
    )
    workflow = FulfillmentWorkflow()

    state = asyncio.run(
        workflow.run(
            "ord_123",
            OrderRequest(
                product_id="SKU-001",
                quantity=2,
                customer_id="cust-42",
                payment_method="tok_visa",
            ),
        )
    )

    assert state.status == "failed"
    assert state.agent_id == "fulfillment-coordinator"
    assert state.compensation_triggered is True
    assert any(event.agent_id == "payment-agent" for event in state.events)
    assert calls == [
        ("check_inventory", ["SKU-001", 2]),
        ("reserve_inventory", ["SKU-001", 2, "ord_123"]),
        ("process_payment", ["cust-42", 2000, "tok_visa", "ord_123"]),
        ("refund_payment_by_idempotency_key", ["ord_123"]),
        ("release_inventory", ["SKU-001", 2, "ord_123"]),
    ]
