from fastapi.testclient import TestClient

from anchora.api.app import app
from anchora.models import WorkflowState
from anchora.store import (
    payment_store,
    reset_runtime_stores,
    warehouse_store,
    workflow_registry,
)


class _FakeHandle:
    async def query(self, _query):
        return WorkflowState(
            order_id="ord_test",
            agent_id="fulfillment-coordinator",
            status="completed",
            current_step="update_warehouse",
            steps_completed=[
                "check_inventory",
                "reserve_inventory",
                "process_payment",
                "update_warehouse",
            ],
        )


class _FakeClient:
    def __init__(self) -> None:
        self.started = []

    async def start_workflow(self, workflow, args, id, task_queue):
        self.started.append(
            {"workflow": workflow, "args": args, "id": id, "task_queue": task_queue}
        )

    def get_workflow_handle(self, workflow_id):
        return _FakeHandle()


def test_agent_workflow_lifecycle_and_engine_snapshot(monkeypatch) -> None:
    import asyncio

    asyncio.run(reset_runtime_stores())
    fake_client = _FakeClient()

    async def fake_get_temporal_client():
        return fake_client

    monkeypatch.setattr("anchora.api.app.get_temporal_client", fake_get_temporal_client)
    app.dependency_overrides = {}
    client = TestClient(app)

    agents = client.get("/agents")
    assert agents.status_code == 200
    assert any(
        agent["agent_id"] == "fulfillment-coordinator" for agent in agents.json()
    )

    response = client.post(
        "/workflows",
        json={
            "product_id": "SKU-001",
            "quantity": 2,
            "customer_id": "cust-42",
            "payment_method": "tok_visa",
            "workflow_id": "wf-order-test",
        },
    )
    assert response.status_code == 200
    body = response.json()
    assert body["workflow_id"] == "wf-order-test"
    assert body["agent_id"] == "fulfillment-coordinator"

    status = client.get("/workflows/wf-order-test/status")
    assert status.status_code == 200
    assert status.json()["status"] == "completed"
    assert status.json()["agent_id"] == "fulfillment-coordinator"

    workflows = client.get("/workflows")
    assert workflows.status_code == 200
    assert any(
        workflow["workflow_id"] == "wf-order-test" for workflow in workflows.json()
    )

    snapshot = client.get("/engine/snapshot")
    assert snapshot.status_code == 200
    assert any(
        workflow["workflow_id"] == "wf-order-test"
        and workflow["status"] == "completed"
        for workflow in snapshot.json()["workflows"]
    )


def test_payment_and_warehouse_endpoints() -> None:
    import asyncio

    asyncio.run(reset_runtime_stores())
    client = TestClient(app)

    async def seed() -> str:
        seeded_charge_id = await payment_store.charge(1000, "tok_visa", "api-test-charge")
        await warehouse_store.update("ord_api", "SKU-001", 1)
        await workflow_registry.record(
            "wf-api", "ord_api", "fulfillment-coordinator", "running"
        )
        return seeded_charge_id

    seeded_charge_id = asyncio.run(seed())

    payment = client.get(f"/payments/{seeded_charge_id}")
    assert payment.status_code == 200
    assert payment.json()["charge_id"] == seeded_charge_id

    warehouse = client.get("/warehouse")
    assert warehouse.status_code == 200
    assert any(record["order_id"] == "ord_api" for record in warehouse.json())
