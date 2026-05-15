from __future__ import annotations

import asyncio
import os
from contextlib import suppress
from datetime import timedelta

import pytest
from temporalio.client import Client
from temporalio.worker import Worker

from flowforge.config import TEMPORAL_HOST
from flowforge.models import OrderRequest
from flowforge.store import reset_runtime_stores
from flowforge.worker.worker import ORDER_ACTIVITIES
from flowforge.workflows.workflows import FulfillmentWorkflow


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_TEMPORAL_INTEGRATION") != "1",
    reason="requires RUN_TEMPORAL_INTEGRATION=1 and a live Temporal server",
)


def test_live_temporal_success_and_compensation(monkeypatch) -> None:
    async def run() -> None:
        await reset_runtime_stores()
        client = await Client.connect(TEMPORAL_HOST)
        task_queue = f"flowforge-test-{os.getpid()}"
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[FulfillmentWorkflow],
            activities=ORDER_ACTIVITIES,
        )
        worker_task = asyncio.create_task(worker.run())

        try:
            order = OrderRequest(
                product_id="SKU-001",
                quantity=1,
                customer_id="cust-test",
                payment_method="tok_visa",
            )
            success = await client.execute_workflow(
                FulfillmentWorkflow.run,
                args=["ord_integration_success", order],
                id="wf-integration-success",
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
            )
            assert success.status == "completed"

            monkeypatch.setenv("FAIL_AT", "warehouse")
            failed = await client.execute_workflow(
                FulfillmentWorkflow.run,
                args=["ord_integration_fail", order],
                id="wf-integration-fail",
                task_queue=task_queue,
                execution_timeout=timedelta(seconds=30),
            )
            assert failed.status == "failed"
            assert failed.compensation_triggered is True
            assert "update_warehouse" in failed.steps_completed
        finally:
            monkeypatch.delenv("FAIL_AT", raising=False)
            worker_task.cancel()
            with suppress(asyncio.CancelledError):
                await worker_task

    asyncio.run(run())
