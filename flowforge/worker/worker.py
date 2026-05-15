from __future__ import annotations

import asyncio

from temporalio.client import Client
from temporalio.worker import Worker

from flowforge.activities.order import (
    check_inventory,
    process_payment,
    refund_payment,
    refund_payment_by_idempotency_key,
    release_inventory,
    reserve_inventory,
    revert_warehouse,
    update_warehouse,
)
from flowforge.config import (
    MAX_CONCURRENT_ACTIVITIES,
    MAX_CONCURRENT_WORKFLOW_TASKS,
    TASK_QUEUE,
    TEMPORAL_HOST,
)
from flowforge.workflows.workflows import FulfillmentWorkflow


async def main() -> None:
    client = await Client.connect(TEMPORAL_HOST)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[FulfillmentWorkflow],
        activities=[
            check_inventory,
            reserve_inventory,
            release_inventory,
            process_payment,
            refund_payment,
            refund_payment_by_idempotency_key,
            update_warehouse,
            revert_warehouse,
        ],
        max_concurrent_activities=MAX_CONCURRENT_ACTIVITIES,
        max_concurrent_workflow_tasks=MAX_CONCURRENT_WORKFLOW_TASKS,
    )
    print(f"Worker started on task queue: {TASK_QUEUE}")
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
