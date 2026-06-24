from __future__ import annotations

import asyncio
import uuid

from temporalio.client import Client

from anchora.config import TASK_QUEUE, TEMPORAL_HOST
from anchora.api.schemas import OrderRequest
from anchora.workflows.workflows import FulfillmentWorkflow


async def main() -> None:
    client = await Client.connect(TEMPORAL_HOST)
    order_id = f"ord_{uuid.uuid4().hex[:12]}"
    workflow_id = f"agent-workflow-{order_id}"
    order = OrderRequest(
        product_id="SKU-001",
        quantity=2,
        customer_id="cust-42",
        payment_method="tok_visa",
    )

    handle = await client.start_workflow(
        FulfillmentWorkflow.run,
        args=[order_id, order],
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )

    print(f"Started agent workflow: {handle.id}")
    result = await handle.result()
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    asyncio.run(main())
