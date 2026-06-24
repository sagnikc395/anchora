from __future__ import annotations

import uuid

from fastapi import FastAPI, HTTPException
from temporalio.client import Client, WorkflowHandle
from temporalio.service import RPCError

from anchora.agents import FULFILLMENT_AGENT_ID, list_agent_profiles
from anchora.config import TASK_QUEUE, TEMPORAL_HOST
from anchora.api.schemas import OrderRequest, OrderResponse, WorkflowStatusResponse
from anchora.models import (
    AgentWorkflowResponse,
    AgentWorkflowStatusResponse,
    AgentWorkflowSummary,
    AgentProfile,
    EngineSnapshot,
    InventorySnapshot,
    PaymentRecordView,
    WarehouseRecordView,
)
from anchora.store import (
    inventory_store,
    payment_store,
    warehouse_store,
    workflow_registry,
)
from anchora.workflows.workflows import FulfillmentWorkflow

app = FastAPI(title="Anchora")


async def get_temporal_client() -> Client:
    return await Client.connect(TEMPORAL_HOST)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


async def _start_agent_workflow(order: OrderRequest) -> AgentWorkflowResponse:
    client = await get_temporal_client()
    order_id = f"ord_{uuid.uuid4().hex[:12]}"
    workflow_id = order.workflow_id or f"agent-workflow-{order_id}"
    existing_workflow = await workflow_registry.get(workflow_id)
    if existing_workflow is not None:
        raise HTTPException(status_code=409, detail="Workflow ID already exists")
    workflow_order = order.model_copy(update={"workflow_id": workflow_id})
    await client.start_workflow(
        FulfillmentWorkflow.run,
        args=[order_id, workflow_order],
        id=workflow_id,
        task_queue=TASK_QUEUE,
    )
    await workflow_registry.record(
        workflow_id,
        order_id,
        FULFILLMENT_AGENT_ID,
        "started",
    )
    return AgentWorkflowResponse(
        workflow_id=workflow_id,
        order_id=order_id,
        agent_id=FULFILLMENT_AGENT_ID,
        status="started",
    )


async def _get_status_by_workflow_id(workflow_id: str) -> AgentWorkflowStatusResponse:
    client = await get_temporal_client()
    handle: WorkflowHandle = client.get_workflow_handle(workflow_id)
    try:
        state = await handle.query(FulfillmentWorkflow.get_status)
    except RPCError as exc:
        raise HTTPException(status_code=404, detail="Workflow not found") from exc

    await workflow_registry.record(
        workflow_id,
        state.order_id,
        state.agent_id,
        state.status,
    )
    return AgentWorkflowStatusResponse(workflow_id=workflow_id, **state.model_dump())


@app.get("/agents", response_model=list[AgentProfile])
async def list_agents() -> list[AgentProfile]:
    return list_agent_profiles()


@app.post("/workflows", response_model=AgentWorkflowResponse)
async def create_workflow(order: OrderRequest) -> AgentWorkflowResponse:
    return await _start_agent_workflow(order)


@app.get("/workflows", response_model=list[AgentWorkflowSummary])
async def list_workflows() -> list[AgentWorkflowSummary]:
    return await workflow_registry.list()


@app.get("/workflows/{workflow_id}/status", response_model=AgentWorkflowStatusResponse)
async def get_workflow_status(workflow_id: str) -> AgentWorkflowStatusResponse:
    return await _get_status_by_workflow_id(workflow_id)


@app.post("/orders", response_model=OrderResponse)
async def create_order(order: OrderRequest) -> OrderResponse:
    return await _start_agent_workflow(order)


@app.get("/orders", response_model=list[AgentWorkflowSummary])
async def list_orders() -> list[AgentWorkflowSummary]:
    return await workflow_registry.list()


@app.get("/orders/{workflow_id}/status", response_model=WorkflowStatusResponse)
async def get_order_status(workflow_id: str) -> WorkflowStatusResponse:
    return await _get_status_by_workflow_id(workflow_id)


@app.get("/inventory", response_model=list[InventorySnapshot])
async def get_inventory() -> list[InventorySnapshot]:
    return await inventory_store.snapshot()


@app.get("/payments/{charge_id}", response_model=PaymentRecordView)
async def get_payment(charge_id: str) -> PaymentRecordView:
    payment = await payment_store.get(charge_id)
    if payment is None:
        raise HTTPException(status_code=404, detail="Payment not found")
    return payment


@app.get("/warehouse", response_model=list[WarehouseRecordView])
async def get_warehouse() -> list[WarehouseRecordView]:
    return await warehouse_store.snapshot()


@app.get("/engine/snapshot", response_model=EngineSnapshot)
async def get_engine_snapshot() -> EngineSnapshot:
    return EngineSnapshot(
        agents=list_agent_profiles(),
        workflows=await workflow_registry.list(),
        inventory=await inventory_store.snapshot(),
        payments=await payment_store.snapshot(),
        warehouse=await warehouse_store.snapshot(),
    )
