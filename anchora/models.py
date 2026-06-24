from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field


class AgentProfile(BaseModel):
    agent_id: str
    role: str
    owns_steps: list[str] = Field(default_factory=list)


class OrderRequest(BaseModel):
    product_id: str = Field(..., min_length=1)
    quantity: int = Field(..., gt=0)
    customer_id: str = Field(..., min_length=1)
    payment_method: str = Field(..., min_length=1)
    workflow_id: str | None = None


class AgentWorkflowResponse(BaseModel):
    workflow_id: str
    order_id: str
    agent_id: str
    status: str


class AgentWorkflowStatusResponse(BaseModel):
    workflow_id: str
    order_id: str
    agent_id: str
    status: str
    steps_completed: list[str]
    compensation_triggered: bool
    current_step: str | None = None
    failure_reason: str | None = None
    events: list["WorkflowEvent"] = Field(default_factory=list)


OrderResponse = AgentWorkflowResponse
WorkflowStatusResponse = AgentWorkflowStatusResponse


class InventoryReservation(BaseModel):
    product_id: str
    quantity: int


class InventorySnapshot(BaseModel):
    product_id: str
    available: int
    reserved: int


class PaymentResult(BaseModel):
    charge_id: str
    status: str = "charged"


class PaymentRecordView(BaseModel):
    charge_id: str
    amount: int
    payment_method: str
    idempotency_key: str
    status: Literal["charged", "refunded"]


class WarehouseUpdate(BaseModel):
    order_id: str
    product_id: str
    quantity: int


class WarehouseRecordView(BaseModel):
    order_id: str
    product_id: str
    quantity: int
    status: Literal["applied", "reverted"]


class WorkflowEvent(BaseModel):
    step: str
    agent_id: str | None = None
    status: Literal["started", "completed", "failed", "compensating", "compensated"]
    message: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class WorkflowState(BaseModel):
    order_id: str
    agent_id: str = "fulfillment-coordinator"
    status: str
    current_step: str | None = None
    steps_completed: list[str] = Field(default_factory=list)
    compensation_triggered: bool = False
    failure_reason: str | None = None
    events: list[WorkflowEvent] = Field(default_factory=list)

    def record_event(
        self,
        step: str,
        status: Literal["started", "completed", "failed", "compensating", "compensated"],
        message: str,
        agent_id: str | None = None,
    ) -> None:
        self.current_step = step
        self.events.append(
            WorkflowEvent(step=step, agent_id=agent_id, status=status, message=message)
        )


class AgentWorkflowSummary(BaseModel):
    workflow_id: str
    order_id: str
    agent_id: str
    status: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


OrderSummary = AgentWorkflowSummary


class EngineSnapshot(BaseModel):
    agents: list[AgentProfile]
    workflows: list[AgentWorkflowSummary]
    inventory: list[InventorySnapshot]
    payments: list[PaymentRecordView]
    warehouse: list[WarehouseRecordView]
