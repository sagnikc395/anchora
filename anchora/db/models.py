from __future__ import annotations

from dataclasses import dataclass


@dataclass
class InventoryItem:
    product_id: str
    quantity: int


@dataclass
class AgentWorkflow:
    order_id: str
    workflow_id: str
    agent_id: str
    status: str


@dataclass
class PaymentRecord:
    idempotency_key: str
    charge_id: str
    status: str
