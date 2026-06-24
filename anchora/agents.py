from __future__ import annotations

from anchora.models import AgentProfile


FULFILLMENT_AGENT_ID = "fulfillment-coordinator"
INVENTORY_AGENT_ID = "inventory-agent"
PAYMENT_AGENT_ID = "payment-agent"
WAREHOUSE_AGENT_ID = "warehouse-agent"


_AGENT_PROFILES = [
    AgentProfile(
        agent_id=FULFILLMENT_AGENT_ID,
        role="Coordinates durable workflow execution, retries, and compensation.",
        owns_steps=["workflow", "compensation"],
    ),
    AgentProfile(
        agent_id=INVENTORY_AGENT_ID,
        role="Owns stock checks, reservations, and reservation release.",
        owns_steps=["check_inventory", "reserve_inventory", "release_inventory"],
    ),
    AgentProfile(
        agent_id=PAYMENT_AGENT_ID,
        role="Owns payment charges and refunds keyed by durable workflow order id.",
        owns_steps=["process_payment", "refund_payment", "refund_payment_by_idempotency_key"],
    ),
    AgentProfile(
        agent_id=WAREHOUSE_AGENT_ID,
        role="Owns warehouse application and rollback records.",
        owns_steps=["update_warehouse", "revert_warehouse"],
    ),
]

_STEP_AGENTS = {
    step: profile.agent_id
    for profile in _AGENT_PROFILES
    for step in profile.owns_steps
}


def agent_for_step(step_name: str) -> str:
    return _STEP_AGENTS.get(step_name, FULFILLMENT_AGENT_ID)


def list_agent_profiles() -> list[AgentProfile]:
    return [profile.model_copy(deep=True) for profile in _AGENT_PROFILES]
