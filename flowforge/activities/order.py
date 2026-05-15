from __future__ import annotations

import os
from temporalio import activity

from flowforge.models import PaymentResult, WarehouseUpdate
from flowforge.integrations import runtime_integrations


def _should_fail(step: str) -> bool:
    return os.getenv("FAIL_AT", "").strip().lower() == step


@activity.defn
async def check_inventory(product_id: str, quantity: int) -> None:
    await runtime_integrations.inventory.check_stock(product_id, quantity)
    if _should_fail("inventory-check"):
        raise RuntimeError("Injected failure after inventory check")


@activity.defn
async def reserve_inventory(
    product_id: str, quantity: int, reservation_id: str | None = None
) -> None:
    await runtime_integrations.inventory.reserve_stock(
        product_id, quantity, reservation_id
    )
    if _should_fail("inventory"):
        raise RuntimeError("Injected failure after inventory reservation")


@activity.defn
async def release_inventory(
    product_id: str, quantity: int, reservation_id: str | None = None
) -> None:
    await runtime_integrations.inventory.release_stock(
        product_id, quantity, reservation_id
    )


@activity.defn
async def process_payment(
    customer_id: str, amount: int, payment_method: str, idempotency_key: str
) -> PaymentResult:
    del customer_id
    result = await runtime_integrations.payment.charge(
        amount, payment_method, idempotency_key
    )
    if _should_fail("payment"):
        raise RuntimeError("Injected failure after payment")
    return result


@activity.defn
async def refund_payment(charge_id: str) -> None:
    await runtime_integrations.payment.refund(charge_id)


@activity.defn
async def refund_payment_by_idempotency_key(idempotency_key: str) -> None:
    await runtime_integrations.payment.refund_by_idempotency_key(idempotency_key)


@activity.defn
async def update_warehouse(
    order_id: str, product_id: str, quantity: int
) -> WarehouseUpdate:
    result = await runtime_integrations.warehouse.update(order_id, product_id, quantity)
    if _should_fail("warehouse"):
        raise RuntimeError("Injected failure after warehouse update")
    return result


@activity.defn
async def revert_warehouse(order_id: str) -> None:
    await runtime_integrations.warehouse.revert(order_id)
