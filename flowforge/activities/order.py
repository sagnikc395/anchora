from __future__ import annotations

import os
from temporalio import activity

from flowforge.models import PaymentResult, WarehouseUpdate
from flowforge.store import inventory_store, payment_store, warehouse_store


def _should_fail(step: str) -> bool:
    return os.getenv("FAIL_AT", "").strip().lower() == step


@activity.defn
async def check_inventory(product_id: str, quantity: int) -> None:
    await inventory_store.check_stock(product_id, quantity)
    if _should_fail("inventory-check"):
        raise RuntimeError("Injected failure after inventory check")


@activity.defn
async def reserve_inventory(
    product_id: str, quantity: int, reservation_id: str | None = None
) -> None:
    await inventory_store.reserve_stock(product_id, quantity, reservation_id)
    if _should_fail("inventory"):
        raise RuntimeError("Injected failure after inventory reservation")


@activity.defn
async def release_inventory(
    product_id: str, quantity: int, reservation_id: str | None = None
) -> None:
    await inventory_store.release_stock(product_id, quantity, reservation_id)


@activity.defn
async def process_payment(
    customer_id: str, amount: int, payment_method: str, idempotency_key: str
) -> PaymentResult:
    del customer_id
    charge_id = await payment_store.charge(amount, payment_method, idempotency_key)
    if _should_fail("payment"):
        raise RuntimeError("Injected failure after payment")
    return PaymentResult(charge_id=charge_id)


@activity.defn
async def refund_payment(charge_id: str) -> None:
    await payment_store.refund(charge_id)


@activity.defn
async def refund_payment_by_idempotency_key(idempotency_key: str) -> None:
    await payment_store.refund_by_idempotency_key(idempotency_key)


@activity.defn
async def update_warehouse(
    order_id: str, product_id: str, quantity: int
) -> WarehouseUpdate:
    await warehouse_store.update(order_id, product_id, quantity)
    if _should_fail("warehouse"):
        raise RuntimeError("Injected failure after warehouse update")
    return WarehouseUpdate(order_id=order_id, product_id=product_id, quantity=quantity)


@activity.defn
async def revert_warehouse(order_id: str) -> None:
    await warehouse_store.revert(order_id)
