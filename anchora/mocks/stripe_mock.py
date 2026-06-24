from __future__ import annotations

from anchora.store import payment_store


async def charge(amount: int, payment_method: str, idempotency_key: str) -> str:
    return await payment_store.charge(amount, payment_method, idempotency_key)


async def refund(charge_id: str) -> None:
    await payment_store.refund(charge_id)
