from __future__ import annotations

from anchora.store import inventory_store


async def check_stock(product_id: str, quantity: int) -> None:
    await inventory_store.check_stock(product_id, quantity)


async def reserve_stock(product_id: str, quantity: int) -> None:
    await inventory_store.reserve_stock(product_id, quantity)


async def release_stock(product_id: str, quantity: int) -> None:
    await inventory_store.release_stock(product_id, quantity)
