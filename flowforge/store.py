from __future__ import annotations

import asyncio
from dataclasses import dataclass

from flowforge.models import (
    InventorySnapshot,
    OrderSummary,
    PaymentRecordView,
    WarehouseRecordView,
)


@dataclass
class _InventoryState:
    available: int = 0
    reserved: int = 0


@dataclass(frozen=True)
class _ReservationState:
    product_id: str
    quantity: int


class InventoryStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._items: dict[str, _InventoryState] = {
            "SKU-001": _InventoryState(available=25),
            "SKU-002": _InventoryState(available=10),
            "SKU-003": _InventoryState(available=50),
        }
        self._reservations: dict[str, _ReservationState] = {}

    async def check_stock(self, product_id: str, quantity: int) -> None:
        async with self._lock:
            item = self._items.setdefault(product_id, _InventoryState())
            if item.available < quantity:
                raise ValueError(f"Insufficient stock for {product_id}")

    async def reserve_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        async with self._lock:
            if reservation_id is not None and reservation_id in self._reservations:
                return
            item = self._items.setdefault(product_id, _InventoryState())
            if item.available < quantity:
                raise ValueError(f"Insufficient stock for {product_id}")
            item.available -= quantity
            item.reserved += quantity
            if reservation_id is not None:
                self._reservations[reservation_id] = _ReservationState(
                    product_id=product_id,
                    quantity=quantity,
                )

    async def release_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        async with self._lock:
            if reservation_id is not None:
                reservation = self._reservations.pop(reservation_id, None)
                if reservation is None:
                    return
                item = self._items.setdefault(reservation.product_id, _InventoryState())
                item.available += reservation.quantity
                item.reserved -= reservation.quantity
                return

            item = self._items.setdefault(product_id, _InventoryState())
            if item.reserved < quantity:
                raise ValueError(f"Cannot release more than reserved for {product_id}")
            item.available += quantity
            item.reserved -= quantity

    async def snapshot(self) -> list[InventorySnapshot]:
        async with self._lock:
            return [
                InventorySnapshot(
                    product_id=product_id,
                    available=item.available,
                    reserved=item.reserved,
                )
                for product_id, item in sorted(self._items.items())
            ]


class PaymentStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._charges_by_key: dict[str, PaymentRecordView] = {}
        self._charges_by_id: dict[str, PaymentRecordView] = {}
        self._counter = 0

    async def charge(self, amount: int, payment_method: str, idempotency_key: str) -> str:
        async with self._lock:
            existing = self._charges_by_key.get(idempotency_key)
            if existing is not None:
                return existing.charge_id

            self._counter += 1
            charge_id = f"ch_{self._counter:08d}"
            record = PaymentRecordView(
                charge_id=charge_id,
                amount=amount,
                payment_method=payment_method,
                idempotency_key=idempotency_key,
                status="charged",
            )
            self._charges_by_key[idempotency_key] = record
            self._charges_by_id[charge_id] = record
            return charge_id

    async def refund(self, charge_id: str) -> None:
        async with self._lock:
            record = self._charges_by_id.get(charge_id)
            if record is None:
                raise ValueError(f"Unknown charge {charge_id}")
            record.status = "refunded"

    async def refund_by_idempotency_key(self, idempotency_key: str) -> None:
        async with self._lock:
            record = self._charges_by_key.get(idempotency_key)
            if record is None:
                return
            record.status = "refunded"

    async def get(self, charge_id: str) -> PaymentRecordView | None:
        async with self._lock:
            record = self._charges_by_id.get(charge_id)
            return None if record is None else record.model_copy(deep=True)

    async def snapshot(self) -> list[PaymentRecordView]:
        async with self._lock:
            return [
                self._charges_by_id[charge_id].model_copy(deep=True)
                for charge_id in sorted(self._charges_by_id)
            ]


class WarehouseStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._orders: dict[str, WarehouseRecordView] = {}

    async def update(self, order_id: str, product_id: str, quantity: int) -> None:
        async with self._lock:
            self._orders[order_id] = WarehouseRecordView(
                order_id=order_id,
                product_id=product_id,
                quantity=quantity,
                status="applied",
            )

    async def revert(self, order_id: str) -> None:
        async with self._lock:
            record = self._orders.get(order_id)
            if record is None:
                return
            record.status = "reverted"

    async def snapshot(self) -> list[WarehouseRecordView]:
        async with self._lock:
            return [
                self._orders[order_id].model_copy(deep=True)
                for order_id in sorted(self._orders)
            ]


class WorkflowRegistry:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._orders: dict[str, OrderSummary] = {}

    async def record(self, workflow_id: str, order_id: str, status: str) -> None:
        async with self._lock:
            summary = self._orders.get(workflow_id)
            if summary is None:
                self._orders[workflow_id] = OrderSummary(
                    workflow_id=workflow_id,
                    order_id=order_id,
                    status=status,
                )
                return
            summary.status = status

    async def get(self, workflow_id: str) -> OrderSummary | None:
        async with self._lock:
            summary = self._orders.get(workflow_id)
            return None if summary is None else summary.model_copy(deep=True)

    async def list(self) -> list[OrderSummary]:
        async with self._lock:
            return [
                self._orders[workflow_id].model_copy(deep=True)
                for workflow_id in sorted(self._orders)
            ]


inventory_store = InventoryStore()
payment_store = PaymentStore()
warehouse_store = WarehouseStore()
workflow_registry = WorkflowRegistry()
