from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from anchora.models import PaymentResult, WarehouseUpdate
from anchora.store import inventory_store, payment_store, warehouse_store


class InventoryIntegration(Protocol):
    async def check_stock(self, product_id: str, quantity: int) -> None:
        ...

    async def reserve_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        ...

    async def release_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        ...


class PaymentGateway(Protocol):
    async def charge(
        self, amount: int, payment_method: str, idempotency_key: str
    ) -> PaymentResult:
        ...

    async def refund(self, charge_id: str) -> None:
        ...

    async def refund_by_idempotency_key(self, idempotency_key: str) -> None:
        ...


class WarehouseIntegration(Protocol):
    async def update(
        self, order_id: str, product_id: str, quantity: int
    ) -> WarehouseUpdate:
        ...

    async def revert(self, order_id: str) -> None:
        ...


@dataclass
class StoreInventoryIntegration:
    async def check_stock(self, product_id: str, quantity: int) -> None:
        await inventory_store.check_stock(product_id, quantity)

    async def reserve_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        await inventory_store.reserve_stock(product_id, quantity, reservation_id)

    async def release_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        await inventory_store.release_stock(product_id, quantity, reservation_id)


@dataclass
class StorePaymentGateway:
    async def charge(
        self, amount: int, payment_method: str, idempotency_key: str
    ) -> PaymentResult:
        charge_id = await payment_store.charge(amount, payment_method, idempotency_key)
        return PaymentResult(charge_id=charge_id)

    async def refund(self, charge_id: str) -> None:
        await payment_store.refund(charge_id)

    async def refund_by_idempotency_key(self, idempotency_key: str) -> None:
        await payment_store.refund_by_idempotency_key(idempotency_key)


@dataclass
class StoreWarehouseIntegration:
    async def update(
        self, order_id: str, product_id: str, quantity: int
    ) -> WarehouseUpdate:
        await warehouse_store.update(order_id, product_id, quantity)
        return WarehouseUpdate(order_id=order_id, product_id=product_id, quantity=quantity)

    async def revert(self, order_id: str) -> None:
        await warehouse_store.revert(order_id)


@dataclass
class RuntimeIntegrations:
    inventory: InventoryIntegration
    payment: PaymentGateway
    warehouse: WarehouseIntegration


runtime_integrations = RuntimeIntegrations(
    inventory=StoreInventoryIntegration(),
    payment=StorePaymentGateway(),
    warehouse=StoreWarehouseIntegration(),
)


def set_runtime_integrations(integrations: RuntimeIntegrations) -> None:
    runtime_integrations.inventory = integrations.inventory
    runtime_integrations.payment = integrations.payment
    runtime_integrations.warehouse = integrations.warehouse
