import asyncio

from anchora.store import (
    AgentWorkflowRegistry,
    InventoryStore,
    PaymentStore,
    WarehouseStore,
)


def test_inventory_store_reserves_and_releases_stock() -> None:
    async def run() -> None:
        store = InventoryStore()
        await store.check_stock("SKU-001", 5)
        await store.reserve_stock("SKU-001", 5)
        snapshot = await store.snapshot()
        sku = next(item for item in snapshot if item.product_id == "SKU-001")
        assert sku.available == 20
        assert sku.reserved == 5

        await store.release_stock("SKU-001", 3)
        snapshot = await store.snapshot()
        sku = next(item for item in snapshot if item.product_id == "SKU-001")
        assert sku.available == 23
        assert sku.reserved == 2

    asyncio.run(run())


def test_inventory_reservations_are_idempotent_and_safe_to_compensate() -> None:
    async def run() -> None:
        store = InventoryStore()

        await store.release_stock("SKU-001", 5, "missing-reservation")
        await store.reserve_stock("SKU-001", 5, "ord_1")
        await store.reserve_stock("SKU-001", 5, "ord_1")

        snapshot = await store.snapshot()
        sku = next(item for item in snapshot if item.product_id == "SKU-001")
        assert sku.available == 20
        assert sku.reserved == 5

        await store.release_stock("SKU-001", 5, "ord_1")
        await store.release_stock("SKU-001", 5, "ord_1")
        snapshot = await store.snapshot()
        sku = next(item for item in snapshot if item.product_id == "SKU-001")
        assert sku.available == 25
        assert sku.reserved == 0

    asyncio.run(run())


def test_payment_store_is_idempotent() -> None:
    async def run() -> None:
        store = PaymentStore()
        first = await store.charge(1000, "tok_visa", "ord_1")
        second = await store.charge(1000, "tok_visa", "ord_1")
        assert first == second

        payment = await store.get(first)
        assert payment is not None
        assert payment.status == "charged"

        await store.refund(first)
        refunded = await store.get(first)
        assert refunded is not None
        assert refunded.status == "refunded"

    asyncio.run(run())


def test_payment_store_can_refund_by_idempotency_key() -> None:
    async def run() -> None:
        store = PaymentStore()

        await store.refund_by_idempotency_key("missing-order")
        charge_id = await store.charge(1000, "tok_visa", "ord_1")
        await store.refund_by_idempotency_key("ord_1")

        payment = await store.get(charge_id)
        assert payment is not None
        assert payment.status == "refunded"

    asyncio.run(run())


def test_warehouse_and_registry_snapshots() -> None:
    async def run() -> None:
        warehouse = WarehouseStore()
        registry = AgentWorkflowRegistry()

        await warehouse.update("ord_1", "SKU-001", 2)
        await warehouse.revert("ord_1")
        records = await warehouse.snapshot()
        assert records[0].status == "reverted"

        await registry.record("wf-1", "ord_1", "fulfillment-coordinator", "started")
        await registry.record("wf-1", "ord_1", "fulfillment-coordinator", "completed")
        workflows = await registry.list()
        assert workflows[0].status == "completed"
        assert workflows[0].agent_id == "fulfillment-coordinator"

    asyncio.run(run())


def test_store_state_persists_to_sqlite_file(tmp_path) -> None:
    async def run() -> None:
        db_path = tmp_path / "anchora.sqlite3"
        payments = PaymentStore(db_path)

        charge_id = await payments.charge(2500, "tok_visa", "ord_persist")

        reloaded = PaymentStore(db_path)
        payment = await reloaded.get(charge_id)
        assert payment is not None
        assert payment.idempotency_key == "ord_persist"

    asyncio.run(run())
