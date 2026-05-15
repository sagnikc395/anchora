from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flowforge.config import DATABASE_URL, FLOWFORGE_DB_PATH
from flowforge.models import (
    InventorySnapshot,
    OrderSummary,
    PaymentRecordView,
    WarehouseRecordView,
)


DEFAULT_INVENTORY: dict[str, int] = {
    "SKU-001": 25,
    "SKU-002": 10,
    "SKU-003": 50,
}


def _db_path_from_url(database_url: str) -> str | None:
    if not database_url:
        return None
    if database_url == "sqlite:///:memory:":
        return ":memory:"
    if database_url.startswith("sqlite:///"):
        return database_url.removeprefix("sqlite:///")
    raise ValueError("Only sqlite:/// DATABASE_URL values are supported")


def _runtime_db_path() -> str:
    return _db_path_from_url(DATABASE_URL) or FLOWFORGE_DB_PATH


class FlowForgeDatabase:
    def __init__(self, db_path: str | Path = ":memory:") -> None:
        self.path = str(db_path)
        self._lock = asyncio.Lock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self.path,
            timeout=10,
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._initialize()

    @property
    def lock(self) -> asyncio.Lock:
        return self._lock

    @property
    def connection(self) -> sqlite3.Connection:
        return self._connection

    def _initialize(self) -> None:
        if self.path != ":memory:":
            self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA busy_timeout=10000")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS inventory_items (
                product_id TEXT PRIMARY KEY,
                available INTEGER NOT NULL CHECK (available >= 0),
                reserved INTEGER NOT NULL CHECK (reserved >= 0)
            );

            CREATE TABLE IF NOT EXISTS inventory_reservations (
                reservation_id TEXT PRIMARY KEY,
                product_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0)
            );

            CREATE TABLE IF NOT EXISTS payments (
                charge_id TEXT PRIMARY KEY,
                amount INTEGER NOT NULL CHECK (amount > 0),
                payment_method TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL CHECK (status IN ('charged', 'refunded'))
            );

            CREATE TABLE IF NOT EXISTS payment_counter (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS warehouse_records (
                order_id TEXT PRIMARY KEY,
                product_id TEXT NOT NULL,
                quantity INTEGER NOT NULL CHECK (quantity > 0),
                status TEXT NOT NULL CHECK (status IN ('applied', 'reverted'))
            );

            CREATE TABLE IF NOT EXISTS orders (
                workflow_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self._seed_defaults()

    def _seed_defaults(self) -> None:
        for product_id, available in DEFAULT_INVENTORY.items():
            self._connection.execute(
                """
                INSERT INTO inventory_items (product_id, available, reserved)
                VALUES (?, ?, 0)
                ON CONFLICT(product_id) DO NOTHING
                """,
                (product_id, available),
            )
        self._connection.execute(
            """
            INSERT INTO payment_counter (name, value)
            VALUES ('charges', 0)
            ON CONFLICT(name) DO NOTHING
            """
        )

    def reset(self) -> None:
        self._connection.executescript(
            """
            DELETE FROM inventory_reservations;
            DELETE FROM inventory_items;
            DELETE FROM payments;
            DELETE FROM payment_counter;
            DELETE FROM warehouse_records;
            DELETE FROM orders;
            """
        )
        self._seed_defaults()


class InventoryStore:
    def __init__(
        self,
        db_path: str | Path = ":memory:",
        database: FlowForgeDatabase | None = None,
    ) -> None:
        self._database = database or FlowForgeDatabase(db_path)

    async def check_stock(self, product_id: str, quantity: int) -> None:
        async with self._database.lock:
            item = self._get_or_create_item(product_id)
            if item["available"] < quantity:
                raise ValueError(f"Insufficient stock for {product_id}")

    async def reserve_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        async with self._database.lock:
            connection = self._database.connection
            if reservation_id is not None:
                existing = connection.execute(
                    "SELECT 1 FROM inventory_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                ).fetchone()
                if existing is not None:
                    return

            item = self._get_or_create_item(product_id)
            if item["available"] < quantity:
                raise ValueError(f"Insufficient stock for {product_id}")

            connection.execute(
                """
                UPDATE inventory_items
                SET available = available - ?, reserved = reserved + ?
                WHERE product_id = ?
                """,
                (quantity, quantity, product_id),
            )
            if reservation_id is not None:
                connection.execute(
                    """
                    INSERT INTO inventory_reservations
                        (reservation_id, product_id, quantity)
                    VALUES (?, ?, ?)
                    """,
                    (reservation_id, product_id, quantity),
                )

    async def release_stock(
        self, product_id: str, quantity: int, reservation_id: str | None = None
    ) -> None:
        async with self._database.lock:
            connection = self._database.connection
            if reservation_id is not None:
                reservation = connection.execute(
                    """
                    SELECT product_id, quantity
                    FROM inventory_reservations
                    WHERE reservation_id = ?
                    """,
                    (reservation_id,),
                ).fetchone()
                if reservation is None:
                    return
                connection.execute(
                    "DELETE FROM inventory_reservations WHERE reservation_id = ?",
                    (reservation_id,),
                )
                connection.execute(
                    """
                    UPDATE inventory_items
                    SET available = available + ?, reserved = reserved - ?
                    WHERE product_id = ?
                    """,
                    (
                        reservation["quantity"],
                        reservation["quantity"],
                        reservation["product_id"],
                    ),
                )
                return

            item = self._get_or_create_item(product_id)
            if item["reserved"] < quantity:
                raise ValueError(f"Cannot release more than reserved for {product_id}")
            connection.execute(
                """
                UPDATE inventory_items
                SET available = available + ?, reserved = reserved - ?
                WHERE product_id = ?
                """,
                (quantity, quantity, product_id),
            )

    async def snapshot(self) -> list[InventorySnapshot]:
        async with self._database.lock:
            rows = self._database.connection.execute(
                """
                SELECT product_id, available, reserved
                FROM inventory_items
                ORDER BY product_id
                """
            ).fetchall()
            return [
                InventorySnapshot(
                    product_id=row["product_id"],
                    available=row["available"],
                    reserved=row["reserved"],
                )
                for row in rows
            ]

    def _get_or_create_item(self, product_id: str) -> sqlite3.Row:
        connection = self._database.connection
        connection.execute(
            """
            INSERT INTO inventory_items (product_id, available, reserved)
            VALUES (?, 0, 0)
            ON CONFLICT(product_id) DO NOTHING
            """,
            (product_id,),
        )
        return connection.execute(
            """
            SELECT product_id, available, reserved
            FROM inventory_items
            WHERE product_id = ?
            """,
            (product_id,),
        ).fetchone()


class PaymentStore:
    def __init__(
        self,
        db_path: str | Path = ":memory:",
        database: FlowForgeDatabase | None = None,
    ) -> None:
        self._database = database or FlowForgeDatabase(db_path)

    async def charge(self, amount: int, payment_method: str, idempotency_key: str) -> str:
        async with self._database.lock:
            connection = self._database.connection
            existing = connection.execute(
                "SELECT charge_id FROM payments WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if existing is not None:
                return existing["charge_id"]

            current = connection.execute(
                "SELECT value FROM payment_counter WHERE name = 'charges'"
            ).fetchone()
            next_value = int(current["value"]) + 1
            charge_id = f"ch_{next_value:08d}"
            connection.execute(
                "UPDATE payment_counter SET value = ? WHERE name = 'charges'",
                (next_value,),
            )
            connection.execute(
                """
                INSERT INTO payments
                    (charge_id, amount, payment_method, idempotency_key, status)
                VALUES (?, ?, ?, ?, 'charged')
                """,
                (charge_id, amount, payment_method, idempotency_key),
            )
            return charge_id

    async def refund(self, charge_id: str) -> None:
        async with self._database.lock:
            cursor = self._database.connection.execute(
                "UPDATE payments SET status = 'refunded' WHERE charge_id = ?",
                (charge_id,),
            )
            if cursor.rowcount == 0:
                raise ValueError(f"Unknown charge {charge_id}")

    async def refund_by_idempotency_key(self, idempotency_key: str) -> None:
        async with self._database.lock:
            self._database.connection.execute(
                "UPDATE payments SET status = 'refunded' WHERE idempotency_key = ?",
                (idempotency_key,),
            )

    async def get(self, charge_id: str) -> PaymentRecordView | None:
        async with self._database.lock:
            row = self._database.connection.execute(
                """
                SELECT charge_id, amount, payment_method, idempotency_key, status
                FROM payments
                WHERE charge_id = ?
                """,
                (charge_id,),
            ).fetchone()
            return None if row is None else _payment_from_row(row)

    async def snapshot(self) -> list[PaymentRecordView]:
        async with self._database.lock:
            rows = self._database.connection.execute(
                """
                SELECT charge_id, amount, payment_method, idempotency_key, status
                FROM payments
                ORDER BY charge_id
                """
            ).fetchall()
            return [_payment_from_row(row) for row in rows]


class WarehouseStore:
    def __init__(
        self,
        db_path: str | Path = ":memory:",
        database: FlowForgeDatabase | None = None,
    ) -> None:
        self._database = database or FlowForgeDatabase(db_path)

    async def update(self, order_id: str, product_id: str, quantity: int) -> None:
        async with self._database.lock:
            self._database.connection.execute(
                """
                INSERT INTO warehouse_records
                    (order_id, product_id, quantity, status)
                VALUES (?, ?, ?, 'applied')
                ON CONFLICT(order_id) DO UPDATE SET
                    product_id = excluded.product_id,
                    quantity = excluded.quantity,
                    status = 'applied'
                """,
                (order_id, product_id, quantity),
            )

    async def revert(self, order_id: str) -> None:
        async with self._database.lock:
            self._database.connection.execute(
                "UPDATE warehouse_records SET status = 'reverted' WHERE order_id = ?",
                (order_id,),
            )

    async def snapshot(self) -> list[WarehouseRecordView]:
        async with self._database.lock:
            rows = self._database.connection.execute(
                """
                SELECT order_id, product_id, quantity, status
                FROM warehouse_records
                ORDER BY order_id
                """
            ).fetchall()
            return [
                WarehouseRecordView(
                    order_id=row["order_id"],
                    product_id=row["product_id"],
                    quantity=row["quantity"],
                    status=row["status"],
                )
                for row in rows
            ]


class WorkflowRegistry:
    def __init__(
        self,
        db_path: str | Path = ":memory:",
        database: FlowForgeDatabase | None = None,
    ) -> None:
        self._database = database or FlowForgeDatabase(db_path)

    async def record(self, workflow_id: str, order_id: str, status: str) -> None:
        async with self._database.lock:
            created_at = datetime.now(timezone.utc).isoformat()
            self._database.connection.execute(
                """
                INSERT INTO orders (workflow_id, order_id, status, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(workflow_id) DO UPDATE SET
                    order_id = excluded.order_id,
                    status = excluded.status
                """,
                (workflow_id, order_id, status, created_at),
            )

    async def get(self, workflow_id: str) -> OrderSummary | None:
        async with self._database.lock:
            row = self._database.connection.execute(
                """
                SELECT workflow_id, order_id, status, created_at
                FROM orders
                WHERE workflow_id = ?
                """,
                (workflow_id,),
            ).fetchone()
            return None if row is None else _order_from_row(row)

    async def list(self) -> list[OrderSummary]:
        async with self._database.lock:
            rows = self._database.connection.execute(
                """
                SELECT workflow_id, order_id, status, created_at
                FROM orders
                ORDER BY workflow_id
                """
            ).fetchall()
            return [_order_from_row(row) for row in rows]


def _payment_from_row(row: sqlite3.Row) -> PaymentRecordView:
    return PaymentRecordView(
        charge_id=row["charge_id"],
        amount=row["amount"],
        payment_method=row["payment_method"],
        idempotency_key=row["idempotency_key"],
        status=row["status"],
    )


def _order_from_row(row: sqlite3.Row) -> OrderSummary:
    return OrderSummary(
        workflow_id=row["workflow_id"],
        order_id=row["order_id"],
        status=row["status"],
        created_at=datetime.fromisoformat(row["created_at"]),
    )


runtime_database = FlowForgeDatabase(_runtime_db_path())
inventory_store = InventoryStore(database=runtime_database)
payment_store = PaymentStore(database=runtime_database)
warehouse_store = WarehouseStore(database=runtime_database)
workflow_registry = WorkflowRegistry(database=runtime_database)


async def reset_runtime_stores() -> None:
    async with runtime_database.lock:
        runtime_database.reset()
