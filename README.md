# FlowForge

A distributed workflow engine built to handle multi-step transactional processes reliably , even when things go wrong.

## Why This Exists

Traditional request/response APIs struggle with multi-step processes. Consider what can go wrong in an order fulfillment flow:

- Payment succeeds, but the warehouse update times out
- The payment processor goes down mid-transaction
- A partial failure leaves your data in an inconsistent state

FlowForge solves this with the **Saga Pattern** backed by [Temporal.io](https://temporal.io). Each step is durable, every failure triggers a compensating action, and the system self-heals — without you writing any retry or rollback logic by hand.

## Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| API | FastAPI + Uvicorn | Order intake, async HTTP |
| Orchestration | Temporal.io (Python SDK) | Durable workflow execution |
| Payments | Stripe Mock | Payment processing simulation |
| Database | PostgreSQL | Warehouse inventory state |
| Async Runtime | Python AsyncIO | Non-blocking activity execution |
| Containerization | Docker + Docker Compose | Local dev environment |

## Project Structure

```
flowforge/
├── api/
│   ├── app.py               # FastAPI app — order endpoints
│   └── schemas.py           # Pydantic request/response models
├── workflows/
│   ├── workflows.py         # Core Temporal workflow definition
│   └── compensation.py      # Saga compensation logic
├── activities/
│   ├── activities.py        # Legacy hello-world demo activity
│   └── order.py             # Fulfillment activities + compensations
├── worker/
│   └── worker.py            # Temporal worker entrypoint
├── config.py                # Shared Temporal host/task queue settings
├── models.py                # Shared Pydantic workflow models
├── store.py                 # In-memory inventory/payment/warehouse state
├── mocks/
│   ├── stripe_mock.py       # Thin wrapper over the in-memory payment mock
│   └── inventory_api.py     # Thin wrapper over the in-memory inventory mock
├── db/
│   └── models.py            # Placeholder persistence models
├── tests/
│   ├── test_workflow.py     # Placeholder workflow tests
│   └── test_compensation.py # Placeholder compensation tests
├── Dockerfile
└── pyproject.toml
```

## How Compensation Works

Each activity registers a **compensating action** before it executes. If any downstream step fails, Temporal unwinds the saga in reverse order — so a failed warehouse update will automatically trigger a payment refund and a stock release, in the right order, every time.

## Getting Started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- [Temporal CLI](https://docs.temporal.io/cli) (for local dev server)

### 1. Clone & Install

```bash
git clone https://github.com/yourusername/flowforge.git
cd flowforge
python -m venv venv && source venv/bin/activate
pip install fastapi[standard] temporalio
```

### 2. Start Temporal Server (local)

```bash
temporal server start-dev
```

This starts a local Temporal server with a Web UI at `http://localhost:8233`.

### 3. Start the Worker

```bash
python -m flowforge.worker.worker
```

The worker polls Temporal for workflow and activity tasks.

### 4. Start the FastAPI Server

```bash
uvicorn flowforge.api.app:app --reload --port 8000
```

### 5. Place an Order

```bash
curl -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{
    "product_id": "SKU-001",
    "quantity": 2,
    "customer_id": "cust-42",
    "payment_method": "tok_visa"
  }'
```

### 6. Simulate a Failure

Set `FAIL_AT` to trigger compensation at a specific step:

```bash
FAIL_AT=warehouse python -m flowforge.worker.worker
```

Watch the Temporal Web UI at `http://localhost:8233` — you'll see the payment refund and stock release fire automatically in reverse order.

## API Reference

### `POST /orders`

Place a new order and kick off the fulfillment workflow.

**Request**
```json
{
  "product_id": "string",
  "quantity": 1,
  "customer_id": "string",
  "payment_method": "string"
}
```

**Response**
```json
{
  "workflow_id": "order-uuid-1234",
  "status": "started",
  "order_id": "ord_abc123"
}
```

### `GET /orders/{workflow_id}/status`

Poll the current state of a running or completed workflow.

**Response**
```json
{
  "workflow_id": "order-uuid-1234",
  "status": "completed | running | compensating | failed",
  "steps_completed": ["check_stock", "process_payment", "update_warehouse"],
  "compensation_triggered": false
}
```

## Key Design Decisions

**Durable Execution** — Temporal persists every workflow step to an event log. If the worker crashes mid-execution, the workflow resumes exactly where it left off on restart.

**Idempotency** — Every activity is safe to retry. Processing the same payment twice with the same idempotency key won't double-charge.

**Saga Compensation** — Failures never leave the system in a broken state. Every side effect has a registered undo operation that Temporal calls automatically.

**Non-blocking API** — `POST /orders` uses `start_workflow` (not `execute_workflow`), so the HTTP response returns immediately with a `workflow_id`. Workflows can run for minutes; clients poll for status separately.

**Async Activities** — All I/O (external APIs, DB writes) runs as non-blocking async activities, keeping throughput high under concurrent load.

**Visibility** — Every workflow run, activity execution, and compensation event is visible in the Temporal Web UI with full event history.

## Current State

The repository now contains a real fulfillment prototype, not just the original Temporal hello-world:

- `POST /orders` starts `FulfillmentWorkflow` asynchronously and returns a `workflow_id`
- `GET /orders/{workflow_id}/status` queries workflow state directly from Temporal
- The workflow runs `check_inventory`, `reserve_inventory`, `process_payment`, and `update_warehouse`
- Compensation runs in reverse order using `release_inventory`, `refund_payment`, and `revert_warehouse`
- `FAIL_AT` can inject a failure at `inventory-check`, `inventory`, `payment`, or `warehouse`

What remains incomplete:

- persistence is still in-memory, not PostgreSQL-backed
- mocks are simplistic and not external-service substitutes
- tests are placeholders, not comprehensive Temporal workflow tests
- there is no `docker-compose.yml` yet, despite the original design docs mentioning one
- the legacy hello-world files are still present alongside the new implementation

This means the project is aligned with the intended workflow design, but it is still a prototype and not production-ready.

## Latest Challenges

- The code compiles, but runtime validation could not be completed in-session because the active Python environment was missing `fastapi` and `temporalio`
- The documentation had drifted behind the codebase and still described the old `SayHelloWorkflow` scaffold
- The documented file layout and the actual repository layout did not match, so some compatibility modules had to be added
- A proper persistence layer was deferred to keep the implementation self-contained and runnable without introducing an unfinished database stack
- End-to-end verification still depends on running a Temporal dev server and installing project dependencies locally

## Running Tests

```bash
# Unit tests (no Temporal server needed)
pytest tests/ -v

# Integration tests (requires a running Temporal server)
TEMPORAL_HOST=localhost:7233 pytest tests/integration/ -v
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `TASK_QUEUE` | `fulfillment-queue` | Temporal task queue name |
| `DATABASE_URL` | `sqlite:///./flowforge.db` | SQLAlchemy DB connection string |
| `STRIPE_SECRET_KEY` | `sk_test_mock` | Stripe API key (mock in dev) |
| `FAIL_AT` | `""` | Inject a failure at a named step to test compensation |

## Deployment

Deployment scaffolding is still pending. The original design called for a `docker-compose.yml` that would start:

- Temporal server + Postgres
- FlowForge API
- FlowForge Worker
- Temporal Web UI

That file does not exist in the repository yet. For now, run the Temporal server, worker, and API as separate local processes. For production, deploy the API and Worker as separate services pointing to a managed [Temporal Cloud](https://temporal.io/cloud) instance or a self-hosted Temporal cluster.

## References

- [Temporal.io Python SDK Docs](https://docs.temporal.io/develop/python)
- [Saga Pattern — Microsoft Architecture Guide](https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/saga/saga)
- [FastAPI Async Docs](https://fastapi.tiangolo.com/async/)
