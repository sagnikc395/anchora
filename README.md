# FlowForge

FlowForge is a small fulfillment workflow prototype built with FastAPI and
Temporal. It models a real order flow with inventory reservation, payment
processing, warehouse updates, and saga-style compensation when something fails.

The goal is not to hide failures. The goal is to make them recoverable.

## What It Does

An order moves through four steps:

1. Check inventory
2. Reserve inventory
3. Process payment
4. Update the warehouse

If a later step fails, FlowForge runs compensating actions in reverse order. For
example, if payment succeeds but the warehouse update fails, the workflow refunds
the payment and releases the reserved stock.

Temporal owns the durable workflow execution. FastAPI gives clients a simple HTTP
surface for starting orders and checking state.

## Why This Exists

Multi-step business operations are awkward in a normal request/response API. A
single HTTP request can return a success or failure, but the real world is often
messier:

- a payment provider accepts the charge and then the warehouse service times out
- inventory is reserved, but the next system in the chain is unavailable
- a worker process crashes halfway through an order
- a retry runs the same side effect more than once

FlowForge demonstrates how to handle that class of problem with the saga pattern.
Every side effect has an undo step, and the workflow records enough state to know
what should happen next.

## Tech Stack

| Layer | Technology | Notes |
| --- | --- | --- |
| API | FastAPI | Starts workflows and exposes read endpoints |
| Workflow engine | Temporal Python SDK | Runs durable order workflows |
| Worker | Temporal Worker | Executes workflow activities |
| Models | Pydantic | Request, response, and state schemas |
| State | In-memory stores | Prototype storage for inventory, payments, warehouse, and order summaries |
| Package manager | uv | Dependency and test runner |

## Repository Layout

```text
flowforge/
├── api/
│   ├── app.py                 # FastAPI application and HTTP endpoints
│   └── schemas.py             # API schema exports
├── activities/
│   ├── order.py               # Order activities and compensation activities
│   └── activities.py          # Legacy demo activity
├── workflows/
│   ├── workflows.py           # FulfillmentWorkflow
│   └── compensation.py        # Saga compensation helper
├── worker/
│   └── worker.py              # Temporal worker entrypoint
├── mocks/
│   ├── inventory_api.py       # Thin inventory mock wrapper
│   └── stripe_mock.py         # Thin payment mock wrapper
├── tests/
│   ├── test_api.py
│   ├── test_compensation.py
│   ├── test_store.py
│   └── test_workflow.py
├── config.py                  # Environment-based runtime settings
├── models.py                  # Shared Pydantic models
└── store.py                   # In-memory state stores
```

## How Compensation Works

The workflow registers compensation before it runs each side-effecting step.
That matters because an activity can mutate external state and then fail before
returning cleanly.

Current compensation behavior:

| Step | Side effect | Compensation |
| --- | --- | --- |
| `reserve_inventory` | reserves stock by order id | `release_inventory` |
| `process_payment` | charges by idempotency key | `refund_payment_by_idempotency_key` |
| `update_warehouse` | writes a warehouse record | `revert_warehouse` |

Compensations are intentionally idempotent where possible. Retrying a reservation
or running a missing refund should not corrupt the prototype state.

## Getting Started

### Prerequisites

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- [Temporal CLI](https://docs.temporal.io/cli)

### Install Dependencies

```bash
uv sync
```

### Start Temporal

```bash
temporal server start-dev
```

Temporal's local UI will be available at:

```text
http://localhost:8233
```

### Start the Worker

Run this in a separate terminal:

```bash
uv run python -m flowforge.worker.worker
```

The worker listens on the configured task queue and executes workflow activities.

### Start the API

Run this in another terminal:

```bash
uv run uvicorn flowforge.api.app:app --reload --port 8000
```

The API will be available at:

```text
http://localhost:8000
```

## Try the Order Flow

Create an order:

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

Example response:

```json
{
  "workflow_id": "order-ord_abc123",
  "order_id": "ord_abc123",
  "status": "started"
}
```

Check workflow status:

```bash
curl http://localhost:8000/orders/order-ord_abc123/status
```

Inspect the full in-memory engine snapshot:

```bash
curl http://localhost:8000/engine/snapshot
```

## Simulate Failures

Set `FAIL_AT` on the worker process to force a failure after a named step:

```bash
FAIL_AT=warehouse uv run python -m flowforge.worker.worker
```

Supported values:

| Value | Failure point |
| --- | --- |
| `inventory-check` | after inventory check |
| `inventory` | after inventory reservation |
| `payment` | after payment charge |
| `warehouse` | after warehouse update |

When a failure is injected after a side effect, the workflow should move into
compensation and unwind the completed side effects in reverse order.

## API Reference

### `GET /health`

Basic API health check.

```json
{
  "status": "ok"
}
```

### `POST /orders`

Starts a new fulfillment workflow.

Request:

```json
{
  "product_id": "SKU-001",
  "quantity": 2,
  "customer_id": "cust-42",
  "payment_method": "tok_visa",
  "workflow_id": "optional-client-provided-id"
}
```

Response:

```json
{
  "workflow_id": "optional-client-provided-id",
  "order_id": "ord_abc123",
  "status": "started"
}
```

### `GET /orders`

Lists known orders from the in-memory workflow registry.

### `GET /orders/{workflow_id}/status`

Queries Temporal for the current workflow state.

### `GET /inventory`

Returns the current inventory snapshot.

### `GET /payments/{charge_id}`

Returns a payment record by charge id.

### `GET /warehouse`

Returns warehouse records.

### `GET /engine/snapshot`

Returns orders, inventory, payments, and warehouse records in one response.

## Running Tests

```bash
uv run pytest
```

The current suite covers the API surface, in-memory stores, workflow state, and
compensation ordering. It does not yet run a full Temporal integration test with
a live worker and Temporal server.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `TASK_QUEUE` | `fulfillment-queue` | Temporal task queue name |
| `MAX_CONCURRENT_ACTIVITIES` | `100` | Worker activity concurrency |
| `MAX_CONCURRENT_WORKFLOW_TASKS` | `100` | Worker workflow task concurrency |
| `FAIL_AT` | empty | Optional failure injection point |

## Current Limitations

FlowForge is still a prototype. The core saga behavior is real, but several
production concerns are intentionally out of scope for now:

- state is in memory and disappears when the process exits
- PostgreSQL models are only placeholders
- payment and inventory integrations are local mocks
- there is no Docker Compose environment yet
- there are no live Temporal integration tests yet
- the legacy hello-world activity still exists beside the fulfillment flow

## Good Next Steps

- add a real persistence layer for order, inventory, payment, and warehouse state
- add a `docker-compose.yml` for Temporal, API, worker, and backing services
- move mock integrations behind interfaces that can be swapped in tests
- add live Temporal integration tests for successful and compensated workflows
- remove the legacy demo activity once it is no longer useful

## References

- [Temporal Python SDK](https://docs.temporal.io/develop/python)
- [Temporal CLI](https://docs.temporal.io/cli)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Saga pattern overview](https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/saga/saga)
