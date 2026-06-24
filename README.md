# Anchora

Anchora is a small agent-native durable workflow prototype built with FastAPI
and Temporal. It models a real fulfillment flow where named agents own inventory
reservation, payment processing, warehouse updates, and saga-style compensation
when something fails.

The goal is not to hide failures. The goal is to make them recoverable.

## What It Does

An order moves through four steps:

1. Check inventory
2. Reserve inventory
3. Process payment
4. Update the warehouse

If a later step fails, Anchora runs compensating actions in reverse order. For
example, if payment succeeds but the warehouse update fails, the workflow refunds
the payment and releases the reserved stock.

Temporal owns durable workflow execution. FastAPI gives clients a simple HTTP
surface for starting agent workflows and checking state.

## Why This Exists

Multi-step business operations are awkward in a normal request/response API. A
single HTTP request can return a success or failure, but the real world is often
messier:

- a payment provider accepts the charge and then the warehouse service times out
- inventory is reserved, but the next system in the chain is unavailable
- a worker process crashes halfway through an order
- a retry runs the same side effect more than once

Anchora demonstrates how to handle that class of problem with the saga pattern.
Every side effect has an undo step, and the workflow records enough state to know
which agent owns the current step and what should happen next.

## Tech Stack

| Layer | Technology | Notes |
| --- | --- | --- |
| API | FastAPI | Starts agent workflows and exposes read endpoints |
| Workflow engine | Temporal Python SDK | Runs durable agent workflows |
| Worker | Temporal Worker | Executes workflow activities |
| Models | Pydantic | Request, response, and state schemas |
| State | SQLite-backed stores | Persistent local storage for agent workflows, inventory, payments, and warehouse state |
| Package manager | uv | Dependency and test runner |
| Containers | Docker Compose | Local Temporal, API, worker, starter, and test services |

## Repository Layout

```text
anchora/
├── api/
│   ├── app.py                 # FastAPI application and HTTP endpoints
│   └── schemas.py             # API schema exports
├── activities/
│   └── order.py               # Order activities and compensation activities
├── agents.py                  # Built-in agent identities and step ownership
├── integrations.py            # Swappable inventory, payment, and warehouse interfaces
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
│   ├── test_docker_config.py
│   ├── test_store.py
│   └── test_workflow.py
├── config.py                  # Environment-based runtime settings
├── models.py                  # Shared Pydantic models
└── store.py                   # SQLite-backed state stores
```

## How Compensation Works

The workflow registers compensation before it runs each side-effecting step.
That matters because an activity can mutate external state and then fail before
returning cleanly.

## Native Agents

Anchora records agent ownership directly in workflow state and event history.
The built-in agents are:

| Agent | Owns |
| --- | --- |
| `fulfillment-coordinator` | durable workflow orchestration, retries, and compensation |
| `inventory-agent` | stock checks, reservations, and reservation release |
| `payment-agent` | payment charges and refunds |
| `warehouse-agent` | warehouse records and rollback |

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
- Docker and Docker Compose, for the containerized local setup
- [Temporal CLI](https://docs.temporal.io/cli), for the non-Docker setup

## Docker Setup

The easiest way to run the full local stack is Docker Compose. It starts:

- Temporal dev server on `localhost:7233`
- Temporal UI on `http://localhost:8233`
- Anchora API on `http://localhost:8000`
- Anchora Temporal worker on the `fulfillment-queue` task queue
- Persistent Anchora state in the `anchora-state` Docker volume

Start the stack:

```bash
docker compose up --build
```

Run the test service in Docker:

```bash
docker compose --profile test run --rm tests
```

Run the one-shot demo starter against the Docker stack:

```bash
docker compose --profile demo run --rm starter
```

The project image has separate Dockerfile targets:

| Target | Purpose |
| --- | --- |
| `api` | Runs `python main.py api` |
| `worker` | Runs the Temporal worker |
| `starter` | Starts a sample workflow and waits for the result |
| `tests` | Runs `pytest -q` |

The Docker services use `TEMPORAL_HOST=temporal:7233` so containers connect to
Temporal over the Compose network. Local processes outside Docker should use the
default `localhost:7233`.

The API and worker share `DATABASE_URL=sqlite:////data/anchora.sqlite3` in
Docker so workflow side effects and HTTP read endpoints see the same persisted
state.

## Manual Setup

Use this path if you want to run the app directly on your machine without
Docker.

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
uv run python main.py worker
```

The worker listens on the configured task queue and executes workflow activities.

### Start the API

Run this in another terminal:

```bash
uv run python main.py api --port 8000
```

The API will be available at:

```text
http://localhost:8000
```

You can also start the API and worker in one process:

```bash
uv run python main.py all
```

## Try the Agent Workflow

Start a durable workflow:

```bash
curl -X POST http://localhost:8000/workflows \
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
  "workflow_id": "agent-workflow-ord_abc123",
  "order_id": "ord_abc123",
  "agent_id": "fulfillment-coordinator",
  "status": "started"
}
```

Check workflow status:

```bash
curl http://localhost:8000/workflows/agent-workflow-ord_abc123/status
```

Inspect the full engine snapshot:

```bash
curl http://localhost:8000/engine/snapshot
```

## Simulate Failures

Set `FAIL_AT` on the worker process to force a failure after a named step:

```bash
FAIL_AT=warehouse uv run python main.py worker
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

### `GET /agents`

Lists the built-in agents and the workflow steps they own.

### `POST /workflows`

Starts a new durable agent workflow.

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
  "agent_id": "fulfillment-coordinator",
  "status": "started"
}
```

### `GET /workflows`

Lists known durable workflows from the persisted workflow registry.

### `GET /workflows/{workflow_id}/status`

Queries Temporal for the current workflow state.

### `POST /orders`, `GET /orders`, `GET /orders/{workflow_id}/status`

Compatibility aliases for the workflow endpoints.

### `GET /inventory`

Returns the current inventory snapshot.

### `GET /payments/{charge_id}`

Returns a payment record by charge id.

### `GET /warehouse`

Returns warehouse records.

### `GET /engine/snapshot`

Returns agents, workflows, inventory, payments, and warehouse records in one
response.

## Running Tests

Run the local Python test suite:

```bash
uv run pytest
```

Run the live Temporal integration test against a running Temporal server:

```bash
RUN_TEMPORAL_INTEGRATION=1 uv run pytest anchora/tests/test_temporal_integration.py
```

Run the containerized test target:

```bash
docker compose --profile test run --rm tests
```

The default suite covers the API surface, persistent stores, workflow state,
compensation ordering, and Docker/Compose configuration. The live Temporal test
is opt-in so normal test runs do not require an external Temporal server.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `TEMPORAL_HOST` | `localhost:7233` | Temporal server address |
| `TASK_QUEUE` | `fulfillment-queue` | Temporal task queue name |
| `MAX_CONCURRENT_ACTIVITIES` | `100` | Worker activity concurrency |
| `MAX_CONCURRENT_WORKFLOW_TASKS` | `100` | Worker workflow task concurrency |
| `DATABASE_URL` | empty | Optional SQLite URL, for example `sqlite:////data/anchora.sqlite3` |
| `ANCHORA_DB_PATH` | `.anchora/anchora.sqlite3` | Local SQLite path when `DATABASE_URL` is empty |
| `FAIL_AT` | empty | Optional failure injection point |

## Current Limitations

Anchora is still a prototype. The core saga behavior and persistence behavior
are real, but several production concerns are intentionally out of scope for
now:

- PostgreSQL models are only placeholders
- payment and inventory integrations are local mocks
- the live Temporal tests are opt-in rather than part of every unit run

## Good Next Steps

- replace the placeholder PostgreSQL model layer with a production database adapter
- add migrations for schema changes
- add API authentication and request-level authorization
- expose workflow cancellation and retry controls through the API
- add observability for activity retries, compensation events, and workflow latency

## References

- [Temporal Python SDK](https://docs.temporal.io/develop/python)
- [Temporal CLI](https://docs.temporal.io/cli)
- [FastAPI](https://fastapi.tiangolo.com/)
- [Saga pattern overview](https://learn.microsoft.com/en-us/azure/architecture/reference-architectures/saga/saga)
