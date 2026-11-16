CREATE SCHEMA IF NOT EXISTS pgflow;

-- One row per workflow execution. The row is the workflow.
CREATE TABLE IF NOT EXISTS pgflow.workflow_status (
    workflow_id        TEXT PRIMARY KEY,
    workflow_name      TEXT NOT NULL,
    status             TEXT NOT NULL,
    app_version        TEXT NOT NULL,
    executor_id        TEXT,
    lease_expires_at   TIMESTAMPTZ,
    epoch              BIGINT NOT NULL DEFAULT 0,
    recovery_attempts  INT NOT NULL DEFAULT 0,
    parent_workflow_id TEXT,
    parent_function_id INT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at         TIMESTAMPTZ,
    CONSTRAINT workflow_status_valid CHECK (status IN
        ('ENQUEUED', 'PENDING', 'SUCCESS', 'ERROR', 'CANCELLED', 'RETRIES_EXCEEDED'))
);

CREATE INDEX IF NOT EXISTS workflow_status_recovery_idx
    ON pgflow.workflow_status (status, lease_expires_at);
CREATE INDEX IF NOT EXISTS workflow_status_parent_idx
    ON pgflow.workflow_status (parent_workflow_id);

-- Inputs and outputs are separated from the status row so frequent status
-- updates never rewrite the payloads.
CREATE TABLE IF NOT EXISTS pgflow.workflow_io (
    workflow_id TEXT PRIMARY KEY REFERENCES pgflow.workflow_status (workflow_id) ON DELETE CASCADE,
    inputs      BYTEA NOT NULL,
    output      BYTEA,
    error       JSONB
);

-- The event log: one row per completed engine operation.
CREATE TABLE IF NOT EXISTS pgflow.operation_outputs (
    workflow_id       TEXT NOT NULL REFERENCES pgflow.workflow_status (workflow_id) ON DELETE CASCADE,
    function_id       INT NOT NULL,
    function_name     TEXT NOT NULL,
    kind              TEXT NOT NULL,
    output            BYTEA,
    error             JSONB,
    attempts          INT NOT NULL DEFAULT 1,
    child_workflow_id TEXT,
    started_at        TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, function_id)
);

-- In-flight step attempts, for observability and heartbeats.
CREATE TABLE IF NOT EXISTS pgflow.step_attempts (
    workflow_id       TEXT NOT NULL REFERENCES pgflow.workflow_status (workflow_id) ON DELETE CASCADE,
    function_id       INT NOT NULL,
    attempt           INT NOT NULL,
    executor_id       TEXT NOT NULL,
    heartbeat_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_details BYTEA,
    PRIMARY KEY (workflow_id, function_id, attempt)
);

-- Durable messages between workflows.
CREATE TABLE IF NOT EXISTS pgflow.notifications (
    message_id     TEXT PRIMARY KEY,
    destination_id TEXT NOT NULL,
    topic          TEXT NOT NULL DEFAULT '',
    payload        BYTEA NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    consumed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS notifications_pending_idx
    ON pgflow.notifications (destination_id, topic, created_at)
    WHERE consumed_at IS NULL;
