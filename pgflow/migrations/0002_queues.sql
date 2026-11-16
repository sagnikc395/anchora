-- Queues, events, streams, and fork/continuation bookkeeping.

ALTER TABLE pgflow.workflow_status
    ADD COLUMN IF NOT EXISTS queue_name   TEXT,
    ADD COLUMN IF NOT EXISTS priority     INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS partition_key TEXT,
    ADD COLUMN IF NOT EXISTS deadline_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS forked_from  TEXT,
    ADD COLUMN IF NOT EXISTS continued_to TEXT;

CREATE INDEX IF NOT EXISTS workflow_status_queue_idx
    ON pgflow.workflow_status (queue_name, status, priority, created_at)
    WHERE status = 'ENQUEUED';

-- Key/value events a workflow publishes for external callers and other
-- workflows (progress, partial results).
CREATE TABLE IF NOT EXISTS pgflow.workflow_events (
    workflow_id TEXT NOT NULL REFERENCES pgflow.workflow_status (workflow_id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    value       BYTEA NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, key)
);

-- Append-only streams (LLM token output, agent traces). Chunks are a
-- read-through side channel; the durable checkpoint stores the final value.
CREATE TABLE IF NOT EXISTS pgflow.streams (
    workflow_id TEXT NOT NULL REFERENCES pgflow.workflow_status (workflow_id) ON DELETE CASCADE,
    stream_key  TEXT NOT NULL,
    seq         BIGINT NOT NULL,
    chunk       BYTEA NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, stream_key, seq)
);

-- Queue configuration. An executor dequeues only queues it has registered,
-- so an application never drains another application's work.
CREATE TABLE IF NOT EXISTS pgflow.queues (
    name               TEXT PRIMARY KEY,
    global_concurrency INT,
    partition_concurrency INT,
    rate_limit_count   INT,
    rate_limit_period  INTERVAL,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
