package anchora

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// postgresStore is the durable Store: workflows, checkpoints, messages, and
// leases all live in the anchora schema.
type postgresStore struct {
	pool *pgxpool.Pool
}

var _ Store = (*postgresStore)(nil)

// scanner is satisfied by both pgx.Row and pgx.Rows.
type scanner interface {
	Scan(dest ...any) error
}

const workflowSelect = `
SELECT w.workflow_id, w.workflow_name, w.status, w.app_version, COALESCE(w.executor_id, ''), w.epoch,
       w.recovery_attempts, COALESCE(w.parent_workflow_id, ''), COALESCE(w.parent_function_id, 0),
       w.parent_function_id IS NOT NULL, COALESCE(w.forked_from, ''), COALESCE(w.continued_to, ''),
       COALESCE(w.queue_name, ''), w.priority, COALESCE(w.partition_key, ''),
       w.lease_expires_at, w.created_at, w.updated_at, w.started_at,
       i.inputs, i.output, i.error
FROM anchora.workflow_status w
LEFT JOIN anchora.workflow_io i ON i.workflow_id = w.workflow_id
WHERE w.workflow_id = $1`

func scanWorkflow(row scanner) (*WorkflowRecord, error) {
	var rec WorkflowRecord
	var status string
	var parentFunctionID int
	var hasParentFunction bool
	var errJSON []byte
	if err := row.Scan(
		&rec.ID, &rec.Name, &status, &rec.AppVersion, &rec.ExecutorID, &rec.Epoch,
		&rec.RecoveryAttempts, &rec.ParentWorkflowID, &parentFunctionID, &hasParentFunction,
		&rec.ForkedFrom, &rec.ContinuedTo, &rec.QueueName, &rec.Priority, &rec.PartitionKey,
		&rec.LeaseExpiresAt, &rec.CreatedAt, &rec.UpdatedAt, &rec.StartedAt,
		&rec.Inputs, &rec.Output, &errJSON,
	); err != nil {
		return nil, err
	}
	rec.Status = Status(status)
	if hasParentFunction {
		rec.ParentFunctionID = &parentFunctionID
	}
	if len(errJSON) > 0 {
		var info ErrorInfo
		if err := json.Unmarshal(errJSON, &info); err != nil {
			return nil, fmt.Errorf("anchora: decode stored workflow error: %w", err)
		}
		rec.Error = &info
	}
	return &rec, nil
}

func (s *postgresStore) CreateWorkflow(ctx context.Context, rec WorkflowRecord, lease time.Duration) (bool, *WorkflowRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, nil, err
	}
	defer tx.Rollback(ctx)

	// Queue-backed starts are stored as ENQUEUED with no owner; a dequeue
	// sweep claims them later. Everything else belongs to this executor now.
	enqueue := rec.Status == StatusEnqueued
	status, executor, leaseArg := "PENDING", rec.ExecutorID, lease.Milliseconds()
	if enqueue {
		status, executor, leaseArg = "ENQUEUED", "", 0
	}
	var id string
	err = tx.QueryRow(ctx, `
INSERT INTO anchora.workflow_status
    (workflow_id, workflow_name, status, app_version, executor_id, lease_expires_at, epoch,
     queue_name, priority, partition_key, parent_workflow_id, parent_function_id,
     forked_from, started_at)
VALUES ($1, $2, $10, $3, NULLIF($4, ''), CASE WHEN $10 = 'PENDING' THEN now() + ($5::bigint * interval '1 millisecond') END, 1,
        NULLIF($6, ''), $7, NULLIF($8, ''), NULLIF($9, ''), $11,
        NULLIF($12, ''), CASE WHEN $10 = 'PENDING' THEN now() END)
ON CONFLICT (workflow_id) DO NOTHING
RETURNING workflow_id`,
		rec.ID, rec.Name, rec.AppVersion, executor, leaseArg,
		rec.QueueName, rec.Priority, rec.PartitionKey, rec.ParentWorkflowID, status,
		rec.ParentFunctionID, rec.ForkedFrom).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, err := scanWorkflow(tx.QueryRow(ctx, workflowSelect, rec.ID))
		if err != nil {
			return false, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, nil, err
		}
		return false, existing, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("anchora: create workflow: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO anchora.workflow_io (workflow_id, inputs) VALUES ($1, $2)`, rec.ID, rec.Inputs); err != nil {
		return false, nil, fmt.Errorf("anchora: store workflow input: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

func (s *postgresStore) GetWorkflow(ctx context.Context, workflowID string) (*WorkflowRecord, error) {
	rec, err := scanWorkflow(s.pool.QueryRow(ctx, workflowSelect, workflowID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
	}
	if err != nil {
		return nil, err
	}
	return rec, nil
}

const checkpointSelect = `
SELECT function_id, function_name, kind, output, error, attempts, COALESCE(child_workflow_id, ''),
       started_at, completed_at
FROM anchora.operation_outputs
WHERE workflow_id = $1 AND function_id = $2`

func scanCheckpoint(row scanner) (*Checkpoint, error) {
	var cp Checkpoint
	var kind string
	var errJSON []byte
	if err := row.Scan(&cp.FunctionID, &cp.FunctionName, &kind, &cp.Output, &errJSON, &cp.Attempts,
		&cp.ChildWorkflowID, &cp.StartedAt, &cp.CompletedAt); err != nil {
		return nil, err
	}
	cp.Kind = Kind(kind)
	if len(errJSON) > 0 {
		var info ErrorInfo
		if err := json.Unmarshal(errJSON, &info); err != nil {
			return nil, fmt.Errorf("anchora: decode stored checkpoint error: %w", err)
		}
		cp.Err = &info
	}
	return &cp, nil
}

func (s *postgresStore) LoadCheckpoints(ctx context.Context, workflowID string) (map[int]*Checkpoint, error) {
	rows, err := s.pool.Query(ctx, `
SELECT function_id, function_name, kind, output, error, attempts, COALESCE(child_workflow_id, ''),
       started_at, completed_at
FROM anchora.operation_outputs
WHERE workflow_id = $1
ORDER BY function_id`, workflowID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	history := make(map[int]*Checkpoint)
	for rows.Next() {
		cp, err := scanCheckpoint(rows)
		if err != nil {
			return nil, err
		}
		history[cp.FunctionID] = cp
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return history, nil
}

// insertCheckpointSQL writes a checkpoint only while the executor still owns
// the workflow at the execution's epoch. Without a matching epoch no row is
// produced, which is the fence.
const insertCheckpointSQL = `
INSERT INTO anchora.operation_outputs
    (workflow_id, function_id, function_name, kind, output, error, attempts, child_workflow_id,
     started_at, completed_at)
SELECT $1, $3, $4, $5, $6, $7::jsonb, $8, NULLIF($9, ''), $10, $11
WHERE EXISTS (SELECT 1 FROM anchora.workflow_status WHERE workflow_id = $1 AND epoch = $2)
ON CONFLICT (workflow_id, function_id) DO NOTHING`

func insertCheckpointTx(ctx context.Context, tx pgx.Tx, workflowID string, epoch int64, cp *Checkpoint) (bool, error) {
	tag, err := tx.Exec(ctx, insertCheckpointSQL,
		workflowID, epoch, cp.FunctionID, cp.FunctionName, string(cp.Kind), cp.Output,
		encodeErrorJSON(cp.Err), cp.Attempts, cp.ChildWorkflowID, cp.StartedAt, cp.CompletedAt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func encodeErrorJSON(info *ErrorInfo) any {
	if info == nil {
		return nil
	}
	encoded, err := json.Marshal(info)
	if err != nil {
		return nil
	}
	return string(encoded)
}

// resolveCheckpoint explains why a checkpoint insert wrote nothing: either the
// slot was already recorded (in which case that record wins), or this executor
// was fenced out.
func resolveCheckpoint(ctx context.Context, tx pgx.Tx, workflowID string, epoch int64, functionID int) (*Checkpoint, error) {
	existing, err := scanCheckpoint(tx.QueryRow(ctx, checkpointSelect, workflowID, functionID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}
	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT epoch FROM anchora.workflow_status WHERE workflow_id = $1`, workflowID).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
	}
	if err != nil {
		return nil, err
	}
	if currentEpoch != epoch {
		return nil, ErrFenced
	}
	return nil, nil
}

func (s *postgresStore) SaveCheckpoint(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint) (bool, *Checkpoint, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, nil, err
	}
	defer tx.Rollback(ctx)

	created, err := insertCheckpointTx(ctx, tx, workflowID, epoch, cp)
	if err != nil {
		return false, nil, err
	}
	if created {
		if err := tx.Commit(ctx); err != nil {
			return false, nil, err
		}
		return true, nil, nil
	}
	existing, err := resolveCheckpoint(ctx, tx, workflowID, epoch, cp.FunctionID)
	if err != nil {
		return false, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, nil, err
	}
	if existing == nil {
		return false, nil, fmt.Errorf("anchora: checkpoint %s/%d was not written", workflowID, cp.FunctionID)
	}
	return false, existing, nil
}

func (s *postgresStore) CompleteWorkflow(ctx context.Context, workflowID string, epoch int64, status Status, output []byte, wfErr *ErrorInfo) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
UPDATE anchora.workflow_status
SET status = $3, updated_at = now(), lease_expires_at = NULL
WHERE workflow_id = $1 AND epoch = $2 AND status = 'PENDING'`,
		workflowID, epoch, string(status))
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE anchora.workflow_io SET output = $2, error = $3::jsonb WHERE workflow_id = $1`,
		workflowID, output, encodeErrorJSON(wfErr)); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *postgresStore) CancelWorkflow(ctx context.Context, workflowID string) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `
UPDATE anchora.workflow_status
SET status = 'CANCELLED', updated_at = now()
WHERE workflow_id = $1 AND status IN ('ENQUEUED', 'PENDING')`, workflowID)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	if _, err := tx.Exec(ctx, `
UPDATE anchora.workflow_io SET error = $2::jsonb WHERE workflow_id = $1`,
		workflowID, encodeErrorJSON(encodeError(ErrCancelled))); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (s *postgresStore) ClaimExpired(ctx context.Context, req ClaimExpiredRequest) ([]WorkflowRecord, error) {
	if req.Limit <= 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
WITH candidates AS (
    SELECT workflow_id
    FROM anchora.workflow_status
    WHERE status = 'PENDING'
      AND app_version = $2
      AND (lease_expires_at IS NULL OR lease_expires_at < now() OR ($5 AND executor_id = $1))
    ORDER BY lease_expires_at NULLS FIRST
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE anchora.workflow_status w
SET executor_id = $1,
    lease_expires_at = now() + ($3::bigint * interval '1 millisecond'),
    epoch = w.epoch + 1,
    recovery_attempts = w.recovery_attempts + 1,
    started_at = COALESCE(w.started_at, now()),
    updated_at = now()
FROM candidates c
WHERE w.workflow_id = c.workflow_id
RETURNING w.workflow_id`,
		req.ExecutorID, req.AppVersion, req.Lease.Milliseconds(), req.Limit, req.IncludeOwned)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	records := make([]WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := scanWorkflow(tx.QueryRow(ctx, workflowSelect, id))
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *postgresStore) RenewLeases(ctx context.Context, executorID string, lease time.Duration, workflowIDs []string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
UPDATE anchora.workflow_status
SET lease_expires_at = now() + ($2::bigint * interval '1 millisecond'), updated_at = now()
WHERE executor_id = $1 AND status = 'PENDING' AND workflow_id = ANY($3)
RETURNING workflow_id`,
		executorID, lease.Milliseconds(), workflowIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var owned []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		owned = append(owned, id)
	}
	return owned, rows.Err()
}

func (s *postgresStore) StartStepAttempt(ctx context.Context, workflowID string, functionID, attempt int, executorID string) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO anchora.step_attempts (workflow_id, function_id, attempt, executor_id)
VALUES ($1, $2, $3, $4)
ON CONFLICT (workflow_id, function_id, attempt)
DO UPDATE SET executor_id = EXCLUDED.executor_id, heartbeat_at = now()`,
		workflowID, functionID, attempt, executorID)
	return err
}

func (s *postgresStore) HeartbeatStep(ctx context.Context, workflowID string, functionID, attempt int, details []byte) error {
	_, err := s.pool.Exec(ctx, `
UPDATE anchora.step_attempts
SET heartbeat_at = now(), heartbeat_details = $4
WHERE workflow_id = $1 AND function_id = $2 AND attempt = $3`,
		workflowID, functionID, attempt, details)
	return err
}

func (s *postgresStore) TxStep(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint, fn func(context.Context, TxConn) error) (bool, *Checkpoint, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, nil, err
	}
	defer tx.Rollback(ctx)

	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT epoch FROM anchora.workflow_status WHERE workflow_id = $1`, workflowID).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil, fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
	}
	if err != nil {
		return false, nil, err
	}
	if currentEpoch != epoch {
		return false, nil, ErrFenced
	}
	// A recorded slot means this step already happened; its transaction must not
	// run a second time.
	prior, err := scanCheckpoint(tx.QueryRow(ctx, checkpointSelect, workflowID, cp.FunctionID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return false, nil, err
	}
	if prior != nil {
		if err := tx.Commit(ctx); err != nil {
			return false, nil, err
		}
		return false, prior, nil
	}
	if err := fn(ctx, tx); err != nil {
		return false, nil, err
	}
	created, err := insertCheckpointTx(ctx, tx, workflowID, epoch, cp)
	if err != nil {
		return false, nil, err
	}
	if !created {
		// Another executor committed the same slot first. Roll back this
		// executor's business writes so they happen exactly once.
		return false, nil, ErrFenced
	}
	if err := tx.Commit(ctx); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

func (s *postgresStore) DequeueWorkflows(ctx context.Context, req DequeueRequest) ([]WorkflowRecord, error) {
	if req.Limit <= 0 {
		return nil, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	// The queue lock serializes concurrent sweeps of the same queue so the
	// concurrency and rate-limit counts are read consistently.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, req.Queue); err != nil {
		return nil, err
	}
	pendingTotal := 0
	budget := req.Limit
	if req.GlobalConcurrency > 0 {
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM anchora.workflow_status WHERE queue_name = $1 AND status = 'PENDING'`, req.Queue).
			Scan(&pendingTotal); err != nil {
			return nil, err
		}
		if pendingTotal >= req.GlobalConcurrency {
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		}
		budget = min(budget, req.GlobalConcurrency-pendingTotal)
	}
	if req.RateLimitCount > 0 && req.RateLimitPeriod > 0 {
		var started int
		if err := tx.QueryRow(ctx, `
SELECT count(*) FROM anchora.workflow_status
WHERE queue_name = $1 AND started_at > now() - ($2::bigint * interval '1 millisecond')`,
			req.Queue, req.RateLimitPeriod.Milliseconds()).Scan(&started); err != nil {
			return nil, err
		}
		if started >= req.RateLimitCount {
			if err := tx.Commit(ctx); err != nil {
				return nil, err
			}
			return nil, nil
		}
		// A sweep may only spend the rate budget left in this window; it must
		// not start a full batch just because the check passed.
		budget = min(budget, req.RateLimitCount-started)
	}
	if budget <= 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	// Fairness across partitions: visit partitions in order of their oldest
	// enqueued workflow so one partition cannot starve the others.
	rows, err := tx.Query(ctx, `
SELECT partition_key, count(*) FILTER (WHERE status = 'PENDING')
FROM anchora.workflow_status
WHERE queue_name = $1 AND status IN ('ENQUEUED', 'PENDING')
GROUP BY 1
ORDER BY min(created_at)`, req.Queue)
	if err != nil {
		return nil, err
	}
	type partition struct {
		key     any // nil for the default partition
		pending int
	}
	var partitions []partition
	for rows.Next() {
		var p partition
		if err := rows.Scan(&p.key, &p.pending); err != nil {
			rows.Close()
			return nil, err
		}
		partitions = append(partitions, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var ids []string
	for _, p := range partitions {
		if len(ids) >= budget {
			break
		}
		take := budget - len(ids)
		if req.PartitionConcurrency > 0 {
			capacity := req.PartitionConcurrency - p.pending
			if capacity <= 0 {
				continue
			}
			if capacity < take {
				take = capacity
			}
		}
		claimed, err := tx.Query(ctx, `
WITH candidates AS (
    SELECT workflow_id
    FROM anchora.workflow_status
    WHERE queue_name = $1 AND status = 'ENQUEUED' AND app_version = $2
      AND partition_key IS NOT DISTINCT FROM $3::text
    ORDER BY priority DESC, created_at
    LIMIT $4
    FOR UPDATE SKIP LOCKED
)
UPDATE anchora.workflow_status w
SET status = 'PENDING', executor_id = $5,
    lease_expires_at = now() + ($6::bigint * interval '1 millisecond'),
    epoch = w.epoch + 1,
    started_at = COALESCE(w.started_at, now()),
    updated_at = now()
FROM candidates c
WHERE w.workflow_id = c.workflow_id
RETURNING w.workflow_id`,
			req.Queue, req.AppVersion, p.key, take, req.ExecutorID, req.Lease.Milliseconds())
		if err != nil {
			return nil, err
		}
		for claimed.Next() {
			var id string
			if err := claimed.Scan(&id); err != nil {
				claimed.Close()
				return nil, err
			}
			ids = append(ids, id)
		}
		claimed.Close()
		if err := claimed.Err(); err != nil {
			return nil, err
		}
	}
	if len(ids) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}
	records := make([]WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		rec, err := scanWorkflow(tx.QueryRow(ctx, workflowSelect, id))
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

func (s *postgresStore) UpsertQueue(ctx context.Context, q Queue) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO anchora.queues
    (name, global_concurrency, partition_concurrency, rate_limit_count, rate_limit_period)
VALUES ($1, NULLIF($2, 0), NULLIF($3, 0), NULLIF($4, 0),
        CASE WHEN $5::bigint > 0 THEN ($5::bigint * interval '1 millisecond') END)
ON CONFLICT (name) DO UPDATE SET
    global_concurrency = EXCLUDED.global_concurrency,
    partition_concurrency = EXCLUDED.partition_concurrency,
    rate_limit_count = EXCLUDED.rate_limit_count,
    rate_limit_period = EXCLUDED.rate_limit_period,
    updated_at = now()`,
		q.Name, q.GlobalConcurrency, q.PartitionConcurrency, q.RateLimitCount, q.RateLimitPeriod.Milliseconds())
	return err
}

func (s *postgresStore) SetWorkflowEvent(ctx context.Context, workflowID, key string, value []byte) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO anchora.workflow_events (workflow_id, key, value)
VALUES ($1, $2, $3)
ON CONFLICT (workflow_id, key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()`,
		workflowID, key, value)
	return err
}

func (s *postgresStore) GetWorkflowEvent(ctx context.Context, workflowID, key string) ([]byte, bool, error) {
	var value []byte
	err := s.pool.QueryRow(ctx,
		`SELECT value FROM anchora.workflow_events WHERE workflow_id = $1 AND key = $2`, workflowID, key).
		Scan(&value)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

func (s *postgresStore) AppendStream(ctx context.Context, workflowID, key string, chunk []byte) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	// Serialize appends to one stream so sequence numbers have no gaps.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1 || ':' || $2, 0))`, workflowID, key); err != nil {
		return 0, err
	}
	var seq int64
	if err := tx.QueryRow(ctx, `
INSERT INTO anchora.streams (workflow_id, stream_key, seq, chunk)
SELECT $1, $2, COALESCE(max(seq) + 1, 0), $3
FROM anchora.streams WHERE workflow_id = $1 AND stream_key = $2
RETURNING seq`, workflowID, key, chunk).Scan(&seq); err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return seq, nil
}

func (s *postgresStore) ReadStreams(ctx context.Context, workflowID, key string, afterSeq int64, limit int) ([]StreamChunk, error) {
	rows, err := s.pool.Query(ctx, `
SELECT seq, chunk, created_at FROM anchora.streams
WHERE workflow_id = $1 AND stream_key = $2 AND seq > $3
ORDER BY seq LIMIT $4`, workflowID, key, afterSeq, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var chunks []StreamChunk
	for rows.Next() {
		var c StreamChunk
		if err := rows.Scan(&c.Seq, &c.Chunk, &c.CreatedAt); err != nil {
			return nil, err
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}

func (s *postgresStore) ForkWorkflow(ctx context.Context, srcID, newID string, uptoFunctionID int, executorID string, lease time.Duration) (*WorkflowRecord, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
INSERT INTO anchora.workflow_status
    (workflow_id, workflow_name, status, app_version, executor_id, lease_expires_at, epoch, forked_from, started_at)
SELECT $2, workflow_name, 'PENDING', app_version, $3,
       now() + ($4::bigint * interval '1 millisecond'), 1, $1, now()
FROM anchora.workflow_status WHERE workflow_id = $1`,
		srcID, newID, executorID, lease.Milliseconds()); err != nil {
		return nil, fmt.Errorf("anchora: fork %s: %w", srcID, err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO anchora.workflow_io (workflow_id, inputs)
SELECT $2, inputs FROM anchora.workflow_io WHERE workflow_id = $1`, srcID, newID); err != nil {
		return nil, fmt.Errorf("anchora: fork %s: %w", srcID, err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO anchora.operation_outputs
    (workflow_id, function_id, function_name, kind, output, error, attempts, child_workflow_id, started_at, completed_at)
SELECT $2, function_id, function_name, kind, output, error, attempts, child_workflow_id, started_at, completed_at
FROM anchora.operation_outputs WHERE workflow_id = $1 AND function_id < $3`, srcID, newID, uptoFunctionID); err != nil {
		return nil, fmt.Errorf("anchora: fork %s: %w", srcID, err)
	}
	rec, err := scanWorkflow(tx.QueryRow(ctx, workflowSelect, newID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return rec, nil
}

func (s *postgresStore) SetContinuedTo(ctx context.Context, workflowID, continuedTo string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE anchora.workflow_status SET continued_to = $2, updated_at = now() WHERE workflow_id = $1`,
		workflowID, continuedTo)
	return err
}

func (s *postgresStore) ListWorkflows(ctx context.Context, req ListWorkflowsRequest) ([]WorkflowRecord, error) {
	if req.Limit <= 0 {
		req.Limit = 100
	}
	rows, err := s.pool.Query(ctx, `
SELECT w.workflow_id, w.workflow_name, w.status, w.app_version, COALESCE(w.executor_id, ''), w.epoch,
       w.recovery_attempts, COALESCE(w.parent_workflow_id, ''), COALESCE(w.parent_function_id, 0),
       w.parent_function_id IS NOT NULL, COALESCE(w.forked_from, ''), COALESCE(w.continued_to, ''),
       COALESCE(w.queue_name, ''), w.priority, COALESCE(w.partition_key, ''),
       w.lease_expires_at, w.created_at, w.updated_at, w.started_at,
       i.inputs, i.output, i.error
FROM anchora.workflow_status w
LEFT JOIN anchora.workflow_io i ON i.workflow_id = w.workflow_id
WHERE ($1 = '' OR w.status = $1)
  AND ($2 = '' OR w.queue_name = $2)
ORDER BY w.created_at DESC
LIMIT $3`, string(req.Status), req.Queue, req.Limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var records []WorkflowRecord
	for rows.Next() {
		rec, err := scanWorkflow(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, *rec)
	}
	return records, rows.Err()
}

func (s *postgresStore) DeleteTerminalWorkflowsBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
DELETE FROM anchora.workflow_status WHERE workflow_id IN (
    SELECT workflow_id FROM anchora.workflow_status
    WHERE status IN ('SUCCESS', 'ERROR', 'CANCELLED', 'RETRIES_EXCEEDED')
      AND updated_at < $1
    LIMIT $2)`, cutoff, limit)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *postgresStore) SendMessage(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint, msg Message) (bool, *Checkpoint, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, nil, err
	}
	defer tx.Rollback(ctx)

	created, err := insertCheckpointTx(ctx, tx, workflowID, epoch, cp)
	if err != nil {
		return false, nil, err
	}
	if !created {
		existing, err := resolveCheckpoint(ctx, tx, workflowID, epoch, cp.FunctionID)
		if err != nil {
			return false, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return false, nil, err
		}
		return false, existing, nil
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO anchora.notifications (message_id, destination_id, topic, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (message_id) DO NOTHING`,
		msg.ID, msg.DestinationID, msg.Topic, msg.Payload); err != nil {
		return false, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

func (s *postgresStore) ReceiveMessage(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint, destinationID, topic string) (*Message, bool, *Checkpoint, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, nil, err
	}
	defer tx.Rollback(ctx)

	var currentEpoch int64
	err = tx.QueryRow(ctx, `SELECT epoch FROM anchora.workflow_status WHERE workflow_id = $1`, workflowID).Scan(&currentEpoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil, fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
	}
	if err != nil {
		return nil, false, nil, err
	}
	if currentEpoch != epoch {
		return nil, false, nil, ErrFenced
	}
	prior, err := scanCheckpoint(tx.QueryRow(ctx, checkpointSelect, workflowID, cp.FunctionID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil, err
	}
	if prior != nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, false, nil, err
		}
		return nil, false, prior, nil
	}

	var msg Message
	err = tx.QueryRow(ctx, `
SELECT message_id, destination_id, topic, payload, created_at
FROM anchora.notifications
WHERE destination_id = $1 AND topic = $2 AND consumed_at IS NULL
ORDER BY created_at, message_id
LIMIT 1
FOR UPDATE SKIP LOCKED`, destinationID, topic).
		Scan(&msg.ID, &msg.DestinationID, &msg.Topic, &msg.Payload, &msg.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil, nil
	}
	if err != nil {
		return nil, false, nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE anchora.notifications SET consumed_at = now() WHERE message_id = $1`, msg.ID); err != nil {
		return nil, false, nil, err
	}
	cp.Output = msg.Payload
	if _, err := insertCheckpointTx(ctx, tx, workflowID, epoch, cp); err != nil {
		return nil, false, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, false, nil, err
	}
	return &msg, true, cp, nil
}
