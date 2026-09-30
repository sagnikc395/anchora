package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sagnikc395/anchora/internal/workflow"
)

// Store is the durable record of every job, step, and event.
//
// It is also the authority on ownership. The Redis queue decides who gets to
// look at a job next; this store decides who is actually allowed to run it, by
// way of a lease held in workflow_jobs.owner / lease_expires_at. Every write a
// worker makes while running a job is fenced on that lease, so a worker that
// was declared dead and had its job handed to someone else cannot corrupt the
// new owner's progress even if it is still running.
type Store struct{ pool *pgxpool.Pool }

// WorkerInfo is one row of the worker registry.
type WorkerInfo struct {
	ID          string    `json:"id"`
	Hostname    string    `json:"hostname"`
	PID         int       `json:"pid"`
	Queue       string    `json:"queue"`
	StartedAt   time.Time `json:"started_at"`
	LastBeatAt  time.Time `json:"last_heartbeat_at"`
	JobsClaimed int64     `json:"jobs_claimed"`
}

// NewStore connects to PostgreSQL, verifies the connection, and applies the
// schema. The caller owns the returned store and must Close it.
func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect PostgreSQL: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	store := &Store{pool: pool}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate PostgreSQL: %w", err)
	}
	return store, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

// schema is additive so it can run against a database created by an earlier
// version of Anchora.
const schema = `
CREATE TABLE IF NOT EXISTS workflow_jobs (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ
);
ALTER TABLE workflow_jobs ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_jobs ADD COLUMN IF NOT EXISTS owner TEXT;
ALTER TABLE workflow_jobs ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;
ALTER TABLE workflow_jobs ADD COLUMN IF NOT EXISTS error TEXT NOT NULL DEFAULT '';
ALTER TABLE workflow_jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
CREATE INDEX IF NOT EXISTS workflow_jobs_lease ON workflow_jobs(status, lease_expires_at);

CREATE TABLE IF NOT EXISTS workflow_steps (
  job_id TEXT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE,
  id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  agent TEXT NOT NULL,
  prompt TEXT NOT NULL,
  depends_on JSONB NOT NULL DEFAULT '[]',
  status TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0,
  output TEXT NOT NULL DEFAULT '',
  error TEXT NOT NULL DEFAULT '',
  PRIMARY KEY(job_id,id)
);
ALTER TABLE workflow_steps ADD COLUMN IF NOT EXISTS owner TEXT;
ALTER TABLE workflow_steps ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE workflow_steps ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS workflow_events (
  id BIGSERIAL PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE,
  type TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS workflow_events_job_id_id ON workflow_events(job_id, id);

CREATE TABLE IF NOT EXISTS workflow_workers (
  id TEXT PRIMARY KEY,
  hostname TEXT NOT NULL DEFAULT '',
  pid INTEGER NOT NULL DEFAULT 0,
  queue TEXT NOT NULL DEFAULT '',
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  jobs_claimed BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS workflow_workers_heartbeat ON workflow_workers(last_heartbeat_at);
`

func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schema)
	return err
}

// Create records a job and its steps in one transaction, so a job is never
// visible without the steps it is made of.
func (s *Store) Create(ctx context.Context, job *Job) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO workflow_jobs (id,status,created_at,updated_at) VALUES ($1,$2,$3,$3)`, job.ID, job.Status, job.CreatedAt); err != nil {
		return err
	}
	for i, step := range job.Steps {
		deps, err := json.Marshal(step.DependsOn)
		if err != nil {
			return err
		}
		result := job.Results[i]
		if _, err = tx.Exec(ctx, `INSERT INTO workflow_steps (job_id,id,ordinal,agent,prompt,depends_on,status,attempts,output,error) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
			job.ID, step.ID, i, step.Agent, step.Prompt, deps, result.Status, result.Attempts, result.Output, result.Error); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// Get loads a job with its steps in declaration order. A job that does not
// exist is reported as a nil job and a nil error.
func (s *Store) Get(ctx context.Context, id string) (*Job, error) {
	job := &Job{ID: id}
	var started, finished, lease *time.Time
	var owner *string
	err := s.pool.QueryRow(ctx, `SELECT status,created_at,started_at,finished_at,attempts,owner,lease_expires_at,error FROM workflow_jobs WHERE id=$1`, id).
		Scan(&job.Status, &job.CreatedAt, &started, &finished, &job.Attempts, &owner, &lease, &job.Error)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	job.StartedAt, job.FinishedAt, job.LeaseExpiresAt = started, finished, lease
	if owner != nil {
		job.Owner = *owner
	}
	rows, err := s.pool.Query(ctx, `SELECT id,agent,prompt,depends_on,status,attempts,output,error FROM workflow_steps WHERE job_id=$1 ORDER BY ordinal`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var step Step
		var deps []byte
		var result workflow.StepResult
		if err := rows.Scan(&step.ID, &step.Agent, &step.Prompt, &deps, &result.Status, &result.Attempts, &result.Output, &result.Error); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(deps, &step.DependsOn); err != nil {
			return nil, err
		}
		result.ID = step.ID
		job.Steps = append(job.Steps, step)
		job.Results = append(job.Results, result)
	}
	return job, rows.Err()
}

// ClaimJob takes ownership of a job for the given lease window and reports the
// attempt number this claim represents. It refuses jobs that already reached a
// terminal state and jobs whose lease is still held by a live worker, so a
// duplicate queue delivery is a no-op rather than a second execution.
func (s *Store) ClaimJob(ctx context.Context, id, owner string, lease time.Duration) (attempts int, claimed bool, err error) {
	err = s.pool.QueryRow(ctx, `
UPDATE workflow_jobs
   SET status='running',
       owner=$2,
       lease_expires_at=now() + make_interval(secs => ($3::int8)::float8 / 1000),
       started_at=COALESCE(started_at, now()),
       attempts=attempts+1,
       updated_at=now()
 WHERE id=$1
   AND status NOT IN ('succeeded','failed')
   AND (owner IS NULL OR owner=$2 OR lease_expires_at IS NULL OR lease_expires_at <= now())
RETURNING attempts`, id, owner, lease.Milliseconds()).Scan(&attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return attempts, true, nil
}

// ExtendLease renews owner's claim. A false result means the lease was
// reclaimed and the worker must abandon the job immediately.
func (s *Store) ExtendLease(ctx context.Context, id, owner string, lease time.Duration) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_jobs
   SET lease_expires_at=now() + make_interval(secs => ($3::int8)::float8 / 1000), updated_at=now()
 WHERE id=$1 AND owner=$2 AND status='running'`, id, owner, lease.Milliseconds())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// FinishJob records a terminal state, but only if owner still holds the lease.
// An empty owner matches an unclaimed job, which is how a job that failed to
// dispatch is failed by the submitting request rather than by a worker.
func (s *Store) FinishJob(ctx context.Context, id, owner string, status workflow.Status, jobErr string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_jobs
   SET status=$3, error=$4, finished_at=now(), owner=NULL, lease_expires_at=NULL, updated_at=now()
 WHERE id=$1 AND (owner=$2 OR ($2='' AND owner IS NULL))`, id, owner, status, jobErr)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseJob hands a job back voluntarily so another worker can pick it up.
func (s *Store) ReleaseJob(ctx context.Context, id, owner string) error {
	_, err := s.pool.Exec(ctx, `
UPDATE workflow_jobs
   SET status='pending', owner=NULL, lease_expires_at=NULL, updated_at=now()
 WHERE id=$1 AND owner=$2 AND status='running'`, id, owner)
	return err
}

// ReclaimExpiredJobs returns jobs abandoned by dead workers to the pending
// state and reports their IDs so the caller can re-enqueue them. It exists in
// addition to the queue's own reaper: it recovers jobs whose queue entry was
// lost entirely, for example after a Redis failover with no persistence.
//
// grace holds it back until a lease has been expired for that long, which lets
// the cheaper queue reaper handle the ordinary case first and keeps the two
// reapers from re-enqueuing the same job at the same moment.
func (s *Store) ReclaimExpiredJobs(ctx context.Context, grace time.Duration, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultReclaimBatch
	}
	rows, err := s.pool.Query(ctx, `
UPDATE workflow_jobs
   SET status='pending', owner=NULL, lease_expires_at=NULL, updated_at=now()
 WHERE id IN (
   SELECT id FROM workflow_jobs
    WHERE status='running'
      AND lease_expires_at IS NOT NULL
      AND lease_expires_at <= now() - make_interval(secs => ($1::int8)::float8 / 1000)
    ORDER BY lease_expires_at
    LIMIT $2
    FOR UPDATE SKIP LOCKED
 )
RETURNING id`, grace.Milliseconds(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ClaimStep marks a single step as running, fenced on the job lease. It returns
// false when the step already succeeded (a resumed job) or when this worker no
// longer owns the job.
func (s *Store) ClaimStep(ctx context.Context, jobID, stepID, owner string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
UPDATE workflow_steps s
   SET status='running', owner=$3, started_at=now()
  FROM workflow_jobs j
 WHERE s.job_id=$1 AND s.id=$2 AND j.id=s.job_id AND j.owner=$3 AND s.status <> 'succeeded'`, jobID, stepID, owner)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// UpdateStep persists a step result, fenced on the job lease so a worker that
// lost its claim cannot overwrite the new owner's work.
func (s *Store) UpdateStep(ctx context.Context, jobID, owner string, result workflow.StepResult) error {
	_, err := s.pool.Exec(ctx, `
UPDATE workflow_steps s
   SET status=$4, attempts=$5, output=$6, error=$7,
       finished_at=CASE WHEN $4 IN ('succeeded','failed','skipped') THEN now() ELSE s.finished_at END
  FROM workflow_jobs j
 WHERE s.job_id=$1 AND s.id=$2 AND j.id=s.job_id AND ($3='' OR j.owner=$3)`,
		jobID, result.ID, owner, result.Status, result.Attempts, result.Output, result.Error)
	return err
}

// AppendEvent adds one entry to a job's event log. Events are advisory: a
// failure to record one never fails the job it describes.
func (s *Store) AppendEvent(ctx context.Context, jobID, typ string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO workflow_events (job_id,type,data) VALUES ($1,$2,$3)`, jobID, typ, encoded)
	return err
}

// Events returns a job's events with an ID greater than after, in order, which
// is how a dropped SSE stream resumes without replaying what it already sent.
func (s *Store) Events(ctx context.Context, jobID string, after int64) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,type,data,created_at FROM workflow_events WHERE job_id=$1 AND id>$2 ORDER BY id`, jobID, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []Event
	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.Type, &e.Data, &e.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

// RegisterWorker records a worker in the registry, resetting its heartbeat.
func (s *Store) RegisterWorker(ctx context.Context, w WorkerInfo) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO workflow_workers (id,hostname,pid,queue,started_at,last_heartbeat_at)
VALUES ($1,$2,$3,$4,now(),now())
ON CONFLICT (id) DO UPDATE SET hostname=EXCLUDED.hostname, pid=EXCLUDED.pid, queue=EXCLUDED.queue, started_at=now(), last_heartbeat_at=now()`,
		w.ID, w.Hostname, w.PID, w.Queue)
	return err
}

// HeartbeatWorker keeps a worker visible to the rest of the cluster. claimed is
// added to the worker's lifetime job counter.
func (s *Store) HeartbeatWorker(ctx context.Context, id string, claimed int64) error {
	_, err := s.pool.Exec(ctx, `UPDATE workflow_workers SET last_heartbeat_at=now(), jobs_claimed=jobs_claimed+$2 WHERE id=$1`, id, claimed)
	return err
}

// UnregisterWorker removes a worker that shut down cleanly.
func (s *Store) UnregisterWorker(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM workflow_workers WHERE id=$1`, id)
	return err
}

// PruneWorkers drops workers that have not sent a heartbeat within ttl.
func (s *Store) PruneWorkers(ctx context.Context, ttl time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM workflow_workers WHERE last_heartbeat_at < now() - make_interval(secs => ($1::int8)::float8 / 1000)`, ttl.Milliseconds())
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Workers lists workers that have sent a heartbeat within ttl.
func (s *Store) Workers(ctx context.Context, ttl time.Duration) ([]WorkerInfo, error) {
	rows, err := s.pool.Query(ctx, `
SELECT id,hostname,pid,queue,started_at,last_heartbeat_at,jobs_claimed
  FROM workflow_workers
 WHERE last_heartbeat_at >= now() - make_interval(secs => ($1::int8)::float8 / 1000)
 ORDER BY started_at`, ttl.Milliseconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var workers []WorkerInfo
	for rows.Next() {
		var w WorkerInfo
		if err := rows.Scan(&w.ID, &w.Hostname, &w.PID, &w.Queue, &w.StartedAt, &w.LastBeatAt, &w.JobsClaimed); err != nil {
			return nil, err
		}
		workers = append(workers, w)
	}
	return workers, rows.Err()
}
