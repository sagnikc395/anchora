package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sagnikc395/anchora"
	"time"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	store := &Store{pool: pool}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}
func (s *Store) Close() { s.pool.Close() }
func (s *Store) migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS workflow_jobs (id TEXT PRIMARY KEY, status TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL, started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ); CREATE TABLE IF NOT EXISTS workflow_steps (job_id TEXT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE, id TEXT NOT NULL, ordinal INTEGER NOT NULL, agent TEXT NOT NULL, prompt TEXT NOT NULL, depends_on JSONB NOT NULL DEFAULT '[]', status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, output TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', PRIMARY KEY(job_id,id)); CREATE TABLE IF NOT EXISTS workflow_events (id BIGSERIAL PRIMARY KEY, job_id TEXT NOT NULL REFERENCES workflow_jobs(id) ON DELETE CASCADE, type TEXT NOT NULL, data JSONB NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now()); CREATE INDEX IF NOT EXISTS workflow_events_job_id_id ON workflow_events(job_id, id);`)
	return err
}
func (s *Store) Create(ctx context.Context, job *Job) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `INSERT INTO workflow_jobs (id,status,created_at) VALUES ($1,$2,$3)`, job.ID, job.Status, job.CreatedAt); err != nil {
		return err
	}
	for i, step := range job.Steps {
		deps, _ := json.Marshal(step.DependsOn)
		result := job.Results[i]
		if _, err = tx.Exec(ctx, `INSERT INTO workflow_steps (job_id,id,ordinal,agent,prompt,depends_on,status,attempts,output,error) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, job.ID, step.ID, i, step.Agent, step.Prompt, deps, result.Status, result.Attempts, result.Output, result.Error); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) Get(ctx context.Context, id string) (*Job, error) {
	job := &Job{ID: id}
	var started, finished *time.Time
	err := s.pool.QueryRow(ctx, `SELECT status,created_at,started_at,finished_at FROM workflow_jobs WHERE id=$1`, id).Scan(&job.Status, &job.CreatedAt, &started, &finished)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	job.StartedAt, job.FinishedAt = started, finished
	rows, err := s.pool.Query(ctx, `SELECT id,agent,prompt,depends_on,status,attempts,output,error FROM workflow_steps WHERE job_id=$1 ORDER BY ordinal`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var step Step
		var deps []byte
		var result anchora.StepResult
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
func (s *Store) SetStatus(ctx context.Context, id string, status anchora.Status) error {
	if status == anchora.Running {
		_, err := s.pool.Exec(ctx, `UPDATE workflow_jobs SET status=$2,started_at=now() WHERE id=$1`, id, status)
		return err
	}
	_, err := s.pool.Exec(ctx, `UPDATE workflow_jobs SET status=$2,finished_at=now() WHERE id=$1`, id, status)
	return err
}
func (s *Store) UpdateStep(ctx context.Context, jobID string, result anchora.StepResult) error {
	_, err := s.pool.Exec(ctx, `UPDATE workflow_steps SET status=$3,attempts=$4,output=$5,error=$6 WHERE job_id=$1 AND id=$2`, jobID, result.ID, result.Status, result.Attempts, result.Output, result.Error)
	return err
}
func (s *Store) AppendEvent(ctx context.Context, jobID, typ string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `INSERT INTO workflow_events (job_id,type,data) VALUES ($1,$2,$3)`, jobID, typ, encoded)
	return err
}
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
