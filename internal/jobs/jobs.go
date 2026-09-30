// Package jobs provides durable, queued workflow execution across many nodes.
//
// Delivery is at-least-once and execution is effectively at-most-once per
// step. Three mechanisms combine to make that true:
//
//   - A claimed job holds a lease in both Redis and PostgreSQL. The owning
//     worker renews it on a heartbeat; a worker that dies stops renewing, and a
//     reaper hands the job to somebody else.
//   - Every write a worker makes while running a job is fenced on the
//     PostgreSQL lease, so a partitioned worker that has already been replaced
//     cannot overwrite the new owner's progress.
//   - A redelivered job resumes. Steps that already succeeded are replayed from
//     the store instead of being sent to an agent again, so recovering from a
//     crash costs only the work that was actually lost.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/sagnikc395/anchora/internal/workflow"
)

// Defaults applied to any Config field left at its zero value.
const (
	defaultLease          = 30 * time.Second
	defaultReaperInterval = 5 * time.Second
	defaultReclaimBatch   = 100
)

// Timeouts for the bookkeeping that must outlive a cancelled run: recording a
// job's outcome, persisting a step, and unregistering a worker.
const (
	finishTimeout     = 10 * time.Second
	stepWriteTimeout  = 5 * time.Second
	unregisterTimeout = 5 * time.Second
)

// ErrLeaseLost reports that a worker's claim on a job was reclaimed while it
// was still running. The worker abandons the job; it does not fail it.
var ErrLeaseLost = errors.New("job lease lost")

// The event types appended to a job's event log, in the order a successful job
// normally produces them.
const (
	EventQueued        = "job.queued"
	EventRunning       = "job.running"
	EventResumed       = "job.resumed"
	EventStepCompleted = "step.completed"
	EventReclaimed     = "job.reclaimed"
	EventAbandoned     = "job.abandoned"
	EventDeadLettered  = "job.dead_lettered"
	EventCompleted     = "job.completed"
)

// IsTerminal reports whether an event type ends a job's event stream, so a
// reader knows when to stop without polling the job itself.
func IsTerminal(eventType string) bool {
	return eventType == EventCompleted || eventType == EventDeadLettered
}

// Step is one agent call in a submitted workflow. Unlike workflow.Step it names
// its agent, because a job outlives the process that submitted it.
type Step struct {
	ID        string   `json:"id"`
	Agent     string   `json:"agent"`
	Prompt    string   `json:"prompt"`
	DependsOn []string `json:"depends_on,omitempty"`
}

// Job is a submitted workflow and everything known about its execution: the
// delivery count, the worker that currently owns it, and a result per step.
type Job struct {
	ID             string                `json:"id"`
	Status         workflow.Status       `json:"status"`
	Attempts       int                   `json:"attempts"`
	Owner          string                `json:"owner,omitempty"`
	Error          string                `json:"error,omitempty"`
	CreatedAt      time.Time             `json:"created_at"`
	StartedAt      *time.Time            `json:"started_at,omitempty"`
	FinishedAt     *time.Time            `json:"finished_at,omitempty"`
	LeaseExpiresAt *time.Time            `json:"lease_expires_at,omitempty"`
	Steps          []Step                `json:"steps"`
	Results        []workflow.StepResult `json:"results"`
}

// Event is one entry in a job's append-only event log. ID is monotonic per
// store, which is what lets an SSE reader resume from where it left off.
type Event struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
}

// Stats summarises cluster health for an operator endpoint.
type Stats struct {
	QueueReady    int64        `json:"queue_ready"`
	QueueInFlight int64        `json:"queue_in_flight"`
	Workers       []WorkerInfo `json:"workers"`
}

// AgentResolver maps a step's agent name to a runnable agent.
type AgentResolver interface {
	Resolve(string) (workflow.Agent, bool)
}

// JobStore is the durable half of the backend. *Store implements it.
type JobStore interface {
	Create(ctx context.Context, job *Job) error
	Get(ctx context.Context, id string) (*Job, error)
	ClaimJob(ctx context.Context, id, owner string, lease time.Duration) (int, bool, error)
	ExtendLease(ctx context.Context, id, owner string, lease time.Duration) (bool, error)
	FinishJob(ctx context.Context, id, owner string, status workflow.Status, jobErr string) (bool, error)
	ReleaseJob(ctx context.Context, id, owner string) error
	ReclaimExpiredJobs(ctx context.Context, grace time.Duration, limit int) ([]string, error)
	ClaimStep(ctx context.Context, jobID, stepID, owner string) (bool, error)
	UpdateStep(ctx context.Context, jobID, owner string, result workflow.StepResult) error
	AppendEvent(ctx context.Context, jobID, typ string, data any) error
	Events(ctx context.Context, jobID string, after int64) ([]Event, error)
	RegisterWorker(ctx context.Context, w WorkerInfo) error
	HeartbeatWorker(ctx context.Context, id string, claimed int64) error
	UnregisterWorker(ctx context.Context, id string) error
	PruneWorkers(ctx context.Context, ttl time.Duration) (int64, error)
	Workers(ctx context.Context, ttl time.Duration) ([]WorkerInfo, error)
}

// JobQueue is the dispatch half of the backend. *Queue implements it.
type JobQueue interface {
	Push(ctx context.Context, id string) error
	Claim(ctx context.Context, owner string, lease time.Duration) (string, error)
	Extend(ctx context.Context, id, owner string, lease time.Duration) (bool, error)
	Ack(ctx context.Context, id string) error
	Requeue(ctx context.Context, id string) error
	Reclaim(ctx context.Context, limit int) ([]string, error)
	Depth(ctx context.Context) (int64, int64, error)
}

// Config tunes the distributed behaviour of the backend.
type Config struct {
	// Lease is the visibility timeout: how long a claim survives without a
	// heartbeat before another worker may take the job.
	Lease time.Duration
	// Heartbeat is the lease renewal interval. It must be comfortably shorter
	// than Lease; it defaults to a third of it.
	Heartbeat time.Duration
	// MaxAttempts caps how many times a job may be delivered before it is
	// dead-lettered. Zero means unlimited.
	MaxAttempts int
	// ReaperInterval is how often expired leases are swept.
	ReaperInterval time.Duration
	// WorkerTTL is how long a worker may go without a heartbeat before it is
	// dropped from the registry.
	WorkerTTL time.Duration
	// ReclaimBatch caps how many jobs one sweep recovers.
	ReclaimBatch int
	// QueueName is recorded in the worker registry for observability.
	QueueName string
}

// Service is the job backend: it submits workflows, runs workers, and sweeps up
// after workers that died. One Service is shared by the HTTP handlers and every
// worker goroutine in a process.
type Service struct {
	Store   JobStore
	Queue   JobQueue
	Agents  AgentResolver
	Options workflow.Options
	Config  Config
	// Logf receives operational messages. It defaults to discarding them.
	Logf func(format string, args ...any)
}

func (c Config) lease() time.Duration {
	if c.Lease <= 0 {
		return defaultLease
	}
	return c.Lease
}

func (c Config) heartbeat() time.Duration {
	if c.Heartbeat > 0 && c.Heartbeat < c.lease() {
		return c.Heartbeat
	}
	return c.lease() / 3
}

func (c Config) reaperInterval() time.Duration {
	if c.ReaperInterval <= 0 {
		return defaultReaperInterval
	}
	return c.ReaperInterval
}

func (c Config) workerTTL() time.Duration {
	if c.WorkerTTL <= 0 {
		return 4 * c.lease()
	}
	return c.WorkerTTL
}

func (c Config) reclaimBatch() int {
	if c.ReclaimBatch <= 0 {
		return defaultReclaimBatch
	}
	return c.ReclaimBatch
}

func (s *Service) logf(format string, args ...any) {
	if s.Logf != nil {
		s.Logf(format, args...)
	}
}

// Submit validates a workflow, records it, and enqueues it for execution.
func (s *Service) Submit(ctx context.Context, steps []Step) (*Job, error) {
	workflowSteps, err := s.resolveSteps(steps)
	if err != nil {
		return nil, err
	}
	if _, err := workflow.NewWorkflow(workflowSteps, s.Options); err != nil {
		return nil, err
	}
	id, err := NewID()
	if err != nil {
		return nil, err
	}
	job := &Job{ID: id, Status: workflow.Pending, CreatedAt: time.Now().UTC(), Steps: steps, Results: make([]workflow.StepResult, len(steps))}
	for i, step := range steps {
		job.Results[i] = workflow.StepResult{ID: step.ID, Status: workflow.Pending}
	}
	if err := s.Store.Create(ctx, job); err != nil {
		return nil, err
	}
	if err := s.Store.AppendEvent(ctx, id, EventQueued, job); err != nil {
		return nil, err
	}
	if err := s.Queue.Push(ctx, id); err != nil {
		// The job is durable but undispatched; the store reaper will not see
		// it because it never ran. Surface the failure to the caller instead.
		_, _ = s.Store.FinishJob(ctx, id, "", workflow.Failed, "enqueue failed: "+err.Error())
		return nil, fmt.Errorf("enqueue job: %w", err)
	}
	return job, nil
}

// Get loads a job. A job that does not exist is a nil job and a nil error.
func (s *Service) Get(ctx context.Context, id string) (*Job, error) { return s.Store.Get(ctx, id) }

// Events returns a job's events recorded after the given event ID.
func (s *Service) Events(ctx context.Context, id string, after int64) ([]Event, error) {
	return s.Store.Events(ctx, id, after)
}

// Stats reports queue depth and the live worker roster.
func (s *Service) Stats(ctx context.Context) (Stats, error) {
	ready, inFlight, err := s.Queue.Depth(ctx)
	if err != nil {
		return Stats{}, err
	}
	workers, err := s.Store.Workers(ctx, s.Config.workerTTL())
	if err != nil {
		return Stats{}, err
	}
	return Stats{QueueReady: ready, QueueInFlight: inFlight, Workers: workers}, nil
}

// RunWorker claims and executes jobs until ctx is cancelled. workerID must be
// unique across the cluster; use NewWorkerID to generate one.
func (s *Service) RunWorker(ctx context.Context, workerID string) error {
	info := WorkerInfo{ID: workerID, PID: os.Getpid(), Queue: s.Config.QueueName}
	info.Hostname, _ = os.Hostname()
	if err := s.Store.RegisterWorker(ctx, info); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), unregisterTimeout)
		defer cancel()
		if err := s.Store.UnregisterWorker(shutdown, workerID); err != nil {
			s.logf("worker %s: unregister: %v", workerID, err)
		}
	}()

	beat := time.NewTicker(s.Config.heartbeat())
	defer beat.Stop()
	var claimed atomic.Int64
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-beat.C:
				if err := s.Store.HeartbeatWorker(ctx, workerID, claimed.Swap(0)); err != nil && ctx.Err() == nil {
					s.logf("worker %s: heartbeat: %v", workerID, err)
				}
			}
		}
	}()

	backoff := time.Duration(0)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		id, err := s.Queue.Claim(ctx, workerID, s.Config.lease())
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// A broken queue must not spin: back off up to five seconds so a
			// Redis outage costs a slow poll rather than a hot loop.
			backoff = nextBackoff(backoff)
			s.logf("worker %s: claim: %v (retrying in %s)", workerID, err, backoff)
			if !sleepCtx(ctx, backoff) {
				return ctx.Err()
			}
			continue
		}
		backoff = 0
		if id == "" {
			continue
		}
		claimed.Add(1)
		s.process(ctx, workerID, id)
	}
}

// RunReaper recovers work abandoned by dead workers and prunes the registry.
// Running it on every node is safe and expected; the sweeps are atomic.
func (s *Service) RunReaper(ctx context.Context) error {
	ticker := time.NewTicker(s.Config.reaperInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.reap(ctx)
		}
	}
}

func (s *Service) reap(ctx context.Context) {
	batch := s.Config.reclaimBatch()
	if recovered, err := s.Queue.Reclaim(ctx, batch); err != nil {
		if ctx.Err() == nil {
			s.logf("reaper: queue reclaim: %v", err)
		}
	} else {
		for _, id := range recovered {
			s.logf("reaper: requeued job %s after lease expiry", id)
			_ = s.Store.AppendEvent(ctx, id, EventReclaimed, map[string]string{"reason": "queue lease expired"})
		}
	}
	// Jobs whose queue entry vanished entirely — a Redis flush or failover —
	// are still recoverable because the store holds the authoritative lease.
	orphans, err := s.Store.ReclaimExpiredJobs(ctx, s.Config.lease(), batch)
	if err != nil {
		if ctx.Err() == nil {
			s.logf("reaper: store reclaim: %v", err)
		}
		return
	}
	for _, id := range orphans {
		if err := s.Queue.Push(ctx, id); err != nil {
			s.logf("reaper: re-enqueue orphaned job %s: %v", id, err)
			continue
		}
		s.logf("reaper: re-enqueued orphaned job %s", id)
		_ = s.Store.AppendEvent(ctx, id, EventReclaimed, map[string]string{"reason": "store lease expired"})
	}
	if pruned, err := s.Store.PruneWorkers(ctx, s.Config.workerTTL()); err != nil {
		if ctx.Err() == nil {
			s.logf("reaper: prune workers: %v", err)
		}
	} else if pruned > 0 {
		s.logf("reaper: pruned %d dead worker(s)", pruned)
	}
}

// process takes ownership of one delivery and drives it to a conclusion.
func (s *Service) process(ctx context.Context, workerID, id string) {
	lease := s.Config.lease()
	attempts, claimed, err := s.Store.ClaimJob(ctx, id, workerID, lease)
	if err != nil {
		// Leave the queue lease alone; it expires and someone retries.
		s.logf("worker %s: claim job %s: %v", workerID, id, err)
		return
	}
	if !claimed {
		// Already finished, or still leased by a live worker. This delivery is
		// a duplicate, which at-least-once dispatch makes normal.
		s.logf("worker %s: job %s not claimable, dropping duplicate delivery", workerID, id)
		s.ack(ctx, id)
		return
	}
	if limit := s.Config.MaxAttempts; limit > 0 && attempts > limit {
		s.deadLetter(ctx, workerID, id, attempts, limit)
		s.ack(ctx, id)
		return
	}
	_ = s.Store.AppendEvent(ctx, id, EventRunning, map[string]any{"status": workflow.Running, "worker": workerID, "attempt": attempts})

	runCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		s.holdLease(runCtx, cancel, id, workerID)
	}()

	results, jobErr, infraErr := s.runJob(runCtx, workerID, id)
	cancel(nil)
	<-heartbeatDone

	// Finishing uses a context detached from the run so a shutdown mid-job
	// still records the outcome rather than leaving the job leased.
	final, finalCancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	defer finalCancel()

	if infraErr != nil {
		s.abandon(final, workerID, id, infraErr)
		return
	}
	status := workflow.Succeeded
	message := ""
	if jobErr != nil {
		status, message = workflow.Failed, jobErr.Error()
	}
	for _, result := range results {
		if err := s.Store.UpdateStep(final, id, workerID, result); err != nil {
			s.logf("worker %s: persist step %s of job %s: %v", workerID, result.ID, id, err)
		}
	}
	held, err := s.Store.FinishJob(final, id, workerID, status, message)
	if err != nil {
		s.logf("worker %s: finish job %s: %v", workerID, id, err)
		return
	}
	if !held {
		// Our lease was reclaimed between the last heartbeat and now. The new
		// owner is authoritative; drop the result rather than fight it.
		s.logf("worker %s: job %s finished without a lease, discarding outcome", workerID, id)
		return
	}
	_ = s.Store.AppendEvent(final, id, EventCompleted, map[string]any{"status": status, "results": results, "worker": workerID, "attempt": attempts})
	s.ack(final, id)
}

// holdLease renews the job lease on a heartbeat and cancels the run the moment
// the claim is lost, so a superseded worker stops burning agent calls.
func (s *Service) holdLease(ctx context.Context, cancel context.CancelCauseFunc, id, workerID string) {
	ticker := time.NewTicker(s.Config.heartbeat())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lease := s.Config.lease()
			heldStore, err := s.Store.ExtendLease(ctx, id, workerID, lease)
			if err != nil {
				// Transient. The lease still has most of its window left, so
				// try again on the next beat rather than dropping the job.
				s.logf("worker %s: extend store lease on %s: %v", workerID, id, err)
				continue
			}
			heldQueue, err := s.Queue.Extend(ctx, id, workerID, lease)
			if err != nil {
				s.logf("worker %s: extend queue lease on %s: %v", workerID, id, err)
				continue
			}
			if !heldStore || !heldQueue {
				s.logf("worker %s: lost lease on job %s", workerID, id)
				cancel(ErrLeaseLost)
				return
			}
		}
	}
}

// runJob executes a job, resuming any steps that already succeeded. It
// separates a workflow failure (terminal: the agents ran and something failed)
// from an infrastructure failure (retryable: we lost the lease or shut down).
func (s *Service) runJob(ctx context.Context, workerID, id string) (results []workflow.StepResult, jobErr, infraErr error) {
	job, err := s.Store.Get(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("load job %s: %w", id, err)
	}
	if job == nil {
		return nil, fmt.Errorf("job %s disappeared from the store", id), nil
	}
	steps, err := s.resolveSteps(job.Steps)
	if err != nil {
		// A missing agent is a configuration error. Retrying cannot fix it.
		return nil, err, nil
	}
	// A step claim can fail before the heartbeat notices the lease is gone.
	// The fence records that directly so the outcome is classified as a lease
	// loss (retryable elsewhere) rather than as a workflow failure (terminal).
	fence := &leaseFence{}
	for i := range steps {
		steps[i].Agent = fencedAgent{fence: fence, store: s.Store, inner: steps[i].Agent, jobID: id, stepID: steps[i].ID, owner: workerID}
	}

	options := s.Options
	options.Resume = resumable(job)
	options.OnStepState = func(result workflow.StepResult) {
		// Detached: a step that lands as the run is being cancelled should
		// still be recorded, and it is fenced on the lease regardless.
		write, cancel := context.WithTimeout(context.WithoutCancel(ctx), stepWriteTimeout)
		defer cancel()
		if err := s.Store.UpdateStep(write, id, workerID, result); err != nil {
			s.logf("worker %s: persist step %s of job %s: %v", workerID, result.ID, id, err)
		}
		_ = s.Store.AppendEvent(write, id, EventStepCompleted, result)
	}
	if len(options.Resume) > 0 {
		s.logf("worker %s: resuming job %s with %d completed step(s)", workerID, id, len(options.Resume))
		_ = s.Store.AppendEvent(ctx, id, EventResumed, map[string]any{"completed": len(options.Resume)})
	}

	wf, err := workflow.NewWorkflow(steps, options)
	if err != nil {
		return nil, err, nil
	}
	results, runErr := wf.Run(ctx)
	if cause := context.Cause(ctx); cause != nil && !errors.Is(cause, context.Canceled) {
		return results, nil, cause
	}
	if ctx.Err() != nil {
		return results, nil, ctx.Err()
	}
	if fence.lost.Load() || errors.Is(runErr, ErrLeaseLost) {
		return results, nil, ErrLeaseLost
	}
	return results, runErr, nil
}

// abandon releases a job that could not be completed for infrastructure
// reasons so another worker retries it promptly.
func (s *Service) abandon(ctx context.Context, workerID, id string, cause error) {
	s.logf("worker %s: abandoning job %s: %v", workerID, id, cause)
	_ = s.Store.AppendEvent(ctx, id, EventAbandoned, map[string]string{"worker": workerID, "reason": cause.Error()})
	if errors.Is(cause, ErrLeaseLost) {
		// Someone else already owns it; touching the queue would duplicate it.
		return
	}
	if err := s.Store.ReleaseJob(ctx, id, workerID); err != nil {
		s.logf("worker %s: release job %s: %v", workerID, id, err)
		return
	}
	if err := s.Queue.Requeue(ctx, id); err != nil {
		s.logf("worker %s: requeue job %s: %v", workerID, id, err)
	}
}

// deadLetter permanently fails a job that exhausted its delivery budget.
func (s *Service) deadLetter(ctx context.Context, workerID, id string, attempts, limit int) {
	reason := fmt.Sprintf("exceeded %d delivery attempt(s)", limit)
	s.logf("worker %s: dead-lettering job %s after %d attempt(s)", workerID, id, attempts)
	if _, err := s.Store.FinishJob(ctx, id, workerID, workflow.Failed, reason); err != nil {
		s.logf("worker %s: dead-letter job %s: %v", workerID, id, err)
	}
	_ = s.Store.AppendEvent(ctx, id, EventDeadLettered, map[string]any{"attempts": attempts, "max_attempts": limit, "worker": workerID})
}

func (s *Service) ack(ctx context.Context, id string) {
	if err := s.Queue.Ack(ctx, id); err != nil {
		s.logf("ack job %s: %v", id, err)
	}
}

func (s *Service) resolveSteps(inputs []Step) ([]workflow.Step, error) {
	steps := make([]workflow.Step, 0, len(inputs))
	for _, input := range inputs {
		agent, ok := s.Agents.Resolve(input.Agent)
		if !ok {
			return nil, fmt.Errorf("unknown agent: %s", input.Agent)
		}
		steps = append(steps, workflow.Step{ID: input.ID, Agent: agent, Prompt: input.Prompt, DependsOn: input.DependsOn})
	}
	return steps, nil
}

// leaseFence records that a step claim was refused during a run.
type leaseFence struct{ lost atomic.Bool }

// fencedAgent claims a step against the job lease immediately before the agent
// call. The claim is what keeps a superseded worker from spending a second
// inference on work the new owner is already doing.
type fencedAgent struct {
	fence                *leaseFence
	store                JobStore
	inner                workflow.Agent
	jobID, stepID, owner string
}

func (a fencedAgent) Run(ctx context.Context, prompt string) (string, error) {
	claimed, err := a.store.ClaimStep(ctx, a.jobID, a.stepID, a.owner)
	if err != nil {
		return "", fmt.Errorf("claim step %q: %w", a.stepID, err)
	}
	if !claimed {
		a.fence.lost.Store(true)
		return "", fmt.Errorf("%w: step %q is no longer ours", ErrLeaseLost, a.stepID)
	}
	return a.inner.Run(ctx, prompt)
}

// resumable returns the results of steps that already succeeded on a previous
// delivery, so they are replayed rather than re-executed.
func resumable(job *Job) []workflow.StepResult {
	var done []workflow.StepResult
	for _, result := range job.Results {
		if result.Status == workflow.Succeeded {
			done = append(done, result)
		}
	}
	return done
}

func nextBackoff(current time.Duration) time.Duration {
	const max = 5 * time.Second
	if current <= 0 {
		return 100 * time.Millisecond
	}
	if next := current * 2; next < max {
		return next
	}
	return max
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// NewID returns a random 128-bit identifier.
func NewID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// NewWorkerID returns an identifier that is unique across the cluster and
// readable in the worker registry.
func NewWorkerID() (string, error) {
	suffix, err := NewID()
	if err != nil {
		return "", err
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), suffix[:8]), nil
}
