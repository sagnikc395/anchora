package pgflow

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Status is the lifecycle state of a workflow execution.
type Status string

const (
	// StatusEnqueued is reserved for queue-backed starts. The core engine
	// starts work immediately, so it writes StatusPending directly.
	StatusEnqueued Status = "ENQUEUED"
	// StatusPending means an executor owns the workflow and it may still run.
	StatusPending Status = "PENDING"
	// StatusSuccess means the workflow function returned without an error.
	StatusSuccess Status = "SUCCESS"
	// StatusError means the workflow function or one of its steps failed.
	StatusError Status = "ERROR"
	// StatusCancelled means the workflow was cancelled through Engine.Cancel.
	StatusCancelled Status = "CANCELLED"
	// StatusRetriesExceeded means recovery gave up on the workflow.
	StatusRetriesExceeded Status = "RETRIES_EXCEEDED"
)

// Terminal reports whether a workflow in this state will never run again.
func (s Status) Terminal() bool {
	switch s {
	case StatusSuccess, StatusError, StatusCancelled, StatusRetriesExceeded:
		return true
	}
	return false
}

// Kind identifies the engine operation a checkpoint records. Every kind is
// stored in the same (workflow_id, function_id) slot, so replay compares both
// the kind and the name to detect a workflow whose code changed.
type Kind string

const (
	KindStep   Kind = "STEP"
	KindTx     Kind = "TX"
	KindSleep  Kind = "SLEEP"
	KindRecv   Kind = "RECV"
	KindSend   Kind = "SEND"
	KindChild  Kind = "CHILD"
	KindNow    Kind = "NOW"
	KindRandom Kind = "RAND"
	KindUUID   Kind = "UUID"
	// KindPatch records a Patched marker so replays pick the same branch.
	KindPatch Kind = "PATCH"
	// KindEnqueue records a workflow started from inside another workflow, so
	// an enqueue happens exactly once per function slot.
	KindEnqueue Kind = "ENQUEUE"
	// KindSetEvent records a published event.
	KindSetEvent Kind = "SETEVENT"
	// KindGetEvent records a read of a published event.
	KindGetEvent Kind = "GETEVENT"
)

// WorkflowRecord is one row of pgflow.workflow_status joined with its
// pgflow.workflow_io payloads.
type WorkflowRecord struct {
	ID               string
	Name             string
	Status           Status
	AppVersion       string
	ExecutorID       string
	Epoch            int64
	RecoveryAttempts int
	ParentWorkflowID string
	ParentFunctionID *int
	ForkedFrom       string
	ContinuedTo      string
	QueueName        string
	Priority         int
	PartitionKey     string
	LeaseExpiresAt   *time.Time
	Inputs           []byte
	Output           []byte
	Error            *ErrorInfo
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StartedAt        *time.Time
}

// Checkpoint is one completed engine operation: the write-ahead log entry for
// a step, a timer, a message, a child workflow, or a clock read.
type Checkpoint struct {
	FunctionID      int
	FunctionName    string
	Kind            Kind
	Output          []byte
	Err             *ErrorInfo
	Attempts        int
	ChildWorkflowID string
	StartedAt       time.Time
	CompletedAt     time.Time
}

// ErrorInfo is the persisted form of a terminal error: enough to rehydrate an
// equivalent error when the workflow is replayed.
type ErrorInfo struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// Message is a durable message delivered to a workflow by Recv.
type Message struct {
	ID            string
	DestinationID string
	Topic         string
	Payload       []byte
	CreatedAt     time.Time
}

// ClaimExpiredRequest describes a recovery sweep.
type ClaimExpiredRequest struct {
	// ExecutorID becomes the new owner of every claimed workflow.
	ExecutorID string
	// AppVersion restricts recovery to workflows runnable by this binary.
	AppVersion string
	// Lease is how long the new owner holds the workflows before another
	// executor may claim them.
	Lease time.Duration
	// Limit caps how many workflows one sweep claims.
	Limit int
	// IncludeOwned also claims workflows this executor already owns, even if
	// their lease is still valid. Startup recovery uses it so a restarted
	// process picks its own previous work back up immediately; periodic sweeps
	// leave it false so they never claim a live execution.
	IncludeOwned bool
}

// Queue describes how an executor drains a named queue. Queues are
// pgflow.workflow_status rows with status ENQUEUED; an executor dequeues only
// queues it has registered, so one application never drains another's work.
type Queue struct {
	// Name is the queue identifier workflows are enqueued with.
	Name string
	// GlobalConcurrency caps PENDING workflows of this queue across the whole
	// fleet. Zero means unlimited.
	GlobalConcurrency int
	// PartitionConcurrency caps one partition key at a time, so a single
	// tenant's burst cannot starve the others. Zero means unlimited.
	PartitionConcurrency int
	// RateLimitCount allows at most this many workflow starts per
	// RateLimitPeriod, for provider RPM/TPM limits. Zero disables it.
	RateLimitCount int
	// RateLimitPeriod is the window RateLimitCount applies to.
	RateLimitPeriod time.Duration
	// LocalConcurrency is how many workflows of this queue this process runs
	// at once. Zero means unlimited.
	LocalConcurrency int
}

// DequeueRequest describes one dequeue sweep for one queue.
type DequeueRequest struct {
	// ExecutorID becomes the owner of the dequeued workflows.
	ExecutorID string
	// AppVersion restricts the sweep to workflows runnable by this binary.
	AppVersion string
	// Queue is the queue name to drain.
	Queue string
	// Lease is how long the new owner holds each workflow.
	Lease time.Duration
	// Limit caps how many workflows one sweep claims; the executor's local
	// concurrency is expected to be applied by the caller.
	Limit int
	// GlobalConcurrency caps PENDING workflows of the queue across the fleet.
	// Zero means unlimited.
	GlobalConcurrency int
	// PartitionConcurrency caps concurrently running workflows per partition.
	// Zero means unlimited.
	PartitionConcurrency int
	// RateLimitCount and RateLimitPeriod throttle starts per period. Zero
	// disables the throttle.
	RateLimitCount  int
	RateLimitPeriod time.Duration
}

// ListWorkflowsRequest selects workflows for operator tooling.
type ListWorkflowsRequest struct {
	// Status filters on the lifecycle state. Empty means any non-terminal
	// state plus terminal ones (that is, no filter).
	Status Status
	// Queue filters on the queue name.
	Queue string
	// Limit caps the number of rows; zero means 100.
	Limit int
}

// StreamChunk is one entry of an append-only workflow stream.
type StreamChunk struct {
	Seq       int64
	Chunk     []byte
	CreatedAt time.Time
}

// TxConn is the subset of pgx.Tx that a Tx step may use. A pgx.Tx satisfies
// it, so the transaction handed to a step is the real Postgres transaction
// that will also receive the checkpoint.
type TxConn interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Store is the persistence layer the engine runs on. The Postgres
// implementation is what makes a workflow durable; the in-memory
// implementation exists so replay logic can be tested without a database.
//
// Every method that writes workflow state takes the caller's fencing epoch and
// must reject the write if the workflow has since been claimed by another
// executor.
type Store interface {
	// CreateWorkflow inserts a new workflow and its input. If the id already
	// exists it returns false and the existing record, and must not modify it.
	CreateWorkflow(ctx context.Context, rec WorkflowRecord, lease time.Duration) (bool, *WorkflowRecord, error)
	// GetWorkflow loads one workflow with its input, output, and error.
	GetWorkflow(ctx context.Context, workflowID string) (*WorkflowRecord, error)
	// LoadCheckpoints returns every recorded operation for a workflow keyed by
	// function id, so a replay is one query instead of one per step.
	LoadCheckpoints(ctx context.Context, workflowID string) (map[int]*Checkpoint, error)
	// SaveCheckpoint writes cp if the workflow is still owned at epoch. First
	// writer wins: if the slot is already recorded, it returns false and the
	// stored checkpoint. It returns ErrFenced if the epoch is stale.
	SaveCheckpoint(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint) (bool, *Checkpoint, error)
	// CompleteWorkflow writes a terminal status, output, and error, fenced on
	// epoch. It returns false if the workflow already reached a terminal state
	// that it may not overwrite.
	CompleteWorkflow(ctx context.Context, workflowID string, epoch int64, status Status, output []byte, wfErr *ErrorInfo) (bool, error)
	// CancelWorkflow marks a pending workflow CANCELLED. It returns false if
	// the workflow is already terminal.
	CancelWorkflow(ctx context.Context, workflowID string) (bool, error)
	// ClaimExpired takes ownership of pending workflows whose lease expired or
	// that this executor already owned (recovery after a restart). Each claim
	// bumps the epoch and the recovery attempt counter.
	ClaimExpired(ctx context.Context, req ClaimExpiredRequest) ([]WorkflowRecord, error)
	// RenewLeases extends the lease on the given workflows and returns the
	// subset this executor still owns.
	RenewLeases(ctx context.Context, executorID string, lease time.Duration, workflowIDs []string) ([]string, error)
	// StartStepAttempt records a live step attempt for observability. Best
	// effort: the engine logs but does not fail a step if it errors.
	StartStepAttempt(ctx context.Context, workflowID string, functionID, attempt int, executorID string) error
	// HeartbeatStep updates the heartbeat of a live step attempt.
	HeartbeatStep(ctx context.Context, workflowID string, functionID, attempt int, details []byte) error
	// TxStep runs fn inside a transaction that also writes cp, so the step's
	// business writes and its checkpoint commit together. The checkpoint slot
	// is checked first: if it is already recorded, fn does not run.
	TxStep(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint, fn func(context.Context, TxConn) error) (created bool, existing *Checkpoint, err error)
	// SendMessage records the send checkpoint and inserts msg atomically, so a
	// message is enqueued exactly once per function slot.
	SendMessage(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint, msg Message) (bool, *Checkpoint, error)
	// ReceiveMessage consumes the oldest unconsumed message for the
	// destination and topic and records cp with the message payload as its
	// output, atomically. It returns a nil message if none is pending, in
	// which case nothing is written.
	ReceiveMessage(ctx context.Context, workflowID string, epoch int64, cp *Checkpoint, destinationID, topic string) (*Message, bool, *Checkpoint, error)
	// DequeueWorkflows claims ENQUEUED workflows of a queue, honoring the
	// queue's global and per-partition concurrency, its rate limit, and
	// priority order. Each claim starts the workflow and bumps the epoch.
	DequeueWorkflows(ctx context.Context, req DequeueRequest) ([]WorkflowRecord, error)
	// UpsertQueue stores a queue's configuration.
	UpsertQueue(ctx context.Context, q Queue) error
	// SetWorkflowEvent publishes a key/value event for a workflow.
	SetWorkflowEvent(ctx context.Context, workflowID, key string, value []byte) error
	// GetWorkflowEvent reads one event. It returns false when the key has not
	// been published.
	GetWorkflowEvent(ctx context.Context, workflowID, key string) ([]byte, bool, error)
	// AppendStream appends one chunk to a workflow's stream. The chunk sequence
	// is assigned within the call.
	AppendStream(ctx context.Context, workflowID, key string, chunk []byte) (int64, error)
	// ReadStreams returns the chunks of a stream after a sequence number, in
	// order.
	ReadStreams(ctx context.Context, workflowID, key string, afterSeq int64, limit int) ([]StreamChunk, error)
	// ForkWorkflow creates a new PENDING workflow that copies the input and
	// every checkpoint with function_id < uptoFunctionID from src, so a fork
	// re-runs only the tail of the workflow. The fork carries src's name and
	// app version so the executors that ran src can run it.
	ForkWorkflow(ctx context.Context, srcID, newID string, uptoFunctionID int, executorID string, lease time.Duration) (*WorkflowRecord, error)
	// SetContinuedTo records the follow-up workflow of one that ended with
	// ContinueAsNew, so handles and operators can follow the chain.
	SetContinuedTo(ctx context.Context, workflowID, continuedTo string) error
	// ListWorkflows returns workflow rows for operator tooling, newest first.
	ListWorkflows(ctx context.Context, req ListWorkflowsRequest) ([]WorkflowRecord, error)
	// DeleteTerminalWorkflowsBefore deletes terminal workflows older than
	// cutoff, at most limit of them, and returns how many were deleted.
	DeleteTerminalWorkflowsBefore(ctx context.Context, cutoff time.Time, limit int) (int, error)
}
