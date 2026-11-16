// Package pgflow is a Postgres-native durable execution engine.
//
// A workflow is a plain Go function. Every engine call it makes -- a step, a
// transactional step, a durable timer, a message, a clock read -- is recorded
// in Postgres before the result is handed back to the workflow. If the process
// dies, the workflow is re-executed from the top: recorded calls return their
// stored results without repeating side effects, and execution resumes at the
// first call with no recorded result.
//
// Postgres is the only infrastructure dependency. There is no coordinator
// process and no message broker.
package pgflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagnikc395/anchora/pgflow/serde"
)

// tracer emits one span per workflow execution and one per live step attempt.
// It is a no-op until the host application installs an SDK tracer provider.
var tracer = otel.Tracer("github.com/sagnikc395/anchora/pgflow")

// Config configures an Engine. Either DatabaseURL or Store must be set.
type Config struct {
	// DatabaseURL points at the Postgres database that holds engine state.
	DatabaseURL string
	// Store replaces the Postgres store, for tests. When set, DatabaseURL is
	// ignored and no migrations run.
	Store Store
	// ExecutorID identifies this process in workflow_status. Defaults to
	// hostname-pid-random.
	ExecutorID string
	// AppVersion is stamped on every workflow this engine starts and gates
	// recovery: an executor only recovers workflows it can run. Defaults to a
	// hash of the registered workflow names and the Go runtime version, so set
	// it explicitly if two builds register the same names with different code.
	AppVersion string
	// Lease is how long an executor owns a workflow without renewing. A
	// crashed executor's work becomes recoverable after it expires.
	Lease time.Duration
	// RecoveryInterval is how often an executor claims workflows with expired
	// leases.
	RecoveryInterval time.Duration
	// RecoveryBatch caps how many workflows one recovery sweep claims.
	RecoveryBatch int
	// MaxRecoveryAttempts is how many times a workflow may be recovered before
	// it is moved to RETRIES_EXCEEDED.
	MaxRecoveryAttempts int
	// QueuePollInterval is how often registered queues are swept for ENQUEUED
	// work.
	QueuePollInterval time.Duration
	// Codec serializes workflow inputs, outputs, and checkpoint payloads.
	// Defaults to serde.JSON.
	Codec serde.Codec
	// Logf receives background-loop diagnostics. Defaults to discarding them.
	Logf func(format string, args ...any)
}

const (
	defaultLease               = 30 * time.Second
	defaultRecoveryInterval    = 5 * time.Second
	defaultRecoveryBatch       = 100
	defaultMaxRecoveryAttempts = 5
	defaultQueuePollInterval   = 250 * time.Millisecond
)

// registeredWorkflow is a type-erased workflow function.
type registeredWorkflow func(*Context, []byte) ([]byte, error)

// runState is one in-process execution of a workflow.
type runState struct {
	epoch  int64
	queue  string
	cancel context.CancelFunc
}

// Engine owns the workflow registry, the store, and the background recovery
// and lease-renewal loops. One Engine serves one process; a fleet of processes
// pointed at the same database is coordinated by leases and epochs alone.
type Engine struct {
	store      Store
	pool       *pgxpool.Pool
	codec      serde.Codec
	executorID string
	appVersion string
	cfg        Config
	logf       func(string, ...any)

	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	registry  map[string]registeredWorkflow
	running   map[string]*runState
	queues    map[string]Queue
	queueLoad map[string]int // dispatched-but-maybe-not-running queue work
	kick      chan struct{}
	frozen    bool
	closed    bool

	launchOnce sync.Once
	launchErr  error

	migrateOnce sync.Once
	migrateErr  error

	wg           sync.WaitGroup
	shutdownOnce sync.Once
	shutdownErr  error
}

// New connects to Postgres and builds an engine. The context bounds the
// engine's background work; use context.Background() unless the engine really
// should stop with some other operation.
func New(ctx context.Context, cfg Config) (*Engine, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.Store == nil && cfg.DatabaseURL == "" {
		return nil, errors.New("pgflow: Config.DatabaseURL or Config.Store is required")
	}
	if cfg.Lease <= 0 {
		cfg.Lease = defaultLease
	}
	if cfg.RecoveryInterval <= 0 {
		cfg.RecoveryInterval = defaultRecoveryInterval
	}
	if cfg.RecoveryBatch <= 0 {
		cfg.RecoveryBatch = defaultRecoveryBatch
	}
	if cfg.MaxRecoveryAttempts <= 0 {
		cfg.MaxRecoveryAttempts = defaultMaxRecoveryAttempts
	}
	if cfg.QueuePollInterval <= 0 {
		cfg.QueuePollInterval = defaultQueuePollInterval
	}
	if cfg.Codec == nil {
		cfg.Codec = serde.JSON
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	executorID := cfg.ExecutorID
	if executorID == "" {
		generated, err := newExecutorID()
		if err != nil {
			return nil, err
		}
		executorID = generated
	}

	eng := &Engine{
		codec:      cfg.Codec,
		executorID: executorID,
		appVersion: cfg.AppVersion,
		cfg:        cfg,
		logf:       logf,
		registry:   make(map[string]registeredWorkflow),
		running:    make(map[string]*runState),
		queues:     make(map[string]Queue),
		queueLoad:  make(map[string]int),
		kick:       make(chan struct{}, 1),
	}
	eng.ctx, eng.cancel = context.WithCancel(ctx)

	if cfg.Store != nil {
		eng.store = cfg.Store
		return eng, nil
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		eng.cancel()
		return nil, fmt.Errorf("pgflow: connect to postgres: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		eng.cancel()
		return nil, fmt.Errorf("pgflow: connect to postgres: %w", err)
	}
	eng.pool = pool
	eng.store = &postgresStore{pool: pool}
	return eng, nil
}

// RegisterWorkflow adds a workflow function to the engine and returns the
// handle used to start it. Registration must happen before Launch, because
// recovery resolves stored workflow names through the registry.
//
// Registering the same name twice, or registering after Launch, panics: both
// are programming errors that would make recovery ambiguous.
func RegisterWorkflow[I, O any](eng *Engine, name string, fn func(*Context, I) (O, error)) *Workflow[I, O] {
	if name == "" {
		panic("pgflow: workflow name is required")
	}
	if fn == nil {
		panic("pgflow: workflow function is nil")
	}
	entry := func(c *Context, input []byte) ([]byte, error) {
		var decoded I
		if len(input) > 0 {
			if err := eng.codec.Decode(input, &decoded); err != nil {
				return nil, fmt.Errorf("pgflow: decode input for workflow %q: %w", name, err)
			}
		}
		out, err := fn(c, decoded)
		if err != nil {
			return nil, err
		}
		return eng.codec.Encode(out)
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if eng.frozen {
		panic("pgflow: workflows must be registered before Launch")
	}
	if _, exists := eng.registry[name]; exists {
		panic(fmt.Sprintf("pgflow: workflow %q is registered twice", name))
	}
	eng.registry[name] = entry
	return &Workflow[I, O]{engine: eng, name: name}
}

// Launch runs migrations, freezes the registry, recovers work left behind by
// dead executors, and starts the recovery and lease loops. It is idempotent,
// and Start calls it on first use.
func (e *Engine) Launch() error {
	e.launchOnce.Do(func() { e.launchErr = e.launch() })
	return e.launchErr
}

func (e *Engine) launch() error {
	if err := e.ensureMigrated(); err != nil {
		return fmt.Errorf("pgflow: migrate: %w", err)
	}
	e.mu.Lock()
	e.frozen = true
	if e.appVersion == "" {
		e.appVersion = hashRegistry(e.registry)
	}
	e.mu.Unlock()

	// Recover before serving so a restarted process picks its own work back up
	// immediately rather than on the first tick.
	e.recover(true)
	e.wg.Add(3)
	go func() {
		defer e.wg.Done()
		e.recoveryLoop()
	}()
	go func() {
		defer e.wg.Done()
		e.leaseLoop()
	}()
	go func() {
		defer e.wg.Done()
		e.dequeueLoop()
	}()
	return nil
}

// ensureMigrated applies pending migrations once per engine. Store-writing
// entry points call it so they work against a fresh database without a full
// Launch, which would freeze workflow registration.
func (e *Engine) ensureMigrated() error {
	if e.pool == nil {
		return nil // the memory store needs no schema
	}
	e.migrateOnce.Do(func() {
		ctx, cancel := context.WithTimeout(e.ctx, 30*time.Second)
		defer cancel()
		e.migrateErr = migrate(ctx, e.pool)
	})
	return e.migrateErr
}

// hashRegistry builds the default AppVersion from the registered workflow
// names and the Go runtime version.
func hashRegistry(registry map[string]registeredWorkflow) string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	h := sha256.New()
	for _, name := range names {
		h.Write([]byte(name))
		h.Write([]byte{0})
	}
	h.Write([]byte(runtime.Version()))
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// ExecutorID returns this process's identity.
func (e *Engine) ExecutorID() string { return e.executorID }

// AppVersion returns the version stamped on workflows this engine starts.
func (e *Engine) AppVersion() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.appVersion
}

// Workflow loads a workflow's current state, including its input, output, and
// error. It is the same row the engine and any operator can inspect.
func (e *Engine) Workflow(ctx context.Context, workflowID string) (*WorkflowRecord, error) {
	return e.store.GetWorkflow(ctx, workflowID)
}

// WorkflowEvent returns a published key/value event for a workflow. The second
// result is false while the key has not been published.
func (e *Engine) WorkflowEvent(ctx context.Context, workflowID, key string) ([]byte, bool, error) {
	return e.store.GetWorkflowEvent(ctx, workflowID, key)
}

// ListWorkflows returns workflow rows for operator tooling, newest first.
func (e *Engine) ListWorkflows(ctx context.Context, req ListWorkflowsRequest) ([]WorkflowRecord, error) {
	return e.store.ListWorkflows(ctx, req)
}

// Cancel stops a pending workflow. A local executor is cancelled immediately;
// an executor in another process notices on its next lease renewal, which is
// at most a third of the lease interval. Cancelling a finished workflow is a
// no-op.
func (e *Engine) Cancel(ctx context.Context, workflowID string) error {
	cancelled, err := e.store.CancelWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}
	if cancelled {
		e.cancelRun(workflowID)
		return nil
	}
	rec, err := e.store.GetWorkflow(ctx, workflowID)
	if err != nil {
		return err
	}
	if rec.Status.Terminal() {
		return nil
	}
	return fmt.Errorf("%w: %s", ErrWorkflowNotFound, workflowID)
}

// startOptions is the internal form of StartOption.
type startOptions struct {
	id                string
	parentID          string
	parentFunctionID  int
	hasParentFunction bool
	queueName         string
	priority          int
	partitionKey      string
	enqueue           bool
}

// StartOption configures how a workflow execution is started.
type StartOption func(*startOptions)

// WithWorkflowID sets the workflow id, the idempotency key for the execution.
// Starting a second workflow with an id that already exists returns a handle to
// the existing one instead of running a duplicate.
func WithWorkflowID(id string) StartOption {
	return func(o *startOptions) { o.id = id }
}

// start inserts the workflow row unless its id already exists, and reports
// whether this call created it.
func (e *Engine) start(ctx context.Context, name string, input []byte, opts startOptions) (*WorkflowRecord, bool, error) {
	if err := e.Launch(); err != nil {
		return nil, false, err
	}
	id := opts.id
	if id == "" {
		generated, err := newUUIDv7()
		if err != nil {
			return nil, false, err
		}
		id = generated
	}
	rec := WorkflowRecord{
		ID:               id,
		Name:             name,
		Status:           StatusPending,
		AppVersion:       e.AppVersion(),
		ExecutorID:       e.executorID,
		Epoch:            1,
		Inputs:           input,
		ParentWorkflowID: opts.parentID,
		QueueName:        opts.queueName,
		Priority:         opts.priority,
		PartitionKey:     opts.partitionKey,
	}
	if opts.queueName != "" || opts.enqueue {
		// Queue-backed starts wait for a dequeue sweep instead of running here.
		rec.Status = StatusEnqueued
		rec.ExecutorID = ""
	}
	if opts.hasParentFunction {
		functionID := opts.parentFunctionID
		rec.ParentFunctionID = &functionID
	}
	created, existing, err := e.store.CreateWorkflow(ctx, rec, e.cfg.Lease)
	if err != nil {
		return nil, false, err
	}
	if !created {
		if existing == nil {
			return nil, false, fmt.Errorf("%w: %s", ErrWorkflowNotFound, id)
		}
		if existing.Name != name {
			return nil, false, fmt.Errorf("%w: id %q runs %q", ErrWorkflowConflict, id, existing.Name)
		}
		return existing, false, nil
	}
	if rec.Status == StatusEnqueued {
		// Nudge the local sweep so a queued job starts now instead of at the
		// next poll tick; the poll stays as the fallback for cross-process
		// enqueues. Non-blocking: a pending sweep covers it.
		select {
		case e.kick <- struct{}{}:
		default:
		}
	}
	return &rec, true, nil
}

// dispatch runs a newly owned workflow in the background.
func (e *Engine) dispatch(rec *WorkflowRecord) {
	e.mu.Lock()
	if e.closed || e.ctx.Err() != nil {
		e.mu.Unlock()
		return
	}
	// Count queue work at dispatch time: the queue sweep must see that this
	// slot is taken before execute() registers the run itself.
	if rec.QueueName != "" {
		e.queueLoad[rec.QueueName]++
	}
	e.wg.Add(1)
	e.mu.Unlock()
	go e.execute(*rec)
}

// execute runs one workflow to a terminal state and records the outcome.
func (e *Engine) execute(rec WorkflowRecord) {
	defer e.wg.Done()
	if rec.QueueName != "" {
		// Pairs with dispatch's increment, including the fenced early return.
		defer func() {
			e.mu.Lock()
			if n := e.queueLoad[rec.QueueName] - 1; n > 0 {
				e.queueLoad[rec.QueueName] = n
			} else {
				delete(e.queueLoad, rec.QueueName)
			}
			e.mu.Unlock()
		}()
	}

	runCtx, cancel := context.WithCancel(e.ctx)
	state := &runState{epoch: rec.Epoch, cancel: cancel, queue: rec.QueueName}
	e.mu.Lock()
	if current, ok := e.running[rec.ID]; ok {
		if current.epoch >= rec.Epoch {
			// The same or a newer execution already owns the workflow.
			e.mu.Unlock()
			cancel()
			return
		}
		current.cancel()
	}
	e.running[rec.ID] = state
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		if e.running[rec.ID] == state {
			delete(e.running, rec.ID)
		}
		e.mu.Unlock()
		cancel()
	}()

	history, err := e.store.LoadCheckpoints(e.ctx, rec.ID)
	if err != nil {
		e.logf("pgflow: load history for %s: %v", rec.ID, err)
		return
	}

	spanCtx, span := tracer.Start(runCtx, "pgflow.workflow "+rec.Name, trace.WithAttributes(
		attribute.String("pgflow.workflow_id", rec.ID),
		attribute.Int64("pgflow.epoch", rec.Epoch),
		attribute.Int("pgflow.recorded_operations", len(history)),
	))
	wfCtx := &Context{ctx: spanCtx, eng: e, record: rec, history: history, childIDs: make(map[int]string)}
	output, runErr := e.invoke(wfCtx, rec)
	wfCtx.wg.Wait()
	if runErr != nil && !errors.Is(runErr, ErrFenced) && !errors.Is(runErr, ErrCancelled) {
		span.SetStatus(codes.Error, runErr.Error())
	}
	span.End()
	if e.ctx.Err() != nil {
		// Shutting down: leave the workflow PENDING so another executor (or
		// this one after a restart) recovers it from its checkpoints.
		return
	}
	if errors.Is(runErr, ErrFenced) {
		// Another executor owns the workflow now; its history is canonical.
		return
	}
	if errors.Is(runErr, ErrContinueAsNew) {
		// The workflow ended itself by starting a fresh execution; its result
		// lives in the continuation. Record the continuation BEFORE marking
		// the workflow SUCCESS: a terminal row always names its successor, so
		// a waiter can never observe the gap between the two writes. A crash
		// after the first write leaves the workflow PENDING with the link
		// recorded; recovery replays, reattaches to the same child and
		// completes it again.
		childID, ok := wfCtx.continuedTo()
		if ok {
			if err := e.store.SetContinuedTo(e.ctx, rec.ID, childID); err != nil {
				e.logf("pgflow: record continuation of %s: %v", rec.ID, err)
				return
			}
			if _, err := e.store.CompleteWorkflow(e.ctx, rec.ID, rec.Epoch, StatusSuccess, nil, nil); err != nil {
				e.logf("pgflow: continue workflow %s: %v", rec.ID, err)
			}
			return
		}
		runErr = fmt.Errorf("pgflow: workflow %s returned ErrContinueAsNew without starting one", rec.Name)
	}
	status := StatusSuccess
	var info *ErrorInfo
	if runErr != nil {
		output = nil
		info = encodeError(runErr)
		status = StatusError
		if errors.Is(runErr, ErrCancelled) {
			status = StatusCancelled
		}
	}
	if _, err := e.store.CompleteWorkflow(e.ctx, rec.ID, rec.Epoch, status, output, info); err != nil {
		e.logf("pgflow: complete workflow %s: %v", rec.ID, err)
	}
}

// invoke calls the registered function, turning a panic into a workflow error.
func (e *Engine) invoke(c *Context, rec WorkflowRecord) (out []byte, err error) {
	e.mu.Lock()
	entry, ok := e.registry[rec.Name]
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("pgflow: no workflow registered as %q", rec.Name)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("pgflow: workflow %q panicked: %v", rec.Name, recovered)
		}
	}()
	return entry(c, rec.Inputs)
}

func (e *Engine) cancelRun(workflowID string) {
	e.mu.Lock()
	state := e.running[workflowID]
	e.mu.Unlock()
	if state != nil {
		state.cancel()
	}
}

// Shutdown stops the background loops and every in-flight workflow, then waits
// for them to return. In-flight workflows stay PENDING and are recovered by
// another executor once their leases expire.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.shutdownOnce.Do(func() {
		e.mu.Lock()
		e.closed = true
		cancels := make([]context.CancelFunc, 0, len(e.running))
		for _, state := range e.running {
			cancels = append(cancels, state.cancel)
		}
		e.mu.Unlock()
		for _, cancel := range cancels {
			cancel()
		}
		e.cancel()

		drained := make(chan struct{})
		go func() {
			e.wg.Wait()
			close(drained)
		}()
		select {
		case <-drained:
		case <-ctx.Done():
			e.shutdownErr = ctx.Err()
		}
		if e.pool != nil {
			e.pool.Close()
		}
	})
	return e.shutdownErr
}
