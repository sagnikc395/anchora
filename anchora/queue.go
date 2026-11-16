package anchora

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WithQueue starts a workflow as ENQUEUED work, which runs when an executor
// that registered the queue sweeps it. The queue name must have been passed to
// Engine.RegisterQueue on the executors that should drain it.
func WithQueue(name string) StartOption {
	return func(o *startOptions) { o.queueName = name }
}

// WithPriority orders queued work: higher values dequeue first.
func WithPriority(priority int) StartOption {
	return func(o *startOptions) { o.priority = priority }
}

// WithPartition assigns queued work to a partition, typically a tenant or
// user id. PartitionConcurrency caps how many workflows of one partition run
// at once, so one tenant's burst cannot starve the others.
func WithPartition(key string) StartOption {
	return func(o *startOptions) { o.partitionKey = key }
}

// Enqueue starts a workflow from inside another workflow, recording the start
// as an operation so it happens exactly once per function slot: a replayed
// parent reattaches to the same queue entry instead of enqueueing a duplicate.
//
// The child id is derived from this slot, so the enqueue is idempotent no
// matter where a crash lands. Pass WithQueue to choose the destination queue;
// WithPriority and WithPartition order and shard it.
func Enqueue[I, O any](c *Context, w *Workflow[I, O], input I, opts ...StartOption) error {
	if w == nil || w.engine != c.eng {
		return errors.New("anchora: enqueued workflow must be registered on the same engine")
	}
	name := "enqueue:" + w.name
	functionID, prior, err := c.prepare(name, KindEnqueue)
	if err != nil {
		return err
	}
	if prior != nil {
		// Already enqueued by an earlier attempt.
		return nil
	}
	options := startOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.queueName == "" {
		return errors.New("anchora: enqueue requires WithQueue")
	}
	encoded, err := c.eng.codec.Encode(input)
	if err != nil {
		return fmt.Errorf("anchora: encode input for workflow %q: %w", w.name, err)
	}
	childID := fmt.Sprintf("%s/%d", c.record.ID, functionID)
	_, created, err := c.eng.start(c.ctx, w.name, encoded, startOptions{
		id:       childID,
		parentID: c.record.ID,
		enqueue:  true,
	})
	if err != nil {
		return fmt.Errorf("anchora: enqueue workflow %q: %w", w.name, err)
	}
	cp := &Checkpoint{
		FunctionID:      functionID,
		FunctionName:    name,
		Kind:            KindEnqueue,
		Attempts:        1,
		ChildWorkflowID: childID,
		StartedAt:       time.Now(),
		CompletedAt:     time.Now(),
	}
	if _, err := c.persist(cp); err != nil {
		return err
	}
	if created {
		c.mu.Lock()
		c.childIDs[functionID] = childID
		c.mu.Unlock()
	}
	return nil
}

// RegisterQueue adds a queue to this executor: it stores the configuration in
// the anchora.queues table and starts draining the queue in this process.
// Registering a queue whose name is empty panics.
func (e *Engine) RegisterQueue(ctx context.Context, q Queue) error {
	if q.Name == "" {
		return errors.New("anchora: queue name is required")
	}
	if err := e.ensureMigrated(); err != nil {
		return fmt.Errorf("anchora: register queue %q: %w", q.Name, err)
	}
	if err := e.store.UpsertQueue(ctx, q); err != nil {
		return fmt.Errorf("anchora: register queue %q: %w", q.Name, err)
	}
	e.mu.Lock()
	e.queues[q.Name] = q
	e.mu.Unlock()
	return nil
}

// dequeueLoop sweeps every registered queue for ENQUEUED work. Sweeps fire on
// each tick and on every local enqueue, so a queued job starts immediately;
// the tick remains the fallback for enqueues from other processes.
func (e *Engine) dequeueLoop() {
	ticker := time.NewTicker(e.cfg.QueuePollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-e.kick:
			e.dequeueAll()
		case <-ticker.C:
			e.dequeueAll()
		}
	}
}

func (e *Engine) dequeueAll() {
	e.mu.Lock()
	queues := make([]Queue, 0, len(e.queues))
	for _, q := range e.queues {
		queues = append(queues, q)
	}
	e.mu.Unlock()
	for _, q := range queues {
		e.dequeueQueue(q)
	}
}

func (e *Engine) dequeueQueue(q Queue) {
	limit := q.LocalConcurrency
	if limit <= 0 {
		limit = e.cfg.RecoveryBatch
	} else {
		// Leave room for this queue's workflows already dispatched on this
		// executor.
		e.mu.Lock()
		running := e.queueLoad[q.Name]
		e.mu.Unlock()
		if running >= limit {
			return
		}
		limit -= running
	}
	records, err := e.store.DequeueWorkflows(e.ctx, DequeueRequest{
		ExecutorID:           e.executorID,
		AppVersion:           e.AppVersion(),
		Queue:                q.Name,
		Lease:                e.cfg.Lease,
		Limit:                limit,
		GlobalConcurrency:    q.GlobalConcurrency,
		PartitionConcurrency: q.PartitionConcurrency,
		RateLimitCount:       q.RateLimitCount,
		RateLimitPeriod:      q.RateLimitPeriod,
	})
	if err != nil {
		if e.ctx.Err() == nil {
			e.logf("anchora: dequeue %s: %v", q.Name, err)
		}
		return
	}
	for i := range records {
		e.dispatch(&records[i])
	}
	if len(records) > 0 {
		dequeued.WithLabelValues(q.Name).Add(float64(len(records)))
	}
}
