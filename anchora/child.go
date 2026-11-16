package anchora

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Workflow is a registered workflow function. It is the handle used to start
// executions and, through Child, to compose workflows.
type Workflow[I, O any] struct {
	engine *Engine
	name   string
}

// Name is the registered name.
func (w *Workflow[I, O]) Name() string { return w.name }

// Start creates a workflow execution and begins running it. The returned handle
// is ready before the workflow finishes; use Result to wait.
//
// The id is the idempotency key: starting the same id twice returns a handle to
// the existing execution instead of running a second one.
func (w *Workflow[I, O]) Start(ctx context.Context, input I, opts ...StartOption) (*Handle[O], error) {
	options := startOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	encoded, err := w.engine.codec.Encode(input)
	if err != nil {
		return nil, fmt.Errorf("anchora: encode input for workflow %q: %w", w.name, err)
	}
	rec, created, err := w.engine.start(ctx, w.name, encoded, options)
	if err != nil {
		return nil, err
	}
	// Queue-backed rows wait for a dequeue sweep instead of running here.
	if created && rec.Status == StatusPending {
		w.engine.dispatch(rec)
	}
	return &Handle[O]{engine: w.engine, id: rec.ID}, nil
}

// Handle is a started workflow execution.
type Handle[O any] struct {
	engine *Engine
	id     string
}

// ID returns the workflow id.
func (h *Handle[O]) ID() string { return h.id }

// Result waits for the workflow to reach a terminal state and returns its
// output, or its error.
func (h *Handle[O]) Result(ctx context.Context) (O, error) {
	var zero O
	rec, err := h.engine.await(ctx, h.id)
	if err != nil {
		return zero, err
	}
	if len(rec.Output) == 0 {
		return zero, nil
	}
	if err := h.engine.codec.Decode(rec.Output, &zero); err != nil {
		return zero, fmt.Errorf("anchora: decode output of workflow %s: %w", h.id, err)
	}
	return zero, nil
}

// await polls a workflow until it is terminal. Polling costs one primary-key
// lookup per interval and needs no listener connection, which keeps the engine
// a library rather than a service. A workflow that ended with ContinueAsNew
// reports the result of its final continuation.
func (e *Engine) await(ctx context.Context, workflowID string) (*WorkflowRecord, error) {
	delay := 20 * time.Millisecond
	for hops := 0; ; {
		rec, err := e.store.GetWorkflow(ctx, workflowID)
		if err != nil {
			return nil, err
		}
		switch rec.Status {
		case StatusSuccess:
			if rec.ContinuedTo != "" {
				hops++
				if hops > 1000 {
					return nil, fmt.Errorf("anchora: continuation chain from %s does not terminate", workflowID)
				}
				workflowID = rec.ContinuedTo
				continue
			}
			return rec, nil
		case StatusError, StatusCancelled, StatusRetriesExceeded:
			if rec.Error != nil {
				return nil, rec.Error.Err()
			}
			return nil, fmt.Errorf("anchora: workflow %s finished as %s", workflowID, rec.Status)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		if delay < time.Second {
			delay *= 2
		}
	}
}

// Child starts a child workflow and returns its result. The child id is derived
// from the parent id and the function slot, so replay reattaches to the same
// child instead of starting a duplicate.
//
// The parent's checkpoint records the child's terminal result only; if the
// parent crashes while the child is running, the recovered parent rejoins the
// existing child and waits for it.
func Child[I, O any](c *Context, w *Workflow[I, O], input I) (O, error) {
	var zero O
	if w == nil || w.engine != c.eng {
		return zero, errors.New("anchora: child workflow must be registered on the same engine")
	}
	name := "child:" + w.name
	functionID, prior, err := c.prepare(name, KindChild)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return decodeCheckpoint[O](c.eng.codec, prior)
	}

	encoded, err := c.eng.codec.Encode(input)
	if err != nil {
		return zero, fmt.Errorf("anchora: encode input for child workflow %q: %w", w.name, err)
	}
	childID := fmt.Sprintf("%s/%d", c.record.ID, functionID)
	rec, created, err := c.eng.start(c.ctx, w.name, encoded, startOptions{
		id:                childID,
		parentID:          c.record.ID,
		parentFunctionID:  functionID,
		hasParentFunction: true,
	})
	if err != nil {
		return zero, err
	}
	if created {
		c.eng.dispatch(rec)
	}

	childRecord, waitErr := c.eng.await(c.ctx, childID)
	cp := &Checkpoint{
		FunctionID:      functionID,
		FunctionName:    name,
		Kind:            KindChild,
		Attempts:        1,
		ChildWorkflowID: childID,
		StartedAt:       time.Now(),
		CompletedAt:     time.Now(),
	}
	if waitErr != nil {
		if c.ctx.Err() != nil {
			return zero, fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
		}
		cp.Err = encodeError(waitErr)
	} else {
		cp.Output = childRecord.Output
	}
	saved, err := c.persist(cp)
	if err != nil {
		return zero, err
	}
	return decodeCheckpoint[O](c.eng.codec, saved)
}
