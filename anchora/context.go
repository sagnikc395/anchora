package anchora

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sagnikc395/anchora/anchora/serde"
)

// Context is the engine handle a workflow function receives. Workflow code
// may only do deterministic work plus engine calls made through this context;
// everything nondeterministic belongs inside a Step.
type Context struct {
	ctx    context.Context
	eng    *Engine
	record WorkflowRecord

	mu             sync.Mutex
	nextFunctionID int
	history        map[int]*Checkpoint
	// childIDs maps an enqueue or continue slot to the workflow id it created,
	// so the engine can follow a ContinueAsNew chain after the workflow ends.
	childIDs map[int]string
	// wg tracks parallel steps started with Go; the engine drains it before
	// completing the workflow.
	wg sync.WaitGroup
}

// WorkflowID returns the id this execution is stored under.
func (c *Context) WorkflowID() string { return c.record.ID }

// Name returns the registered workflow name.
func (c *Context) Name() string { return c.record.Name }

// Done is closed when the workflow is cancelled or the engine shuts down.
func (c *Context) Done() <-chan struct{} { return c.ctx.Done() }

// Err reports why the workflow is stopping, if it is.
func (c *Context) Err() error { return c.ctx.Err() }

// check turns a cancelled workflow context into ErrCancelled before any new
// side effect starts.
func (c *Context) check() error {
	if err := c.ctx.Err(); err != nil {
		return fmt.Errorf("%w: %v", ErrCancelled, err)
	}
	return nil
}

// prepare reserves the next function id and returns the checkpoint recorded at
// that slot, if any. A recorded operation with a different name or kind means
// the code no longer matches its history, which is fatal for the workflow.
func (c *Context) prepare(name string, kind Kind) (int, *Checkpoint, error) {
	if err := c.check(); err != nil {
		return 0, nil, err
	}
	c.mu.Lock()
	functionID := c.nextFunctionID
	c.nextFunctionID++
	prior := c.history[functionID]
	c.mu.Unlock()
	if prior == nil {
		observeOperation(kind, false)
		return functionID, nil, nil
	}
	if prior.FunctionName != name || prior.Kind != kind {
		nondeterminism.Inc()
		return functionID, nil, &NondeterminismError{
			WorkflowID: c.record.ID,
			FunctionID: functionID,
			Expected:   fmt.Sprintf("%s %q", prior.Kind, prior.FunctionName),
			Actual:     fmt.Sprintf("%s %q", kind, name),
		}
	}
	observeOperation(kind, true)
	return functionID, prior, nil
}

// continuedTo reports the workflow this execution handed its role to through
// ContinueAsNew, if it did.
func (c *Context) continuedTo() (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range c.childIDs {
		return id, true
	}
	return "", false
}

// checkpointable reports whether a terminal step error may be written to
// history. A cancelled or fenced execution must leave no trace: the workflow
// is expected to resume elsewhere, and replaying a recorded cancellation would
// make it unresumable.
func (c *Context) checkpointable(err error) bool {
	if err == nil {
		return true
	}
	return c.ctx.Err() == nil && !errors.Is(err, ErrFenced)
}

// persist writes a live checkpoint, fenced on the execution's epoch. If another
// executor recorded the slot first, its value wins and is returned: the
// workflow only ever observes durable, single-writer history.
func (c *Context) persist(cp *Checkpoint) (*Checkpoint, error) {
	var created bool
	var existing *Checkpoint
	var writeErr error
	observeCheckpoint(func() {
		created, existing, writeErr = c.eng.store.SaveCheckpoint(c.ctx, c.record.ID, c.record.Epoch, cp)
	})
	if writeErr != nil {
		return nil, writeErr
	}
	if !created {
		if existing == nil {
			return nil, fmt.Errorf("anchora: checkpoint %s/%d was neither written nor found", c.record.ID, cp.FunctionID)
		}
		return existing, nil
	}
	c.mu.Lock()
	c.history[cp.FunctionID] = cp
	c.mu.Unlock()
	return cp, nil
}

// decodeCheckpoint returns the recorded value, or the recorded error.
func decodeCheckpoint[T any](codec serde.Codec, cp *Checkpoint) (T, error) {
	var value T
	if cp == nil {
		return value, fmt.Errorf("anchora: missing checkpoint")
	}
	if cp.Err != nil {
		return value, cp.Err.Err()
	}
	if len(cp.Output) == 0 {
		return value, nil
	}
	if err := codec.Decode(cp.Output, &value); err != nil {
		return value, fmt.Errorf("anchora: decode checkpoint %q/%d: %w", cp.FunctionName, cp.FunctionID, err)
	}
	return value, nil
}

// recordValue implements a deterministic recorded read: the live function runs
// once, its value is checkpointed, and every replay returns the checkpoint.
func recordValue[T any](c *Context, name string, kind Kind, live func() (T, error)) (T, error) {
	var zero T
	functionID, prior, err := c.prepare(name, kind)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return decodeCheckpoint[T](c.eng.codec, prior)
	}
	value, liveErr := live()
	cp := &Checkpoint{
		FunctionID:   functionID,
		FunctionName: name,
		Kind:         kind,
		Attempts:     1,
		StartedAt:    time.Now(),
		CompletedAt:  time.Now(),
	}
	if liveErr != nil {
		cp.Err = encodeError(liveErr)
	} else {
		encoded, err := c.eng.codec.Encode(value)
		if err != nil {
			return zero, fmt.Errorf("anchora: encode %s: %w", name, err)
		}
		cp.Output = encoded
	}
	saved, err := c.persist(cp)
	if err != nil {
		return zero, err
	}
	return decodeCheckpoint[T](c.eng.codec, saved)
}
