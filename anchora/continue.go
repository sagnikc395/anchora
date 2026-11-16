package anchora

import (
	"errors"
	"fmt"
	"time"
)

// ErrContinueAsNew is returned by ContinueAsNew. A workflow function returns
// it after handing its state to a fresh execution; the engine then completes
// the workflow as SUCCESS and links it to the continuation.
var ErrContinueAsNew = errors.New("anchora: continue as new")

// ContinueAsNew ends the current workflow and starts a fresh execution of w
// carrying input, with a fresh history. Long-running agent loops call it
// periodically so replays never walk an unbounded event log: the next
// execution starts from the carried-over state alone.
//
// The caller returns ErrContinueAsNew (or the value this returns) from the
// workflow function. Awaited handles follow the chain and return the final
// execution's result.
//
// The continuation id is derived from this slot like a child workflow, so a
// replay reattaches to the same continuation instead of starting a duplicate.
func ContinueAsNew[I, O any](c *Context, w *Workflow[I, O], input I) error {
	if w == nil || w.engine != c.eng {
		return errors.New("anchora: continued workflow must be registered on the same engine")
	}
	name := "continue:" + w.name
	functionID, prior, err := c.prepare(name, KindChild)
	if err != nil {
		return err
	}
	if prior != nil {
		// A replay of an execution that already started its continuation.
		// Reattach to the same child so the engine can follow the chain.
		c.mu.Lock()
		c.childIDs[prior.FunctionID] = prior.ChildWorkflowID
		c.mu.Unlock()
		return ErrContinueAsNew
	}
	childID := fmt.Sprintf("%s/%d", c.record.ID, functionID)
	cp := &Checkpoint{
		FunctionID:      functionID,
		FunctionName:    name,
		Kind:            KindChild,
		Attempts:        1,
		ChildWorkflowID: childID,
		StartedAt:       time.Now(),
		CompletedAt:     time.Now(),
	}
	// The continuation is recorded before the parent's history is abandoned,
	// so a crash between the two steps reattaches instead of duplicating.
	if _, err := c.persist(cp); err != nil {
		return err
	}
	c.mu.Lock()
	c.childIDs[functionID] = childID
	c.mu.Unlock()

	encoded, err := c.eng.codec.Encode(input)
	if err != nil {
		return err
	}
	rec, created, err := c.eng.start(c.ctx, w.name, encoded, startOptions{
		id:       childID,
		parentID: c.record.ID,
	})
	if err != nil {
		return err
	}
	if created {
		c.eng.dispatch(rec)
	}
	return ErrContinueAsNew
}
