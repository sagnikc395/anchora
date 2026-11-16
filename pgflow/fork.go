package pgflow

import (
	"context"
	"errors"
	"fmt"
)

// Fork creates a new execution of w that reuses another execution's history up
// to fromFunctionID: the fork replays the copied steps for free and runs live
// from that slot on. For agents this means rerunning from a chosen step
// without paying for the work before it.
//
// The new workflow runs w, so it must be registered on this engine and its
// app version may differ from the source: forking is how you debug a workflow
// with changed code from a known earlier state.
func Fork[I, O any](ctx context.Context, w *Workflow[I, O], srcWorkflowID string, fromFunctionID int, opts ...StartOption) (*Handle[O], error) {
	if w == nil {
		return nil, errors.New("pgflow: workflow is nil")
	}
	options := startOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	src, err := w.engine.store.GetWorkflow(ctx, srcWorkflowID)
	if err != nil {
		return nil, err
	}
	if src.Name != w.name {
		return nil, fmt.Errorf("%w: %s runs %q, not %q", ErrWorkflowConflict, srcWorkflowID, src.Name, w.name)
	}
	newID := options.id
	if newID == "" {
		generated, err := newUUIDv7()
		if err != nil {
			return nil, err
		}
		newID = generated
	}
	rec, err := w.engine.store.ForkWorkflow(ctx, srcWorkflowID, newID, fromFunctionID, w.engine.executorID, w.engine.cfg.Lease)
	if err != nil {
		return nil, err
	}
	w.engine.dispatch(rec)
	return &Handle[O]{engine: w.engine, id: newID}, nil
}
