package pgflow

import (
	"time"
)

// Patched reports whether the workflow's history was produced by the patched
// branch. Call it before new behavior:
//
//	if pgflow.Patched(ctx, "use-new-planner") {
//	    ... new code ...
//	} else {
//	    ... old code, kept verbatim ...
//	}
//
// A fresh execution records a marker and takes the patched branch; replays of
// that execution consume the marker. A replay of an older history finds a
// different operation at the current slot, takes the unpatched branch, and
// matches the history. The unpatched branch must therefore be kept unchanged
// for as long as history recorded before the patch may be replayed.
//
// If a replay's history has an unrelated marker at the current slot, the
// branch runs on and the next engine call fails with ErrNondeterminism naming
// both operations.
func Patched(c *Context, name string) bool {
	if name == "" {
		return false
	}
	c.mu.Lock()
	functionID := c.nextFunctionID
	prior := c.history[functionID]
	c.mu.Unlock()

	if prior == nil {
		// No history yet: record the marker so every future replay of this
		// execution takes the patched branch too.
		cp := &Checkpoint{
			FunctionID:   functionID,
			FunctionName: name,
			Kind:         KindPatch,
			Attempts:     1,
			StartedAt:    time.Now(),
			CompletedAt:  time.Now(),
		}
		if _, err := c.persist(cp); err != nil {
			return false
		}
		c.mu.Lock()
		c.nextFunctionID++
		c.mu.Unlock()
		return true
	}
	if prior.Kind != KindPatch {
		// History predates the patch: the unpatched branch replays unchanged.
		// Do not consume the slot; the branch's own operations must match it.
		return false
	}
	if prior.FunctionName != name {
		// A different patch occupies this slot: the code reordered its patch
		// checks, which cannot replay safely. Leave the slot in place; the old
		// branch's next engine call reports the nondeterminism.
		return false
	}
	c.mu.Lock()
	c.nextFunctionID++
	c.mu.Unlock()
	// A recorded patch marker whose checkpoint carries an error (a fenced
	// write) is treated as unpatched.
	return prior.Err == nil
}
