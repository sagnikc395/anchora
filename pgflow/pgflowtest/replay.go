// Package pgflowtest provides test helpers for pgflow workflows: replay
// testing against a recorded history and fault-injection plumbing for crash
// tests.
package pgflowtest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/pgflow"
	"github.com/sagnikc395/anchora/pgflow/memory"
)

// History loads a workflow's recorded operations from a store. Record it from
// a production or staging database, check it into the test tree, and replay it
// against changed code before deploying.
func History(ctx context.Context, store pgflow.Store, workflowID string) (map[int]*pgflow.Checkpoint, error) {
	return store.LoadCheckpoints(ctx, workflowID)
}

// Replay runs fn against a recorded history from an earlier execution. The
// recorded operations return their stored values without calling their step
// functions; the first slot without a recording runs live.
//
// It fails the test when replaying diverges from the history — a
// NondeterminismError naming the expected and actual operation at the
// diverging slot — which is how a workflow change is caught before deploy.
func Replay[I, O any](t *testing.T, fn func(*pgflow.Context, I) (O, error), name string, history map[int]*pgflow.Checkpoint, input I) (O, error) {
	t.Helper()
	var zero O
	if name == "" {
		return zero, fmt.Errorf("pgflowtest: workflow name is required")
	}
	ctx := context.Background()
	store := memory.New()
	eng, err := pgflow.New(ctx, pgflow.Config{Store: store, AppVersion: "replay"})
	if err != nil {
		return zero, err
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = eng.Shutdown(shutdown)
	}()
	workflow := pgflow.RegisterWorkflow(eng, name, fn)

	id := "replay-" + name
	if _, _, err := store.CreateWorkflow(ctx, pgflow.WorkflowRecord{
		ID:         id,
		Name:       name,
		Status:     pgflow.StatusPending,
		AppVersion: "replay",
		ExecutorID: eng.ExecutorID(), // startup recovery claims its own rows
	}, time.Minute); err != nil {
		return zero, err
	}
	for _, cp := range history {
		if _, _, err := store.SaveCheckpoint(ctx, id, 1, cp); err != nil {
			return zero, fmt.Errorf("pgflowtest: seed history at slot %d: %w", cp.FunctionID, err)
		}
	}
	handle, err := workflow.Start(ctx, input, pgflow.WithWorkflowID(id))
	if err != nil {
		return zero, err
	}
	deadline, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := handle.Result(deadline)
	if err == nil {
		return out, nil
	}
	return zero, err
}

// CrashAfterEnv builds the PGFLOW_CRASH_AFTER value that kills the process
// when step reaches phase. Wire it into a subprocess test (see the
// shutdown-and-recover tests in pgflow for the in-process equivalent).
func CrashAfterEnv(step, phase string) string {
	return step + ":" + phase
}
