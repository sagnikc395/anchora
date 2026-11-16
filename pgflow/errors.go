package pgflow

import (
	"errors"
	"fmt"
	"reflect"
)

var (
	// ErrWorkflowNotFound reports an id the store does not know.
	ErrWorkflowNotFound = errors.New("pgflow: workflow not found")
	// ErrWorkflowConflict reports a reused id that already belongs to a
	// workflow with a different name.
	ErrWorkflowConflict = errors.New("pgflow: workflow id is already used by another workflow")
	// ErrFenced reports that this executor no longer owns the workflow: a
	// newer executor claimed it, so its writes must be discarded.
	ErrFenced = errors.New("pgflow: workflow was taken over by a newer executor")
	// ErrCancelled reports that the workflow's context was cancelled, either
	// by Engine.Cancel or by Engine.Shutdown.
	ErrCancelled = errors.New("pgflow: workflow cancelled")
	// ErrTimeout reports a step timeout, a step budget, or a Recv deadline.
	ErrTimeout = errors.New("pgflow: timed out")
	// ErrNondeterminism reports replay hitting a different engine call than the
	// recorded history at the same function slot.
	ErrNondeterminism = errors.New("pgflow: nondeterministic replay")
	// ErrMaxRecoveryAttempts reports a workflow that keeps crashing its
	// executor and has been given up on.
	ErrMaxRecoveryAttempts = errors.New("pgflow: workflow exceeded its recovery attempt limit")
)

// NondeterminismError is returned when replayed workflow code calls a
// different operation than the one recorded at the same function id. The
// workflow is failed rather than silently producing a wrong result.
type NondeterminismError struct {
	WorkflowID string
	FunctionID int
	// Expected is what the recorded history has at this slot.
	Expected string
	// Actual is what the running code called.
	Actual string
}

func (e *NondeterminismError) Error() string {
	return fmt.Sprintf("pgflow: workflow %s diverged at function %d: history has %s, code called %s",
		e.WorkflowID, e.FunctionID, e.Expected, e.Actual)
}

// Is makes errors.Is(err, ErrNondeterminism) true.
func (e *NondeterminismError) Is(target error) bool { return target == ErrNondeterminism }

// StepError is a step or workflow error rehydrated from a checkpoint. The
// engine preserves the original Go type name and message, but not the error
// value itself: a step that replays fails with a *StepError, not with the
// original sentinel.
type StepError struct {
	Type    string
	Message string
}

func (e *StepError) Error() string { return e.Message }

// encodeError turns a terminal error into its persisted form.
func encodeError(err error) *ErrorInfo {
	if err == nil {
		return nil
	}
	info := &ErrorInfo{Type: reflect.TypeOf(err).String(), Message: err.Error()}
	switch {
	case errors.Is(err, ErrNondeterminism):
		info.Type = "nondeterminism"
	case errors.Is(err, ErrTimeout):
		info.Type = "timeout"
	case errors.Is(err, ErrCancelled):
		info.Type = "cancelled"
	case errors.Is(err, ErrFenced):
		info.Type = "fenced"
	}
	return info
}

// Err rehydrates a stored error. Engine sentinels stay matchable with
// errors.Is; any other error comes back as a *StepError.
func (i *ErrorInfo) Err() error {
	if i == nil {
		return nil
	}
	switch i.Type {
	case "nondeterminism":
		return fmt.Errorf("%w: %s", ErrNondeterminism, i.Message)
	case "timeout":
		return fmt.Errorf("%w: %s", ErrTimeout, i.Message)
	case "cancelled":
		return fmt.Errorf("%w: %s", ErrCancelled, i.Message)
	case "fenced":
		return fmt.Errorf("%w: %s", ErrFenced, i.Message)
	default:
		return &StepError{Type: i.Type, Message: i.Message}
	}
}
