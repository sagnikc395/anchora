// Package anchora runs ordered, retryable agent workflows.
package anchora

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Status string

const (
	Pending   Status = "pending"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Retrying  Status = "retrying"
)

// Agent is the only dependency required by a workflow. Applications can adapt
// an SDK, a queue, or another HTTP service to this interface.
type Agent interface {
	Run(context.Context, string) (string, error)
}
type Step struct {
	ID     string
	Agent  Agent
	Prompt string
}
type StepResult struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Attempts int    `json:"attempts"`
	Output   string `json:"output,omitempty"`
	Error    string `json:"error,omitempty"`
}
type Options struct {
	MaxRetries int
	RetryDelay time.Duration
}
type Workflow struct {
	steps   []Step
	options Options
}

func NewWorkflow(steps []Step, options Options) (*Workflow, error) {
	if options.MaxRetries < 0 || options.RetryDelay < 0 {
		return nil, errors.New("workflow options must be non-negative")
	}
	if len(steps) == 0 {
		return nil, errors.New("workflow requires at least one step")
	}
	seen := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if step.ID == "" || step.Prompt == "" || step.Agent == nil {
			return nil, errors.New("each step requires an id, agent, and prompt")
		}
		if _, ok := seen[step.ID]; ok {
			return nil, fmt.Errorf("duplicate step id %q", step.ID)
		}
		seen[step.ID] = struct{}{}
	}
	return &Workflow{steps: append([]Step(nil), steps...), options: options}, nil
}

// Run executes steps in order and returns the state of every step reached.
func (w *Workflow) Run(ctx context.Context) ([]StepResult, error) {
	results := make([]StepResult, 0, len(w.steps))
	for _, step := range w.steps {
		result := w.runStep(ctx, step)
		results = append(results, result)
		if result.Status == Failed {
			return results, &WorkflowError{StepID: step.ID, Err: errors.New(result.Error)}
		}
	}
	return results, nil
}
func (w *Workflow) runStep(ctx context.Context, step Step) StepResult {
	result := StepResult{ID: step.ID, Status: Pending}
	for {
		result.Status = Running
		output, err := step.Agent.Run(ctx, step.Prompt)
		if err == nil {
			result.Status, result.Output, result.Error = Succeeded, output, ""
			return result
		}
		result.Error = err.Error()
		if result.Attempts >= w.options.MaxRetries {
			result.Status = Failed
			return result
		}
		result.Attempts++
		result.Status = Retrying
		delay := time.Duration(result.Attempts) * w.options.RetryDelay
		select {
		case <-ctx.Done():
			result.Status, result.Error = Failed, ctx.Err().Error()
			return result
		case <-time.After(delay):
		}
	}
}

type WorkflowError struct {
	StepID string
	Err    error
}

func (e *WorkflowError) Error() string {
	return fmt.Sprintf("workflow failed at step %q: %v", e.StepID, e.Err)
}
func (e *WorkflowError) Unwrap() error { return e.Err }
