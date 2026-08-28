// Package anchora runs ordered, retryable agent workflows.
package anchora

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

type Status string

const (
	Pending   Status = "pending"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Retrying  Status = "retrying"
	Skipped   Status = "skipped"
)

// Agent is the only dependency required by a workflow. Applications can adapt
// an SDK, a queue, or another HTTP service to this interface.
type Agent interface {
	Run(context.Context, string) (string, error)
}
type Step struct {
	ID        string
	Agent     Agent
	Prompt    string
	DependsOn []string
}
type StepResult struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Attempts int    `json:"attempts"`
	Output   string `json:"output,omitempty"`
	Error    string `json:"error,omitempty"`
}
type Options struct {
	MaxRetries  int
	RetryDelay  time.Duration
	OnStepState func(StepResult)
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
	for _, step := range steps {
		dependencies := make(map[string]struct{}, len(step.DependsOn))
		for _, dependency := range step.DependsOn {
			if dependency == "" || dependency == step.ID {
				return nil, fmt.Errorf("step %q has an invalid dependency", step.ID)
			}
			if _, ok := seen[dependency]; !ok {
				return nil, fmt.Errorf("step %q depends on unknown step %q", step.ID, dependency)
			}
			if _, ok := dependencies[dependency]; ok {
				return nil, fmt.Errorf("step %q has duplicate dependency %q", step.ID, dependency)
			}
			dependencies[dependency] = struct{}{}
		}
	}
	if hasCycle(steps) {
		return nil, errors.New("workflow dependencies contain a cycle")
	}
	return &Workflow{steps: append([]Step(nil), steps...), options: options}, nil
}

// Run executes dependency-ready steps concurrently. Prompts may reference an
// upstream result as {{steps.<id>.output}}.
func (w *Workflow) Run(ctx context.Context) ([]StepResult, error) {
	remaining := make(map[string]Step, len(w.steps))
	for _, step := range w.steps {
		remaining[step.ID] = step
	}
	results := make(map[string]StepResult, len(w.steps))
	for len(remaining) > 0 {
		ready := make([]Step, 0)
		for id, step := range remaining {
			if dependenciesSucceeded(step, results) {
				ready = append(ready, step)
				delete(remaining, id)
			}
		}
		if len(ready) == 0 {
			break
		}
		sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
		completed := make(chan StepResult, len(ready))
		for _, step := range ready {
			step := step
			go func() {
				prompt, err := renderPrompt(step.Prompt, results)
				if err != nil {
					completed <- StepResult{ID: step.ID, Status: Failed, Error: err.Error()}
					return
				}
				completed <- w.runStep(ctx, Step{ID: step.ID, Agent: step.Agent, Prompt: prompt})
			}()
		}
		for range ready {
			result := <-completed
			results[result.ID] = result
			if w.options.OnStepState != nil {
				w.options.OnStepState(result)
			}
		}
		for id, step := range remaining {
			if hasFailedDependency(step, results) {
				results[id] = StepResult{ID: id, Status: Skipped, Error: "dependency failed"}
				delete(remaining, id)
			}
		}
	}
	ordered := make([]StepResult, 0, len(results))
	var workflowErr *WorkflowError
	for _, step := range w.steps {
		result, ok := results[step.ID]
		if !ok {
			result = StepResult{ID: step.ID, Status: Skipped, Error: "workflow did not run"}
		}
		ordered = append(ordered, result)
		if result.Status == Failed && workflowErr == nil {
			workflowErr = &WorkflowError{StepID: step.ID, Err: errors.New(result.Error)}
		}
	}
	if workflowErr != nil {
		return ordered, workflowErr
	}
	return ordered, nil
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

func dependenciesSucceeded(step Step, results map[string]StepResult) bool {
	for _, id := range step.DependsOn {
		if result, ok := results[id]; !ok || result.Status != Succeeded {
			return false
		}
	}
	return true
}
func hasFailedDependency(step Step, results map[string]StepResult) bool {
	for _, id := range step.DependsOn {
		if result, ok := results[id]; ok && result.Status != Succeeded {
			return true
		}
	}
	return false
}
func hasCycle(steps []Step) bool {
	dependencies := make(map[string]int, len(steps))
	children := make(map[string][]string, len(steps))
	for _, step := range steps {
		dependencies[step.ID] = len(step.DependsOn)
		for _, dep := range step.DependsOn {
			children[dep] = append(children[dep], step.ID)
		}
	}
	ready := make([]string, 0)
	for id, count := range dependencies {
		if count == 0 {
			ready = append(ready, id)
		}
	}
	visited := 0
	for len(ready) > 0 {
		id := ready[0]
		ready = ready[1:]
		visited++
		for _, child := range children[id] {
			dependencies[child]--
			if dependencies[child] == 0 {
				ready = append(ready, child)
			}
		}
	}
	return visited != len(steps)
}

var outputReference = regexp.MustCompile(`\{\{steps\.([A-Za-z0-9_-]+)\.output\}\}`)

func renderPrompt(prompt string, results map[string]StepResult) (string, error) {
	var renderErr error
	rendered := outputReference.ReplaceAllStringFunc(prompt, func(match string) string {
		id := outputReference.FindStringSubmatch(match)[1]
		result, ok := results[id]
		if !ok || result.Status != Succeeded {
			renderErr = fmt.Errorf("output for step %q is unavailable", id)
			return match
		}
		return result.Output
	})
	if renderErr != nil {
		return "", renderErr
	}
	return strings.TrimSpace(rendered), nil
}

type WorkflowError struct {
	StepID string
	Err    error
}

func (e *WorkflowError) Error() string {
	return fmt.Sprintf("workflow failed at step %q: %v", e.StepID, e.Err)
}
func (e *WorkflowError) Unwrap() error { return e.Err }
