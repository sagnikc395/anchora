// Package workflow runs ordered, retryable agent workflows.
//
// A workflow is a set of [Step] values whose DependsOn edges form a DAG.
// [NewWorkflow] validates the graph up front; [Workflow.Run] then executes
// every wave of dependency-ready steps concurrently, making each step's output
// available to its downstream steps.
package workflow

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Status is the execution state of a single step.
type Status string

// The states a step can be in. Retrying is transient and only ever observed
// through [Options.OnStepState]; everything a step store records is one of the
// other five.
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

// Step is one agent call. Prompt may reference an upstream step's output as
// {{steps.<id>.output}}, and DependsOn names the steps that must succeed first.
type Step struct {
	ID        string
	Agent     Agent
	Prompt    string
	DependsOn []string
}

// StepResult is the outcome of one step, including the number of retries it
// needed. Output is set only on success; Error only on a failure or a skip.
type StepResult struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Attempts int    `json:"attempts"`
	Output   string `json:"output,omitempty"`
	Error    string `json:"error,omitempty"`
}

// Options tunes retry behaviour and observation of a run.
type Options struct {
	// MaxRetries is how many additional attempts a failing step gets after its
	// first call. Zero means a step is tried exactly once.
	MaxRetries int
	// RetryDelay scales the linear backoff between attempts: retry number
	// multiplied by RetryDelay.
	RetryDelay time.Duration
	// OnStepState is called once per step as it reaches a terminal state. It
	// runs on the goroutine draining the wave, so it should not block for long.
	OnStepState func(StepResult)
	// Resume seeds results for steps that already finished in an earlier
	// attempt. Succeeded steps are not re-executed, which makes redelivery of
	// a partially completed job cheap and side-effect free.
	Resume []StepResult
}

// Workflow is a validated, reusable execution plan. It is safe to Run more
// than once; a run holds no state on the Workflow itself.
type Workflow struct {
	steps   []Step
	options Options
}

// NewWorkflow validates steps and options and returns a runnable workflow. It
// rejects empty workflows, steps missing an ID, agent, or prompt, duplicate
// step IDs, self-dependencies, duplicate or unknown dependencies, dependency
// cycles, and negative retry settings.
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

// Run executes dependency-ready steps concurrently, one wave at a time, and
// returns a result for every step in declaration order. A step whose
// dependencies did not all succeed is skipped rather than run.
//
// The error is a [*WorkflowError] naming the first step that failed; the
// results collected up to that point are still returned alongside it.
func (w *Workflow) Run(ctx context.Context) ([]StepResult, error) {
	remaining := make(map[string]Step, len(w.steps))
	for _, step := range w.steps {
		remaining[step.ID] = step
	}
	results := make(map[string]StepResult, len(w.steps))
	for _, prior := range w.options.Resume {
		if prior.Status != Succeeded {
			continue
		}
		if _, ok := remaining[prior.ID]; !ok {
			continue
		}
		results[prior.ID] = prior
		delete(remaining, prior.ID)
	}
	for len(remaining) > 0 {
		ready := readyWave(remaining, results)
		if len(ready) == 0 {
			break
		}
		// Every prompt in the wave is rendered before any step starts, so no
		// goroutine reads results while the drain loop below writes to it.
		prompts := make([]string, len(ready))
		renderErrs := make([]error, len(ready))
		for i, step := range ready {
			prompts[i], renderErrs[i] = renderPrompt(step.Prompt, results)
		}
		completed := make(chan StepResult, len(ready))
		for i, step := range ready {
			if err := renderErrs[i]; err != nil {
				completed <- StepResult{ID: step.ID, Status: Failed, Error: err.Error()}
				continue
			}
			prompt := prompts[i]
			go func() { completed <- w.runStep(ctx, step.ID, step.Agent, prompt) }()
		}
		for range ready {
			result := <-completed
			results[result.ID] = result
			if w.options.OnStepState != nil {
				w.options.OnStepState(result)
			}
		}
		skipBlocked(remaining, results)
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

// readyWave removes every step whose dependencies have all succeeded from
// remaining and returns them in ID order, so a run is deterministic.
func readyWave(remaining map[string]Step, results map[string]StepResult) []Step {
	var ready []Step
	for id, step := range remaining {
		if dependenciesSucceeded(step, results) {
			ready = append(ready, step)
			delete(remaining, id)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].ID < ready[j].ID })
	return ready
}

// skipBlocked marks every step that can no longer run as skipped. It iterates
// to a fixed point so a failure cascades through a whole dependency chain, not
// just to the failed step's immediate children.
func skipBlocked(remaining map[string]Step, results map[string]StepResult) {
	for {
		blocked := false
		for id, step := range remaining {
			if hasFailedDependency(step, results) {
				results[id] = StepResult{ID: id, Status: Skipped, Error: "dependency failed"}
				delete(remaining, id)
				blocked = true
			}
		}
		if !blocked {
			return
		}
	}
}

// runStep calls an agent, retrying on failure with a linear backoff, and
// reports the terminal outcome. Attempts counts the retries it needed.
func (w *Workflow) runStep(ctx context.Context, id string, agent Agent, prompt string) StepResult {
	result := StepResult{ID: id, Status: Running}
	for {
		output, err := agent.Run(ctx, prompt)
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

// hasCycle reports whether the dependency graph cannot be topologically
// sorted, which for a graph this size is the cheapest cycle test available.
func hasCycle(steps []Step) bool {
	dependencies := make(map[string]int, len(steps))
	children := make(map[string][]string, len(steps))
	for _, step := range steps {
		dependencies[step.ID] = len(step.DependsOn)
		for _, dep := range step.DependsOn {
			children[dep] = append(children[dep], step.ID)
		}
	}
	ready := make([]string, 0, len(steps))
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

// outputReference matches the {{steps.<id>.output}} placeholder that a prompt
// uses to interpolate an upstream step's result.
var outputReference = regexp.MustCompile(`\{\{steps\.([A-Za-z0-9_-]+)\.output\}\}`)

// renderPrompt substitutes every upstream output referenced by prompt. It fails
// rather than rendering a blank when a referenced step has no usable output.
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

// WorkflowError reports which step failed a workflow. It wraps the step's
// error, so callers can match on it with [errors.Is].
type WorkflowError struct {
	StepID string
	Err    error
}

func (e *WorkflowError) Error() string {
	return fmt.Sprintf("workflow failed at step %q: %v", e.StepID, e.Err)
}

func (e *WorkflowError) Unwrap() error { return e.Err }
