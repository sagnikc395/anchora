package workflow_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/internal/workflow"
)

// fakeAgent fails its first `failures` calls and then succeeds, which is how
// the retry path is exercised without waiting on anything real.
type fakeAgent struct{ calls, failures int }

func (a *fakeAgent) Run(_ context.Context, prompt string) (string, error) {
	a.calls++
	if a.calls <= a.failures {
		return "", errors.New("temporary failure")
	}
	return "done: " + prompt, nil
}

type echoAgent struct{}

func (echoAgent) Run(_ context.Context, prompt string) (string, error) { return prompt, nil }

type failingAgent struct{}

func (failingAgent) Run(context.Context, string) (string, error) {
	return "", errors.New("permanent failure")
}

func TestWorkflowRunsDAGAndPassesOutputs(t *testing.T) {
	w, err := workflow.NewWorkflow([]workflow.Step{
		{ID: "research", Agent: echoAgent{}, Prompt: "facts"},
		{ID: "summary", Agent: echoAgent{}, Prompt: "summarize {{steps.research.output}}", DependsOn: []string{"research"}},
	}, workflow.Options{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if results[1].Output != "summarize facts" {
		t.Fatalf("unexpected results: %#v", results)
	}
}

func TestWorkflowRetriesAndSucceeds(t *testing.T) {
	agent := &fakeAgent{failures: 2}
	w, err := workflow.NewWorkflow([]workflow.Step{{ID: "one", Agent: agent, Prompt: "work"}},
		workflow.Options{MaxRetries: 2, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if agent.calls != 3 || results[0].Attempts != 2 || results[0].Status != workflow.Succeeded {
		t.Fatalf("unexpected result: %#v, calls=%d", results[0], agent.calls)
	}
}

func TestWorkflowReportsFailingStep(t *testing.T) {
	w, err := workflow.NewWorkflow([]workflow.Step{{ID: "one", Agent: failingAgent{}, Prompt: "work"}}, workflow.Options{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	var workflowErr *workflow.WorkflowError
	if !errors.As(err, &workflowErr) {
		t.Fatalf("error = %v, want a *WorkflowError", err)
	}
	if workflowErr.StepID != "one" {
		t.Fatalf("StepID = %q, want %q", workflowErr.StepID, "one")
	}
	if results[0].Status != workflow.Failed {
		t.Fatalf("status = %q, want %q", results[0].Status, workflow.Failed)
	}
}

// A failure must skip the whole downstream chain, not just the failed step's
// immediate children.
func TestWorkflowSkipsTransitiveDependents(t *testing.T) {
	w, err := workflow.NewWorkflow([]workflow.Step{
		{ID: "a", Agent: failingAgent{}, Prompt: "a"},
		{ID: "b", Agent: echoAgent{}, Prompt: "b", DependsOn: []string{"a"}},
		{ID: "c", Agent: echoAgent{}, Prompt: "c", DependsOn: []string{"b"}},
	}, workflow.Options{})
	if err != nil {
		t.Fatal(err)
	}
	results, _ := w.Run(context.Background())
	if results[0].Status != workflow.Failed {
		t.Fatalf("a: status = %q, want %q", results[0].Status, workflow.Failed)
	}
	for _, result := range results[1:] {
		if result.Status != workflow.Skipped || result.Error != "dependency failed" {
			t.Fatalf("%s: got %q/%q, want skipped/dependency failed", result.ID, result.Status, result.Error)
		}
	}
}

// A wave of concurrent steps all interpolate the same upstream output while
// their siblings' results are being collected. Run this under -race.
func TestWorkflowRendersConcurrentWaveWithoutRacing(t *testing.T) {
	const width = 8
	var started atomic.Int32
	// The first step in the wave returns immediately and the rest linger, so a
	// result is recorded while later steps are still reading earlier outputs.
	agent := agentFunc(func(_ context.Context, prompt string) (string, error) {
		if started.Add(1) > 1 {
			time.Sleep(2 * time.Millisecond)
		}
		return prompt, nil
	})
	steps := []workflow.Step{{ID: "seed", Agent: echoAgent{}, Prompt: "S"}}
	for i := range width {
		steps = append(steps, workflow.Step{
			ID:        string(rune('a' + i)),
			Agent:     agent,
			Prompt:    "use {{steps.seed.output}}",
			DependsOn: []string{"seed"},
		})
	}
	w, err := workflow.NewWorkflow(steps, workflow.Options{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, result := range results[1:] {
		if result.Output != "use S" {
			t.Fatalf("%s: output = %q, want %q", result.ID, result.Output, "use S")
		}
	}
}

func TestWorkflowResumeSkipsCompletedSteps(t *testing.T) {
	agent := &fakeAgent{}
	w, err := workflow.NewWorkflow([]workflow.Step{
		{ID: "research", Agent: agent, Prompt: "facts"},
		{ID: "summary", Agent: agent, Prompt: "summarize {{steps.research.output}}", DependsOn: []string{"research"}},
	}, workflow.Options{Resume: []workflow.StepResult{
		{ID: "research", Status: workflow.Succeeded, Output: "cached facts"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if agent.calls != 1 {
		t.Fatalf("agent calls = %d, want 1 (the resumed step must not re-run)", agent.calls)
	}
	if results[0].Output != "cached facts" {
		t.Fatalf("resumed result was not replayed: %#v", results[0])
	}
	if results[1].Output != "done: summarize cached facts" {
		t.Fatalf("downstream step did not see the resumed output: %#v", results[1])
	}
}

func TestWorkflowResumeIgnoresUnfinishedResults(t *testing.T) {
	agent := &fakeAgent{}
	w, err := workflow.NewWorkflow([]workflow.Step{{ID: "one", Agent: agent, Prompt: "work"}},
		workflow.Options{Resume: []workflow.StepResult{{ID: "one", Status: workflow.Failed, Error: "boom"}}})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if agent.calls != 1 || results[0].Status != workflow.Succeeded {
		t.Fatalf("a failed prior result must be retried, got calls=%d result=%#v", agent.calls, results[0])
	}
}

func TestNewWorkflowRejectsInvalidGraphs(t *testing.T) {
	agent := echoAgent{}
	tests := map[string][]workflow.Step{
		"no steps": nil,
		"missing id": {
			{Agent: agent, Prompt: "p"},
		},
		"missing prompt": {
			{ID: "a", Agent: agent},
		},
		"missing agent": {
			{ID: "a", Prompt: "p"},
		},
		"duplicate id": {
			{ID: "a", Agent: agent, Prompt: "p"},
			{ID: "a", Agent: agent, Prompt: "p"},
		},
		"self dependency": {
			{ID: "a", Agent: agent, Prompt: "p", DependsOn: []string{"a"}},
		},
		"unknown dependency": {
			{ID: "a", Agent: agent, Prompt: "p", DependsOn: []string{"ghost"}},
		},
		"duplicate dependency": {
			{ID: "a", Agent: agent, Prompt: "p"},
			{ID: "b", Agent: agent, Prompt: "p", DependsOn: []string{"a", "a"}},
		},
		"cycle": {
			{ID: "a", Agent: agent, Prompt: "a", DependsOn: []string{"b"}},
			{ID: "b", Agent: agent, Prompt: "b", DependsOn: []string{"a"}},
		},
	}
	for name, steps := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := workflow.NewWorkflow(steps, workflow.Options{}); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestNewWorkflowRejectsNegativeRetrySettings(t *testing.T) {
	steps := []workflow.Step{{ID: "a", Agent: echoAgent{}, Prompt: "p"}}
	if _, err := workflow.NewWorkflow(steps, workflow.Options{MaxRetries: -1}); err == nil {
		t.Fatal("expected an error for a negative MaxRetries")
	}
	if _, err := workflow.NewWorkflow(steps, workflow.Options{RetryDelay: -time.Second}); err == nil {
		t.Fatal("expected an error for a negative RetryDelay")
	}
}

// agentFunc adapts a function to workflow.Agent.
type agentFunc func(context.Context, string) (string, error)

func (f agentFunc) Run(ctx context.Context, prompt string) (string, error) { return f(ctx, prompt) }
