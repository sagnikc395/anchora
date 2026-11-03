package tests

import (
	"context"
	"errors"
	"sync"

	"github.com/sagnikc395/anchora/lib/workflow"
)

// countingAgent is a workflow.Agent stub that records every call, so tests can
// assert that a resumed job does not re-run its completed steps.
type countingAgent struct {
	mu     sync.Mutex
	calls  map[string]int
	fail   map[string]bool
	prefix string
}

func newCountingAgent() *countingAgent {
	return &countingAgent{calls: map[string]int{}, fail: map[string]bool{}, prefix: "out:"}
}

func (a *countingAgent) Run(ctx context.Context, prompt string) (string, error) {
	a.mu.Lock()
	a.calls[prompt]++
	fail := a.fail[prompt]
	a.mu.Unlock()
	if fail {
		return "", errors.New("agent failed")
	}
	return a.prefix + prompt, nil
}

// count reports how many times the agent was called with the given prompt.
func (a *countingAgent) count(prompt string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[prompt]
}

// registry is the AgentResolver used by integration tests, mapping the step
// names defined in the test workflow to their agent stubs.
type registry map[string]workflow.Agent

func (r registry) Resolve(name string) (workflow.Agent, bool) {
	agent, ok := r[name]
	return agent, ok
}
