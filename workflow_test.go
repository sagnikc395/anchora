package anchora_test

import (
	"context"
	"errors"
	"github.com/sagnikc395/anchora"
	"testing"
	"time"
)

type fakeAgent struct{ calls, failures int }

func (a *fakeAgent) Run(_ context.Context, prompt string) (string, error) {
	a.calls++
	if a.calls <= a.failures {
		return "", errors.New("temporary failure")
	}
	return "done: " + prompt, nil
}
func TestWorkflowRetriesAndSucceeds(t *testing.T) {
	agent := &fakeAgent{failures: 2}
	w, err := anchora.NewWorkflow([]anchora.Step{{ID: "one", Agent: agent, Prompt: "work"}}, anchora.Options{MaxRetries: 2, RetryDelay: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	results, err := w.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if agent.calls != 3 || results[0].Attempts != 2 || results[0].Status != anchora.Succeeded {
		t.Fatalf("unexpected result: %#v, calls=%d", results[0], agent.calls)
	}
}
