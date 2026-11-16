package agent_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/pgflow"
	"github.com/sagnikc395/anchora/pgflow/agent"
	"github.com/sagnikc395/anchora/pgflow/memory"
	"github.com/sagnikc395/anchora/pgflow/pgflowtest"
)

// fakeProvider scripts responses and records the requests it saw. Once its
// script runs out it repeats the last response, so always-asking models trip
// MaxTurns instead of failing on a missing script.
type fakeProvider struct {
	mu    sync.Mutex
	resps []agent.Response
	reqs  []agent.Request
	calls atomic.Int32
}

func (f *fakeProvider) Complete(ctx context.Context, req agent.Request) (agent.Response, error) {
	i := int(f.calls.Add(1)) - 1
	f.mu.Lock()
	if i >= len(f.resps) {
		i = len(f.resps) - 1
	}
	resp := f.resps[i]
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	return resp, nil
}

func (f *fakeProvider) Name() string { return "fake" }

func (f *fakeProvider) requestCount() int { return int(f.calls.Load()) }

func (f *fakeProvider) sawTools() []agent.ToolDef {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reqs) == 0 {
		return nil
	}
	return f.reqs[0].Tools
}

func newTestEngine(t *testing.T) (*pgflow.Engine, *memory.Store) {
	t.Helper()
	store := memory.New()
	eng, err := pgflow.New(context.Background(), pgflow.Config{
		Store:      store,
		AppVersion: "test",
		ExecutorID: "exec-a",
		Logf:       t.Logf,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := eng.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return eng, store
}

func TestRunExecutesToolLoop(t *testing.T) {
	eng, store := newTestEngine(t)
	provider := &fakeProvider{resps: []agent.Response{
		{ToolCalls: []agent.ToolCall{{ID: "t1", Name: "search", Input: `{"q":"go"}`}}},
		{Text: "The answer"},
	}}
	registry := agent.NewRegistry()
	var toolInputs []string
	registry.Register(agent.Tool{
		Name: "search",
		Fn: func(ctx context.Context, input string) (any, error) {
			toolInputs = append(toolInputs, input)
			return map[string]int{"hits": 3}, nil
		},
	})

	run := pgflow.RegisterWorkflow(eng, "looped", func(ctx *pgflow.Context, task string) (string, error) {
		messages := []agent.Message{{Role: agent.RoleUser, Content: task}}
		out, err := agent.Run(ctx, "agent", agent.RunConfig{
			Call:     agent.CallConfig{Provider: provider},
			Model:    "fake-1",
			System:   "be brief",
			Tools:    registry,
			MaxTurns: 4,
		}, messages)
		if err != nil {
			return "", err
		}
		return agent.FinalText(out), nil
	})

	h, err := run.Start(context.Background(), "question")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	out, err := h.Result(deadline)
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	if out != "The answer" {
		t.Fatalf("final text = %q", out)
	}
	if got := provider.requestCount(); got != 2 {
		t.Fatalf("model calls = %d, want 2", got)
	}
	if len(toolInputs) != 1 || toolInputs[0] != `{"q":"go"}` {
		t.Fatalf("tool inputs = %v", toolInputs)
	}
	if defs := provider.sawTools(); len(defs) != 1 || defs[0].Name != "search" {
		t.Fatalf("tool defs = %+v", defs)
	}
	// Every call is a named, checkpointed step so a crash resumes mid-loop.
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	want := map[string]bool{"agent:turn0": false, "agent:turn0:tool0": false, "agent:turn1": false}
	for _, cp := range history {
		if _, ok := want[cp.FunctionName]; ok {
			want[cp.FunctionName] = true
		}
	}
	for name, recorded := range want {
		if !recorded {
			t.Fatalf("step %s was not checkpointed; history = %+v", name, history)
		}
	}
}

func TestRunReplaysRecordedCallsForFree(t *testing.T) {
	eng, store := newTestEngine(t)
	provider := &fakeProvider{resps: []agent.Response{
		{Text: "recorded answer", Usage: agent.Usage{InputTokens: 10, OutputTokens: 5}},
	}}

	run := pgflow.RegisterWorkflow(eng, "replayable", func(ctx *pgflow.Context, task string) (string, error) {
		messages := []agent.Message{{Role: agent.RoleUser, Content: task}}
		out, err := agent.Run(ctx, "agent", agent.RunConfig{
			Call:  agent.CallConfig{Provider: provider},
			Model: "fake-1",
		}, messages)
		if err != nil {
			return "", err
		}
		return agent.FinalText(out), nil
	})
	h, err := run.Start(context.Background(), "task")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := h.Result(deadline); err != nil || out != "recorded answer" {
		t.Fatalf("first run = %q, %v", out, err)
	}
	history, err := pgflowtest.History(context.Background(), store, h.ID())
	if err != nil {
		t.Fatal(err)
	}

	// A replay with a provider that refuses to serve: the recorded responses
	// must satisfy the whole loop without a single live model call.
	dead := providerStub{complete: func(ctx context.Context, req agent.Request) (agent.Response, error) {
		return agent.Response{}, errors.New("must not be called")
	}}
	out, err := pgflowtest.Replay(t, func(ctx *pgflow.Context, task string) (string, error) {
		messages := []agent.Message{{Role: agent.RoleUser, Content: task}}
		cfg := agent.RunConfig{Model: "fake-1"}
		cfg.Call = agent.CallConfig{Provider: dead}
		out, err := agent.Run(ctx, "agent", cfg, messages)
		if err != nil {
			return "", err
		}
		return agent.FinalText(out), nil
	}, "replayable", history, "task")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if out != "recorded answer" {
		t.Fatalf("replayed output = %q, want the recorded answer", out)
	}
}

// providerStub adapts a bare function to the Provider interface.
type providerStub struct {
	complete func(ctx context.Context, req agent.Request) (agent.Response, error)
}

func (p providerStub) Complete(ctx context.Context, req agent.Request) (agent.Response, error) {
	return p.complete(ctx, req)
}

func (p providerStub) Name() string { return "stub" }

func TestBudgetExceededFailsDeterministically(t *testing.T) {
	eng, _ := newTestEngine(t)
	provider := &fakeProvider{resps: []agent.Response{
		{Text: "expensive", Usage: agent.Usage{InputTokens: 100, OutputTokens: 10}},
	}}

	run := pgflow.RegisterWorkflow(eng, "spender", func(ctx *pgflow.Context, task string) (string, error) {
		budget := &agent.Budget{MaxTotalTokens: 10}
		messages := []agent.Message{{Role: agent.RoleUser, Content: task}}
		cfg := agent.RunConfig{Model: "fake-1", Budget: budget}
		cfg.Call = agent.CallConfig{Provider: provider}
		_, err := agent.Run(ctx, "agent", cfg, messages)
		return "", err
	})
	h, err := run.Start(context.Background(), "task")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = h.Result(deadline)
	// Terminal errors rehydrate as messages; match on the text.
	if err == nil || !strings.Contains(err.Error(), "budget exceeded") || !strings.Contains(err.Error(), "token budget 10") {
		t.Fatalf("error = %v, want the token budget message", err)
	}
}

func TestMaxTurnsExceeded(t *testing.T) {
	eng, _ := newTestEngine(t)
	// Always asks for the tool; the repeats keep the loop fed until MaxTurns.
	provider := &fakeProvider{resps: []agent.Response{
		{ToolCalls: []agent.ToolCall{{ID: "t1", Name: "search", Input: `{}`}}},
	}}
	registry := agent.NewRegistry()
	registry.Register(agent.Tool{
		Name: "search",
		Fn: func(ctx context.Context, input string) (any, error) {
			return "nothing", nil
		},
	})

	run := pgflow.RegisterWorkflow(eng, "loopy", func(ctx *pgflow.Context, task string) (string, error) {
		messages := []agent.Message{{Role: agent.RoleUser, Content: task}}
		cfg := agent.RunConfig{Model: "fake-1", Tools: registry, MaxTurns: 2}
		cfg.Call = agent.CallConfig{Provider: provider}
		_, err := agent.Run(ctx, "agent", cfg, messages)
		return "", err
	})
	h, err := run.Start(context.Background(), "task")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = h.Result(deadline)
	if err == nil || !strings.Contains(err.Error(), "did not finish within 2 turns") {
		t.Fatalf("error = %v, want the turn-limit message", err)
	}
}

func TestCallRejectsMissingProviderOrModel(t *testing.T) {
	eng, _ := newTestEngine(t)
	run := pgflow.RegisterWorkflow(eng, "guarded", func(ctx *pgflow.Context, in string) (string, error) {
		if _, err := agent.Call(ctx, "x", agent.CallConfig{}, agent.Request{Model: "m"}); err == nil {
			t.Error("nil provider accepted")
		}
		if _, err := agent.Call(ctx, "x", agent.CallConfig{Provider: providerStub{
			complete: func(context.Context, agent.Request) (agent.Response, error) { return agent.Response{}, nil },
		}}, agent.Request{}); err == nil {
			t.Error("empty model accepted")
		}
		return "ok", nil
	})
	h, err := run.Start(context.Background(), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if out, err := h.Result(deadline); err != nil || out != "ok" {
		t.Fatalf("result = %q, %v", out, err)
	}
}
