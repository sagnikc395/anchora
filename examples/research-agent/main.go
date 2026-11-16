// Command research-agent is the plan's §11 demo on anchora: a research agent
// whose every model call and tool call is a durable step. Kill the process at
// any point mid-run and restart it: completed steps replay from Postgres
// without repeating paid calls, the human-approval gate survives restarts,
// and a fork can rerun the agent from any step without paying for the steps
// before it.
//
// Any OpenAI-compatible endpoint works. With Ollama:
//
//	docker compose up -d --wait   # starts Postgres
//	OLLAMA_BASE_URL=http://localhost:11434/v1 OLLAMA_MODEL=llama3.2 \
//	  go run ./examples/research-agent
//
// With OpenAI:
//
//	OPENAI_API_KEY=sk-... OPENAI_MODEL=gpt-4o-mini go run ./examples/research-agent
//
// The run publishes progress events; watch them with the CLI:
//
//	go run ./cmd/anchora list
//	go run ./cmd/anchora steps <workflow-id>
//
// Approve the final report from a second terminal with -approve.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sagnikc395/anchora/anchora"
	"github.com/sagnikc395/anchora/anchora/agent"
)

type plan struct {
	Queries []string `json:"queries"`
}

type note struct {
	Query  string `json:"query"`
	Answer string `json:"answer"`
}

// fakeProvider stands in for a real model when no endpoint is configured, so
// the demo runs offline while exercising exactly the same durable steps.
type fakeProvider struct{ turns int }

func (f *fakeProvider) Name() string { return "fake" }

func (f *fakeProvider) Complete(ctx context.Context, req agent.Request) (agent.Response, error) {
	f.turns++
	var userText string
	for _, m := range req.Messages {
		if m.Role == agent.RoleUser {
			userText = m.Content
		}
	}
	if len(req.Tools) > 0 && f.turns == 1 {
		args, _ := json.Marshal(map[string]any{"query": userText})
		return agent.Response{
			ToolCalls: []agent.ToolCall{{ID: "call-1", Name: "search", Input: string(args)}},
			Usage:     agent.Usage{InputTokens: 40, OutputTokens: 10},
		}, nil
	}
	text := fmt.Sprintf("Report on %q: evidence gathered by %d recorded step(s); drafting complete.", userText, len(req.Messages))
	return agent.Response{Text: text, Usage: agent.Usage{InputTokens: 60, OutputTokens: 30}}, nil
}

func main() {
	approve := flag.Bool("approve", false, "send the approval message instead of running the agent")
	flag.Parse()

	ctx := context.Background()
	engine, err := anchora.New(ctx, anchora.Config{DatabaseURL: os.Getenv("DATABASE_URL"), Logf: log.Printf})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = engine.Shutdown(shutdown)
	}()

	provider := pickProvider()
	tools := agent.NewRegistry()
	tools.Register(agent.Tool{
		Name:        "search",
		Description: "Search the (simulated) web for a query and return notes.",
		Schema:      json.RawMessage(`{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}`),
		Fn: func(_ context.Context, input string) (any, error) {
			var args struct{ Query string }
			if err := json.Unmarshal([]byte(input), &args); err != nil {
				return nil, err
			}
			return []note{{Query: args.Query, Answer: "simulated findings for " + args.Query}}, nil
		},
	})

	var research = anchora.RegisterWorkflow(engine, "research_agent", func(ctx *anchora.Context, task string) (map[string]any, error) {
		// Plan the queries. One recorded call; a crash never replans.
		planOut, err := agent.Call(ctx, "plan", agent.CallConfig{
			Provider:         provider,
			CostPerMTokenUSD: 0.6,
		}, agent.Request{
			Model:    model(),
			System:   "You are a research planner. Reply with a single JSON object {\"queries\": [string,...]} with at most 3 queries.",
			Messages: []agent.Message{{Role: agent.RoleUser, Content: task}},
		})
		if err != nil {
			return nil, err
		}
		var p plan
		if err := json.Unmarshal([]byte(planOut.Text), &p); err != nil || len(p.Queries) == 0 {
			p = plan{Queries: []string{task}}
		}

		// Search the queries in parallel. Each parallel leg is its own
		// checkpointed slot, so a crash resumes exactly where it stopped.
		type leg struct{ Note note }
		legs := make([]*anchora.Future[leg], 0, len(p.Queries))
		for i, q := range p.Queries {
			legs = append(legs, anchora.Go(ctx, fmt.Sprintf("search-%d", i), func(sc anchora.StepCtx) (leg, error) {
				args, _ := json.Marshal(map[string]any{"query": q})
				tool, _ := tools.Get("search")
				out, err := tool.Fn(sc, string(args))
				if err != nil {
					return leg{}, err
				}
				notes, ok := out.([]note)
				if !ok || len(notes) == 0 {
					return leg{}, fmt.Errorf("search returned nothing")
				}
				return leg{Note: notes[0]}, nil
			}))
		}
		notes := make([]note, 0, len(legs))
		for i, f := range legs {
			result, err := f.Wait(context.Background())
			if err != nil {
				return nil, fmt.Errorf("search %d: %w", i, err)
			}
			notes = append(notes, result.Note)
		}

		// Refine: one durable agent loop; each turn is a recorded step.
		messages, err := agent.Run(ctx, "refine", agent.RunConfig{
			Model:    model(),
			System:   "You are a research assistant. Use the search tool when the user asks you to verify something, otherwise answer.",
			Tools:    tools,
			Budget:   &agent.Budget{MaxTotalTokens: 20000, CostPerMTokenUSD: 0.6, MaxCostUSD: 0.01},
			Call:     agent.CallConfig{Provider: provider},
			EventKey: "draft",
		}, []agent.Message{
			{Role: agent.RoleUser, Content: fmt.Sprintf("Research %q. Notes so far: %+v", task, notes)},
		})
		if err != nil {
			return nil, err
		}
		draft := agent.FinalText(messages)

		// Human-in-the-loop gate: survives restarts; the approval arrives
		// from `go run ./examples/research-agent -approve`.
		approved, err := anchora.Recv[bool](ctx, "approval", 24*time.Hour)
		if err != nil {
			return nil, err
		}
		if !approved {
			return nil, fmt.Errorf("research %q rejected", task)
		}
		published, err := anchora.Step(ctx, "publish", func(sc anchora.StepCtx) (map[string]any, error) {
			return map[string]any{"task": task, "report": draft, "notes": notes}, nil
		})
		if err != nil {
			return nil, err
		}
		if err := anchora.SetEvent(ctx, "report", published); err != nil {
			return nil, err
		}
		return published, nil
	})

	approver := anchora.RegisterWorkflow(engine, "approve_research", func(ctx *anchora.Context, id string) (string, error) {
		if err := anchora.Send(ctx, id, "approval", true); err != nil {
			return "", err
		}
		return "approved " + id, nil
	})

	if *approve {
		if len(flag.Args()) != 1 {
			log.Fatal("usage: research-agent -approve <workflow-id>")
		}
		h, err := approver.Start(ctx, flag.Arg(0))
		if err != nil {
			log.Fatal(err)
		}
		result, err := h.Result(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
		return
	}

	task := "durable execution engines for AI agents"
	h, err := research.Start(ctx, task, anchora.WithWorkflowID("research-demo"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("research agent %s is running; approve it with -approve research-demo\n", h.ID())
	result, err := h.Result(ctx)
	if err != nil {
		log.Fatal(err)
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
}

// pickProvider chooses the provider from the environment, defaulting to the
// offline fake so the example always runs.
func pickProvider() agent.Provider {
	if key := os.Getenv("OPENAI_API_KEY"); key != "" {
		return agent.OpenAI{APIKey: key, BaseURL: os.Getenv("OPENAI_BASE_URL")}
	}
	if base := os.Getenv("OLLAMA_BASE_URL"); base != "" {
		return agent.OpenAI{BaseURL: base}
	}
	if key := os.Getenv("ANTHROPIC_API_KEY"); key != "" {
		return agent.Anthropic{APIKey: key}
	}
	return &fakeProvider{}
}

func model() string {
	if m := os.Getenv("OPENAI_MODEL"); m != "" {
		return m
	}
	if m := os.Getenv("OLLAMA_MODEL"); m != "" {
		return m
	}
	if os.Getenv("ANTHROPIC_API_KEY") != "" {
		if m := os.Getenv("ANTHROPIC_MODEL"); m != "" {
			return m
		}
		return "claude-sonnet-4-5"
	}
	return "fake-model"
}
