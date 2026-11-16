package agent

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sagnikc395/anchora/pgflow"
)

// CallConfig configures one recorded model call.
type CallConfig struct {
	// Provider performs the request. Every call is a pgflow step, so a replay
	// returns the recorded response without paying for it twice.
	Provider Provider
	// CostPerMTokenUSD prices one million output tokens, for budgets. Zero
	// disables cost accounting.
	CostPerMTokenUSD float64
}

// Call runs one chat completion as a step named name. The response — text,
// tool calls, and token usage — is checkpointed, so recovering a workflow
// never repeats a paid call.
func Call(c *pgflow.Context, name string, cfg CallConfig, req Request) (Response, error) {
	if cfg.Provider == nil {
		return Response{}, fmt.Errorf("agent: call %q has no provider", name)
	}
	if req.Model == "" {
		return Response{}, fmt.Errorf("agent: call %q has no model", name)
	}
	return pgflow.Step(c, name, func(sc pgflow.StepCtx) (Response, error) {
		return cfg.Provider.Complete(sc, req)
	})
}

// Budget caps one agent run. It counts what recorded calls report, so it is
// deterministic under replay: the same history always crosses the same
// limits.
type Budget struct {
	// MaxTotalTokens caps input+output tokens across the run.
	MaxTotalTokens int
	// MaxCostUSD caps cost at CostPerMTokenUSD.
	MaxCostUSD float64
	// CostPerMTokenUSD prices one million output tokens.
	CostPerMTokenUSD float64

	InputTokens  int
	OutputTokens int
}

// Add records one response's usage and reports whether a limit was exceeded.
func (b *Budget) Add(u Usage) (exceeded bool, reason string) {
	if b == nil {
		return false, ""
	}
	b.InputTokens += u.InputTokens
	b.OutputTokens += u.OutputTokens
	total := b.InputTokens + b.OutputTokens
	if b.MaxTotalTokens > 0 && total > b.MaxTotalTokens {
		return true, fmt.Sprintf("token budget %d exceeded (%d)", b.MaxTotalTokens, total)
	}
	cost := float64(b.OutputTokens) / 1_000_000 * b.CostPerMTokenUSD
	if b.MaxCostUSD > 0 && cost > b.MaxCostUSD {
		return true, fmt.Sprintf("cost budget $%.4f exceeded ($%.4f)", b.MaxCostUSD, cost)
	}
	return false, ""
}

// RunConfig describes one durable agent run: a model plus a tool loop in
// which every model call and every tool invocation is a checkpointed step.
type RunConfig struct {
	// Call configures the provider.
	Call CallConfig
	// Model is the model id passed to the provider.
	Model string
	// System is the system prompt.
	System string
	// Tools may be nil for a plain chat run.
	Tools *Registry
	// MaxTurns caps model round trips; zero means 8.
	MaxTurns int
	// Temperature passes through to the provider.
	Temperature float64
	// MaxTokens per model call; the provider default applies when zero.
	MaxTokens int
	// Budget is checked after every model call. Run fails deterministically
	// when a limit is exceeded.
	Budget *Budget
	// EventKey, when set, publishes the latest assistant text as a
	// pgflow event under this key for external observers.
	EventKey string
}

// Run drives the replays-safe agent loop:
//
//	for turn := 0; turn < maxTurns; turn++ {
//	    response := Call(...)
//	    if no tool calls: return response
//	    for each tool call: Step(tool)
//	}
//
// Calls are named name:turn<N> and tool calls name:turn<N>:tool<index>, so a
// recovered run resumes at the exact call it died in.
func Run(c *pgflow.Context, name string, cfg RunConfig, messages []Message) ([]Message, error) {
	maxTurns := cfg.MaxTurns
	if maxTurns <= 0 {
		maxTurns = 8
	}
	turn := 0
	for ; turn < maxTurns; turn++ {
		req := Request{
			Model:       cfg.Model,
			System:      cfg.System,
			Messages:    messages,
			Tools:       cfg.Tools.Defs(),
			MaxTokens:   cfg.MaxTokens,
			Temperature: cfg.Temperature,
		}
		resp, err := Call(c, fmt.Sprintf("%s:turn%d", name, turn), cfg.Call, req)
		if err != nil {
			return messages, err
		}
		if cfg.EventKey != "" && resp.Text != "" {
			if err := pgflow.SetEvent(c, cfg.EventKey, resp.Text); err != nil {
				return messages, err
			}
		}
		if exceeded, reason := cfg.Budget.Add(resp.Usage); exceeded {
			return messages, &BudgetExceededError{Reason: reason}
		}
		messages = append(messages, Message{Role: RoleAssistant, Content: resp.Text, ToolCalls: resp.ToolCalls})
		if len(resp.ToolCalls) == 0 {
			break
		}
		for i, call := range resp.ToolCalls {
			tool, ok := cfg.Tools.Get(call.Name)
			if !ok {
				return messages, fmt.Errorf("agent: model called unknown tool %q", call.Name)
			}
			stepName := fmt.Sprintf("%s:turn%d:tool%d", name, turn, i)
			result, err := pgflow.Step(c, stepName, func(sc pgflow.StepCtx) (string, error) {
				out, err := tool.Fn(sc, call.Input)
				if err != nil {
					return "", err
				}
				encoded, err := json.Marshal(out)
				if err != nil {
					return "", fmt.Errorf("agent: encode tool %q output: %w", call.Name, err)
				}
				return string(encoded), nil
			})
			if err != nil {
				result = toolError(err) // feed the failure back as the tool result
			}
			messages = append(messages, Message{Role: RoleTool, Content: result, ToolCallID: call.ID})
		}
	}
	if turn == maxTurns {
		// The loop ran out while the model kept asking for tools: every turn
		// ended in tool calls and never produced a plain answer.
		return messages, fmt.Errorf("agent: %s did not finish within %d turns: %w", name, maxTurns, ErrMaxTurnsExceeded)
	}
	return messages, nil
}

// FinalText returns the last assistant message of a run.
func FinalText(messages []Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == RoleAssistant && strings.TrimSpace(messages[i].Content) != "" {
			return messages[i].Content
		}
	}
	return ""
}
