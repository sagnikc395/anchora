package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"
)

// Tool is one capability an agent may invoke. Fn receives the tool arguments
// as JSON and returns anything JSON-serializable.
type Tool struct {
	Name        string
	Description string
	// Schema is the JSON Schema of the arguments.
	Schema json.RawMessage
	// Fn performs the tool's side effect.
	Fn func(ctx context.Context, input string) (any, error)
}

// Defs returns the provider-facing description of the tool.
func (t Tool) Def() ToolDef {
	return ToolDef{Name: t.Name, Description: t.Description, Schema: t.Schema}
}

// Registry is the set of tools one agent may call.
type Registry struct {
	tools map[string]Tool
	order []string
}

// NewRegistry returns an empty tool registry.
func NewRegistry() *Registry {
	return &Registry{tools: map[string]Tool{}}
}

// Register adds a tool. Registering the same name twice panics: it is a
// programming error that would make replays ambiguous.
func (r *Registry) Register(t Tool) {
	if t.Name == "" {
		panic("agent: tool name is required")
	}
	if t.Fn == nil {
		panic(fmt.Sprintf("agent: tool %s has no function", t.Name))
	}
	if _, exists := r.tools[t.Name]; exists {
		panic(fmt.Sprintf("agent: tool %s is registered twice", t.Name))
	}
	r.tools[t.Name] = t
	r.order = append(r.order, t.Name)
}

// Get returns the tool with the given name.
func (r *Registry) Get(name string) (Tool, bool) {
	t, ok := r.tools[name]
	return t, ok
}

// Defs returns every tool's provider-facing description.
func (r *Registry) Defs() []ToolDef {
	if r == nil {
		return nil
	}
	defs := make([]ToolDef, 0, len(r.order))
	for _, name := range r.order {
		defs = append(defs, r.tools[name].Def())
	}
	return defs
}

// Sandbox runs a shell command inside a throwaway Docker container: sandboxed
// code execution as a Tool. Requires the docker CLI on the host.
//
// The timeout bounds the container; the surrounding step's StartToClose
// timeout bounds the whole attempt, so keep the step timeout larger.
func Sandbox(image string, timeout time.Duration) Tool {
	return Tool{
		Name:        "run_code",
		Description: fmt.Sprintf("Run a shell command in an isolated docker container (%s) and return its combined output.", image),
		Schema:      json.RawMessage(`{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}`),
		Fn: func(ctx context.Context, input string) (any, error) {
			var args struct{ Command string }
			if err := decodeJSON([]byte(input), &args); err != nil || args.Command == "" {
				return nil, fmt.Errorf("run_code: command is required")
			}
			runCtx := ctx
			var cancel context.CancelFunc
			if timeout > 0 {
				runCtx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}
			cmd := exec.CommandContext(runCtx, "docker", "run", "--rm", "--network=none", image, "sh", "-c", args.Command)
			out, err := cmd.CombinedOutput()
			if runCtx.Err() != nil {
				return map[string]any{"output": string(out), "timed_out": true}, nil
			}
			if err != nil {
				// A nonzero exit is the tool's result, not a step failure.
				return map[string]any{"output": string(out), "exit_error": err.Error()}, nil
			}
			return map[string]any{"output": string(out)}, nil
		},
	}
}
