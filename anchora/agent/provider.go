// Package agent builds AI-agent workflows on the anchora durable-execution
// engine. Every model call and tool invocation is a recorded step: if the
// process dies mid-loop, the workflow resumes at the exact call without
// repeating paid work.
package agent

import (
	"context"
	"encoding/json"
)

// Role labels a chat message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message is one entry of a conversation.
type Message struct {
	Role       Role
	Content    string
	ToolCallID string          // set on tool result messages
	ToolCalls  []ToolCall      // set on assistant messages that call tools
	Raw        json.RawMessage // provider passthrough for tool call arguments
}

// ToolCall is one tool invocation requested by the model.
type ToolCall struct {
	ID    string
	Name  string
	Input string // JSON as the model produced it
}

// ToolDef is the schema side of a tool, sent to the provider.
type ToolDef struct {
	Name        string
	Description string
	Schema      json.RawMessage
}

// Usage is token accounting reported by the provider.
type Usage struct {
	InputTokens  int
	OutputTokens int
}

// Request is a chat completion request, provider-independent.
type Request struct {
	Model       string
	System      string
	Messages    []Message
	Tools       []ToolDef
	MaxTokens   int
	Temperature float64
}

// Response is a chat completion, provider-independent.
type Response struct {
	Text      string
	ToolCalls []ToolCall
	Usage     Usage
}

// Provider is a chat model backend. Implementations must be safe for
// concurrent use; each call becomes one anchora step in a workflow.
type Provider interface {
	// Complete performs one request/response round trip.
	Complete(ctx context.Context, req Request) (Response, error)
	// Name identifies the provider in logs and errors.
	Name() string
}

func decodeJSON(data []byte, v any) error {
	if len(data) == 0 {
		return nil
	}
	return json.Unmarshal(data, v)
}
