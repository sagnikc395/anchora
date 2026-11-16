package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Anthropic calls the Claude messages API.
type Anthropic struct {
	// APIKey is the x-api-key credential.
	APIKey string
	// Version pins the API version. Defaults to 2023-06-01.
	Version string
	// BaseURL defaults to https://api.anthropic.com.
	BaseURL string
	// HTTPClient bounds one round trip. Defaults to a two-minute client.
	HTTPClient *http.Client
}

var _ Provider = Anthropic{}

func (a Anthropic) Name() string { return "anthropic" }

type anthropicRequest struct {
	Model       string          `json:"model"`
	System      string          `json:"system,omitempty"`
	Messages    []anthropicMSG  `json:"messages"`
	Tools       []anthropicTool `json:"tools,omitempty"`
	MaxTokens   int             `json:"max_tokens"`
	Temperature float64         `json:"temperature,omitempty"`
}

type anthropicMSG struct {
	Role    string           `json:"role"`
	Content []anthropicBlock `json:"content"`
}

type anthropicBlock struct {
	Type      string          `json:"type"`
	Text      string          `json:"text,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
	ToolUseID string          `json:"tool_use_id,omitempty"`
	Content   string          `json:"content,omitempty"`
}

type anthropicTool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"input_schema"`
}

type anthropicResponse struct {
	Content []anthropicBlock `json:"content"`
	Usage   struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
}

func (a Anthropic) Complete(ctx context.Context, req Request) (Response, error) {
	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = 4096
	}
	messages := make([]anthropicMSG, 0, len(req.Messages))
	for _, m := range req.Messages {
		if m.Role == RoleSystem {
			continue // Anthropic takes the system prompt separately.
		}
		am := anthropicMSG{Role: string(m.Role)}
		switch {
		case m.ToolCallID != "":
			am.Content = []anthropicBlock{{Type: "tool_result", ToolUseID: m.ToolCallID, Content: m.Content}}
		case len(m.ToolCalls) > 0:
			am.Content = append(am.Content, anthropicBlock{Type: "text", Text: m.Content})
			for _, call := range m.ToolCalls {
				am.Content = append(am.Content, anthropicBlock{
					Type: "tool_use", ID: call.ID, Name: call.Name,
					Input: json.RawMessage(orEmptyObject(json.RawMessage(call.Input))),
				})
			}
		default:
			am.Content = []anthropicBlock{{Type: "text", Text: m.Content}}
		}
		messages = append(messages, am)
	}
	payload := anthropicRequest{
		Model: req.Model, System: req.System, Messages: messages,
		MaxTokens: maxTokens, Temperature: req.Temperature,
	}
	for _, t := range req.Tools {
		payload.Tools = append(payload.Tools, anthropicTool{
			Name: t.Name, Description: t.Description,
			InputSchema: json.RawMessage(orEmptyObject(t.Schema)),
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Response{}, err
	}
	base := a.BaseURL
	if base == "" {
		base = "https://api.anthropic.com"
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/v1/messages", bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.APIKey)
	if a.Version == "" {
		a.Version = "2023-06-01"
	}
	httpReq.Header.Set("anthropic-version", a.Version)
	client := a.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Minute}
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return Response{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return Response{}, fmt.Errorf("agent: %s returned %d: %s", a.Name(), resp.StatusCode, truncate(raw, 512))
	}
	var decoded anthropicResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return Response{}, fmt.Errorf("agent: decode %s response: %w", a.Name(), err)
	}
	out := Response{Usage: Usage{
		InputTokens:  decoded.Usage.InputTokens,
		OutputTokens: decoded.Usage.OutputTokens,
	}}
	var textParts []string
	for _, block := range decoded.Content {
		switch block.Type {
		case "text":
			textParts = append(textParts, block.Text)
		case "tool_use":
			out.ToolCalls = append(out.ToolCalls, ToolCall{ID: block.ID, Name: block.Name, Input: string(block.Input)})
		}
	}
	out.Text = joinStrings(textParts)
	return out, nil
}

func orEmptyObject(raw json.RawMessage) string {
	if len(raw) == 0 || string(raw) == "null" {
		return "{}"
	}
	return string(raw)
}

func joinStrings(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += "\n"
		}
		out += p
	}
	return out
}
