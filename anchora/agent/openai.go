package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// OpenAI calls an OpenAI-compatible chat completions endpoint. It covers
// OpenAI itself and any compatible backend — Ollama exposes one at
// http://localhost:11434/v1, as do most self-hosted runtimes.
type OpenAI struct {
	// APIKey is the bearer token; empty is fine for Ollama.
	APIKey string
	// BaseURL defaults to https://api.openai.com/v1.
	BaseURL string
	// HTTPClient bounds one round trip. Defaults to a two-minute client.
	HTTPClient *http.Client
}

var _ Provider = OpenAI{}

func (o OpenAI) Name() string { return "openai" }

func (o OpenAI) url() string {
	if o.BaseURL == "" {
		return "https://api.openai.com/v1/chat/completions"
	}
	return strings.TrimRight(o.BaseURL, "/") + "/chat/completions"
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	Tools       []openAITool    `json:"tools,omitempty"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
}

type openAITool struct {
	Type     string             `json:"type"`
	Function openAIToolFunction `json:"function"`
}

type openAIToolFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

type openAIToolCall struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Function struct {
		Name      string `json:"name"`
		Arguments string `json:"arguments"`
	} `json:"function"`
}

type openAIResponse struct {
	Choices []struct {
		Message openAIMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

func (o OpenAI) Complete(ctx context.Context, req Request) (Response, error) {
	messages := make([]openAIMessage, 0, len(req.Messages)+1)
	if req.System != "" {
		messages = append(messages, openAIMessage{Role: string(RoleSystem), Content: req.System})
	}
	for _, m := range req.Messages {
		out := openAIMessage{Role: string(m.Role), Content: m.Content, ToolCallID: m.ToolCallID}
		for _, call := range m.ToolCalls {
			out.ToolCalls = append(out.ToolCalls, openAIToolCall{ID: call.ID, Type: "function", Function: struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			}{Name: call.Name, Arguments: call.Input}})
		}
		messages = append(messages, out)
	}
	payload := openAIRequest{
		Model:       req.Model,
		Messages:    messages,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	}
	for _, t := range req.Tools {
		payload.Tools = append(payload.Tools, openAITool{
			Type:     "function",
			Function: openAIToolFunction{Name: t.Name, Description: t.Description, Parameters: t.Schema},
		})
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return Response{}, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url(), bytes.NewReader(body))
	if err != nil {
		return Response{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if o.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+o.APIKey)
	}
	client := o.HTTPClient
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
		return Response{}, fmt.Errorf("agent: %s returned %d: %s", o.Name(), resp.StatusCode, truncate(raw, 512))
	}
	var decoded openAIResponse
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return Response{}, fmt.Errorf("agent: decode %s response: %w", o.Name(), err)
	}
	if len(decoded.Choices) == 0 {
		return Response{}, fmt.Errorf("agent: %s returned no choices", o.Name())
	}
	out := Response{Usage: Usage{
		InputTokens:  decoded.Usage.PromptTokens,
		OutputTokens: decoded.Usage.CompletionTokens,
	}}
	for _, call := range decoded.Choices[0].Message.ToolCalls {
		out.ToolCalls = append(out.ToolCalls, ToolCall{ID: call.ID, Name: call.Function.Name, Input: call.Function.Arguments})
	}
	out.Text = decoded.Choices[0].Message.Content
	return out, nil
}

func truncate(raw []byte, limit int) string {
	if len(raw) <= limit {
		return string(raw)
	}
	return string(raw[:limit]) + "…"
}
