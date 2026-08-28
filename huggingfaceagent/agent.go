// Package huggingfaceagent adapts Hugging Face Inference Providers to anchora.Agent.
package huggingfaceagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const chatCompletionsURL = "https://router.huggingface.co/v1/chat/completions"

type Config struct {
	Name, ModelID, TokenEnv, Instruction string
	MaxTokens                            int
	Timeout                              time.Duration
	HTTPClient                           *http.Client
}

type Agent struct {
	modelID     string
	token       string
	instruction string
	maxTokens   int
	client      *http.Client
}

// New creates an agent that calls Hugging Face Inference Providers. The model
// ID may include a routing policy or provider suffix (for example, :fastest).
func New(_ context.Context, config Config) (*Agent, error) {
	if config.Name == "" || config.ModelID == "" {
		return nil, errors.New("Hugging Face agent requires a name and model ID")
	}
	if config.TokenEnv == "" {
		config.TokenEnv = "HF_TOKEN"
	}
	token := os.Getenv(config.TokenEnv)
	if token == "" {
		return nil, fmt.Errorf("environment variable %q is not set", config.TokenEnv)
	}
	client := config.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: config.Timeout}
	}
	return &Agent{modelID: config.ModelID, token: token, instruction: config.Instruction, maxTokens: config.MaxTokens, client: client}, nil
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model     string        `json:"model"`
	Messages  []chatMessage `json:"messages"`
	MaxTokens int           `json:"max_tokens,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (a *Agent) Run(ctx context.Context, prompt string) (string, error) {
	if a == nil || a.client == nil {
		return "", errors.New("Hugging Face agent is not initialized")
	}
	messages := make([]chatMessage, 0, 2)
	if a.instruction != "" {
		messages = append(messages, chatMessage{Role: "system", Content: a.instruction})
	}
	messages = append(messages, chatMessage{Role: "user", Content: prompt})
	body, err := json.Marshal(chatRequest{Model: a.modelID, Messages: messages, MaxTokens: a.maxTokens})
	if err != nil {
		return "", fmt.Errorf("encode Hugging Face inference request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, chatCompletionsURL, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create Hugging Face inference request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("call Hugging Face inference: %w", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return "", fmt.Errorf("read Hugging Face inference response: %w", err)
	}
	var response chatResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return "", fmt.Errorf("decode Hugging Face inference response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message := "request failed"
		if response.Error != nil && response.Error.Message != "" {
			message = response.Error.Message
		}
		return "", fmt.Errorf("Hugging Face inference returned HTTP %d: %s", resp.StatusCode, message)
	}
	if len(response.Choices) == 0 || strings.TrimSpace(response.Choices[0].Message.Content) == "" {
		return "", errors.New("Hugging Face inference returned no text output")
	}
	return response.Choices[0].Message.Content, nil
}
