// Package einoagent adapts an Eino chat model to workflow.Agent.
//
// Hugging Face's Inference Providers router is the default endpoint, but the
// adapter speaks plain OpenAI chat completions, so BaseURL points it at any
// compatible provider.
package einoagent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/schema"
)

const (
	defaultBaseURL  = "https://router.huggingface.co/v1"
	defaultTokenEnv = "HF_TOKEN"
)

// Config configures an OpenAI-compatible Eino chat model.
type Config struct {
	// Name identifies the agent in configuration and error messages.
	Name string
	// ModelID is the provider's model name. It is required.
	ModelID string
	// BaseURL is the OpenAI-compatible endpoint. Zero uses Hugging Face's
	// Inference Providers router.
	BaseURL string
	// TokenEnv names the environment variable holding the bearer token. Zero
	// uses HF_TOKEN.
	TokenEnv string
	// Instruction is sent as a system message ahead of every prompt.
	Instruction string
	// MaxTokens caps the response length. Zero omits the field from the
	// request, leaving the provider's own default in place.
	MaxTokens int
	// Timeout bounds a single generation. Zero leaves the client unbounded.
	Timeout time.Duration
	// HTTPClient replaces the default transport. It is primarily a test seam.
	HTTPClient *http.Client
}

// Agent is an Anchora agent backed by Eino's OpenAI-compatible ChatModel.
type Agent struct {
	model       *openai.ChatModel
	instruction string
}

// New creates an Eino chat model for an OpenAI-compatible endpoint. It fails if
// the configured token variable is unset, so a misconfigured agent is caught at
// startup rather than on the first request.
func New(ctx context.Context, config Config) (*Agent, error) {
	if config.Name == "" || config.ModelID == "" {
		return nil, errors.New("Eino agent requires a name and model ID")
	}
	if config.TokenEnv == "" {
		config.TokenEnv = defaultTokenEnv
	}
	token := os.Getenv(config.TokenEnv)
	if token == "" {
		return nil, fmt.Errorf("environment variable %q is not set", config.TokenEnv)
	}
	if config.BaseURL == "" {
		config.BaseURL = defaultBaseURL
	}
	var maxTokens *int
	if config.MaxTokens > 0 {
		maxTokens = &config.MaxTokens
	}
	model, err := openai.NewChatModel(ctx, &openai.ChatModelConfig{
		APIKey:     token,
		BaseURL:    config.BaseURL,
		Model:      config.ModelID,
		MaxTokens:  maxTokens,
		Timeout:    config.Timeout,
		HTTPClient: config.HTTPClient,
	})
	if err != nil {
		return nil, fmt.Errorf("create Eino chat model: %w", err)
	}
	return &Agent{model: model, instruction: config.Instruction}, nil
}

// Run generates one response using Eino's standard chat-model interface. An
// empty completion is an error: a workflow step that produced no text has
// nothing to pass downstream, and retrying it is usually the right move.
func (a *Agent) Run(ctx context.Context, prompt string) (string, error) {
	if a == nil || a.model == nil {
		return "", errors.New("Eino agent is not initialized")
	}
	messages := make([]*schema.Message, 0, 2)
	if a.instruction != "" {
		messages = append(messages, schema.SystemMessage(a.instruction))
	}
	messages = append(messages, schema.UserMessage(prompt))
	response, err := a.model.Generate(ctx, messages)
	if err != nil {
		return "", fmt.Errorf("generate with Eino chat model: %w", err)
	}
	if response == nil || strings.TrimSpace(response.Content) == "" {
		return "", errors.New("Eino chat model returned no text output")
	}
	return response.Content, nil
}
