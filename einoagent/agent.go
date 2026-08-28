// Package einoagent adapts Eino ADK agents to anchora.Agent.
package einoagent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/cloudwego/eino-ext/components/model/openai"
	"github.com/cloudwego/eino/adk"
)

type Config struct {
	Name, Model, APIKeyEnv, BaseURL, Instruction string
	MaxTokens, MaxIterations                     int
	Timeout                                      time.Duration
}
type Agent struct{ runner *adk.Runner }

// New creates an Eino ChatModelAgent backed by an OpenAI-compatible endpoint.
func New(ctx context.Context, config Config) (*Agent, error) {
	if config.Name == "" || config.Model == "" {
		return nil, errors.New("Eino agent requires a name and model")
	}
	if config.APIKeyEnv == "" {
		config.APIKeyEnv = "OPENAI_API_KEY"
	}
	apiKey := os.Getenv(config.APIKeyEnv)
	if apiKey == "" {
		return nil, fmt.Errorf("environment variable %q is not set", config.APIKeyEnv)
	}
	modelConfig := &openai.ChatModelConfig{APIKey: apiKey, Model: config.Model, BaseURL: config.BaseURL, Timeout: config.Timeout}
	if config.MaxTokens > 0 {
		modelConfig.MaxCompletionTokens = &config.MaxTokens
	}
	model, err := openai.NewChatModel(ctx, modelConfig)
	if err != nil {
		return nil, fmt.Errorf("create Eino chat model: %w", err)
	}
	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{Name: config.Name, Instruction: config.Instruction, Model: model, MaxIterations: config.MaxIterations})
	if err != nil {
		return nil, fmt.Errorf("create Eino agent: %w", err)
	}
	return &Agent{runner: adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent})}, nil
}
func (a *Agent) Run(ctx context.Context, prompt string) (string, error) {
	if a == nil || a.runner == nil {
		return "", errors.New("Eino agent is not initialized")
	}
	iterator := a.runner.Query(ctx, prompt)
	var output string
	for {
		event, ok := iterator.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			return "", event.Err
		}
		if event.Output == nil || event.Output.MessageOutput == nil {
			continue
		}
		message, err := event.Output.MessageOutput.GetMessage()
		if err != nil {
			return "", fmt.Errorf("read Eino agent output: %w", err)
		}
		if message != nil && message.Content != "" {
			output = message.Content
		}
	}
	if output == "" {
		return "", errors.New("Eino agent returned no text output")
	}
	return output, nil
}
