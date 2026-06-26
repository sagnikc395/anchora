package main

import (
	"context"
	"fmt"
	"log"

	"charm.land/fantasy"
	"charm.land/fantasy/providers/openaicompat"
	"github.com/joho/godotenv"
)

func main() {
	// Load .env before anything else
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file found, falling back to system env")
	}

	cfg, err := LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	apiKey, err := cfg.Provider.APIKey()
	if err != nil {
		log.Fatalf("api key error: %v", err)
	}

	provider, err := openaicompat.New(
		openaicompat.WithAPIKey(apiKey),
		openaicompat.WithBaseURL(cfg.Provider.BaseURL),
	)
	if err != nil {
		log.Fatalf("provider error: %v", err)
	}

	ctx := context.Background()

	model, err := provider.LanguageModel(ctx, cfg.Provider.Model)
	if err != nil {
		log.Fatalf("model error: %v", err)
	}

	summarizeTool := fantasy.NewAgentTool(
		"summarize",
		"Summarizes a given block of text.",
		func(ctx context.Context, input struct {
			Text string `json:"text"`
		}, call fantasy.ToolCall) (fantasy.ToolResponse, error) {
			summary := fmt.Sprintf("Summary: %s [truncated]", input.Text[:min(len(input.Text), 100)])
			return fantasy.NewTextResponse(summary), nil
		},
	)

	researchAgent := fantasy.NewAgent(
		model,
		fantasy.WithSystemPrompt("You are a research assistant. Be concise."),
		fantasy.WithMaxOutputTokens(cfg.Provider.MaxTokens),
	)

	summaryAgent := fantasy.NewAgent(
		model,
		fantasy.WithSystemPrompt("You are a summarization assistant."),
		fantasy.WithMaxOutputTokens(cfg.Provider.MaxTokens),
		fantasy.WithTools(summarizeTool),
	)

	wf := &Workflow{
		MaxRetries:   cfg.Workflow.MaxRetries,
		RetryDelayMS: cfg.Workflow.RetryDelayMS,
		Steps: []*Step{
			{
				ID:     "research",
				Agent:  researchAgent,
				Prompt: "What are the three main uses of Go's select statement?",
			},
			{
				ID:     "summarize",
				Agent:  summaryAgent,
				Prompt: "Summarize the key points of Go concurrency primitives in two sentences.",
			},
		},
	}

	if err := wf.Run(ctx); err != nil {
		log.Fatal(err)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
