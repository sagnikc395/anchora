package main

import (
	"context"
	"flag"
	"github.com/sagnikc395/anchora/config"
	"github.com/sagnikc395/anchora/einoagent"
	"github.com/sagnikc395/anchora/httpapi"
	"log"
	"net/http"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML configuration file")
	flag.Parse()
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	agents := make(httpapi.AgentRegistry, len(cfg.Agents))
	for name, definition := range cfg.Agents {
		agent, err := einoagent.New(context.Background(), einoagent.Config{Name: name, Model: definition.Model, APIKeyEnv: definition.APIKeyEnv, BaseURL: definition.BaseURL, Instruction: definition.Instruction, MaxTokens: definition.MaxTokens, MaxIterations: definition.MaxIterations, Timeout: definition.Timeout()})
		if err != nil {
			log.Fatalf("configure agent %q: %v", name, err)
		}
		agents[name] = agent
	}
	handler := httpapi.NewRouter(agents, httpapi.Options{MaxRetries: cfg.Workflow.MaxRetries, RetryDelay: cfg.Workflow.RetryDelay()})
	log.Printf("anchora listening on %s", cfg.Server.Address)
	log.Fatal(http.ListenAndServe(cfg.Server.Address, handler))
}
