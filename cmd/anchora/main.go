package main

import (
	"context"
	"flag"
	"github.com/sagnikc395/anchora"
	"github.com/sagnikc395/anchora/config"
	"github.com/sagnikc395/anchora/httpapi"
	"github.com/sagnikc395/anchora/huggingfaceagent"
	"github.com/sagnikc395/anchora/jobs"
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
		agent, err := huggingfaceagent.New(context.Background(), huggingfaceagent.Config{Name: name, ModelID: definition.ModelID, TokenEnv: definition.TokenEnv, Instruction: definition.Instruction, MaxTokens: definition.MaxTokens, Timeout: definition.Timeout()})
		if err != nil {
			log.Fatalf("configure agent %q: %v", name, err)
		}
		agents[name] = agent
	}
	options := httpapi.Options{MaxRetries: cfg.Workflow.MaxRetries, RetryDelay: cfg.Workflow.RetryDelay()}
	var service *jobs.Service
	if cfg.Async.Enabled {
		store, err := jobs.NewStore(context.Background(), cfg.Async.DatabaseURL())
		if err != nil {
			log.Fatalf("connect PostgreSQL: %v", err)
		}
		defer store.Close()
		queue, err := jobs.NewQueue(cfg.Async.RedisURL(), cfg.Async.QueueName)
		if err != nil {
			log.Fatalf("connect Redis: %v", err)
		}
		defer queue.Close()
		service = &jobs.Service{Store: store, Queue: queue, Agents: agents, Options: anchora.Options{MaxRetries: options.MaxRetries, RetryDelay: options.RetryDelay}}
		for i := 0; i < cfg.Async.WorkerCount(); i++ {
			go func() {
				if err := service.RunWorker(context.Background()); err != nil {
					log.Printf("workflow worker stopped: %v", err)
				}
			}()
		}
	}
	handler := httpapi.NewRouterWithJobs(agents, options, service)
	log.Printf("anchora listening on %s", cfg.Server.Address)
	log.Fatal(http.ListenAndServe(cfg.Server.Address, handler))
}
