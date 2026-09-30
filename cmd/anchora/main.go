// Command anchora serves the Anchora workflow API.
//
// It always serves the synchronous API. When async is enabled in the
// configuration it also connects the durable job backend and starts this
// node's worker pool, so a single process is a complete deployment and several
// pointed at the same PostgreSQL and Redis form a cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sagnikc395/anchora/internal/config"
	"github.com/sagnikc395/anchora/internal/einoagent"
	"github.com/sagnikc395/anchora/internal/httpapi"
	"github.com/sagnikc395/anchora/internal/jobs"
	"github.com/sagnikc395/anchora/internal/workflow"
)

const (
	// shutdownGrace bounds the whole drain: the HTTP server stops accepting
	// work, then in-flight jobs get whatever is left to release their leases,
	// so a rolling restart does not wait out the visibility timeout.
	shutdownGrace = 15 * time.Second
	// readHeaderTimeout bounds request headers only. The SSE endpoint streams
	// for as long as a job runs, so the body and response stay unbounded.
	readHeaderTimeout = 10 * time.Second
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML configuration file")
	flag.Parse()
	if err := run(*configPath); err != nil {
		log.Fatal(err)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	agents, err := buildAgents(ctx, cfg.Agents)
	if err != nil {
		return err
	}
	options := httpapi.Options{MaxRetries: cfg.Workflow.MaxRetries, RetryDelay: cfg.Workflow.RetryDelay()}

	var service *jobs.Service
	var workers sync.WaitGroup
	if cfg.Async.Enabled {
		built, closeBackend, err := newJobService(ctx, cfg, agents)
		if err != nil {
			return err
		}
		defer closeBackend()
		service = built
		if err := startWorkers(ctx, &workers, service, cfg.Async); err != nil {
			return err
		}
	}

	server := &http.Server{
		Addr:              cfg.Server.Address,
		Handler:           httpapi.NewRouterWithJobs(agents, options, service),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	serverErr := make(chan error, 1)
	go func() {
		log.Printf("anchora listening on %s", cfg.Server.Address)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- fmt.Errorf("http server: %w", err)
			return
		}
		serverErr <- nil
	}()

	select {
	case err := <-serverErr:
		// The listener failed outright; there is nothing to drain.
		return err
	case <-ctx.Done():
	}

	log.Print("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := server.Shutdown(shutdown); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	drained := make(chan struct{})
	go func() { workers.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-shutdown.Done():
		log.Print("workers did not drain in time; their leases will expire")
	}
	return nil
}

// buildAgents constructs every configured agent up front, so a bad model or a
// missing token is a startup failure rather than a failed workflow step.
func buildAgents(ctx context.Context, definitions map[string]config.Agent) (httpapi.AgentRegistry, error) {
	agents := make(httpapi.AgentRegistry, len(definitions))
	for name, definition := range definitions {
		agent, err := einoagent.New(ctx, einoagent.Config{
			Name:        name,
			ModelID:     definition.ModelID,
			BaseURL:     definition.BaseURL,
			TokenEnv:    definition.TokenEnv,
			Instruction: definition.Instruction,
			MaxTokens:   definition.MaxTokens,
			Timeout:     definition.Timeout(),
		})
		if err != nil {
			return nil, fmt.Errorf("configure agent %q: %w", name, err)
		}
		agents[name] = agent
	}
	return agents, nil
}

// newJobService connects the durable backend and returns the service along with
// a function that releases both connections.
func newJobService(ctx context.Context, cfg config.Config, agents httpapi.AgentRegistry) (*jobs.Service, func(), error) {
	store, err := jobs.NewStore(ctx, cfg.Async.DatabaseURL())
	if err != nil {
		return nil, nil, fmt.Errorf("PostgreSQL: %w", err)
	}
	queue, err := jobs.NewQueue(ctx, cfg.Async.RedisURL(), cfg.Async.QueueName)
	if err != nil {
		store.Close()
		return nil, nil, fmt.Errorf("Redis: %w", err)
	}
	service := &jobs.Service{
		Store:  store,
		Queue:  queue,
		Agents: agents,
		Options: workflow.Options{
			MaxRetries: cfg.Workflow.MaxRetries,
			RetryDelay: cfg.Workflow.RetryDelay(),
		},
		Config: jobs.Config{
			Lease:          cfg.Async.Lease(),
			Heartbeat:      cfg.Async.Heartbeat(),
			MaxAttempts:    cfg.Async.MaxAttempts,
			ReaperInterval: cfg.Async.ReaperInterval(),
			WorkerTTL:      cfg.Async.WorkerTTL(),
			ReclaimBatch:   cfg.Async.ReclaimBatch,
			QueueName:      cfg.Async.QueueName,
		},
		Logf: log.Printf,
	}
	return service, func() {
		if err := queue.Close(); err != nil {
			log.Printf("close Redis: %v", err)
		}
		store.Close()
	}, nil
}

// startWorkers launches this node's worker pool and, unless it is disabled, the
// recovery sweep. Every goroutine it starts is tracked by workers.
func startWorkers(ctx context.Context, workers *sync.WaitGroup, service *jobs.Service, async config.Async) error {
	for range async.WorkerCount() {
		workerID, err := jobs.NewWorkerID()
		if err != nil {
			return fmt.Errorf("generate worker ID: %w", err)
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := service.RunWorker(ctx, workerID); err != nil && ctx.Err() == nil {
				log.Printf("worker %s stopped: %v", workerID, err)
			}
		}()
	}
	if async.ReaperEnabled() {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := service.RunReaper(ctx); err != nil && ctx.Err() == nil {
				log.Printf("reaper stopped: %v", err)
			}
		}()
	}
	log.Printf("async enabled: %d worker(s), %s lease, reaper=%t",
		async.WorkerCount(), async.Lease(), async.ReaperEnabled())
	return nil
}
