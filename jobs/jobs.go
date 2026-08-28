// Package jobs provides durable, queued workflow execution.
package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sagnikc395/anchora"
)

type Step struct {
	ID        string   `json:"id"`
	Agent     string   `json:"agent"`
	Prompt    string   `json:"prompt"`
	DependsOn []string `json:"depends_on,omitempty"`
}
type Job struct {
	ID         string               `json:"id"`
	Status     anchora.Status       `json:"status"`
	CreatedAt  time.Time            `json:"created_at"`
	StartedAt  *time.Time           `json:"started_at,omitempty"`
	FinishedAt *time.Time           `json:"finished_at,omitempty"`
	Steps      []Step               `json:"steps"`
	Results    []anchora.StepResult `json:"results"`
}
type Event struct {
	ID        int64           `json:"id"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data"`
	CreatedAt time.Time       `json:"created_at"`
}
type AgentResolver interface {
	Resolve(string) (anchora.Agent, bool)
}
type Service struct {
	Store   *Store
	Queue   *Queue
	Agents  AgentResolver
	Options anchora.Options
}

func (s *Service) Submit(ctx context.Context, steps []Step) (*Job, error) {
	workflowSteps, err := s.resolveSteps(steps)
	if err != nil {
		return nil, err
	}
	if _, err := anchora.NewWorkflow(workflowSteps, s.Options); err != nil {
		return nil, err
	}
	id, err := newID()
	if err != nil {
		return nil, err
	}
	job := &Job{ID: id, Status: anchora.Pending, CreatedAt: time.Now().UTC(), Steps: steps, Results: make([]anchora.StepResult, len(steps))}
	for i, step := range steps {
		job.Results[i] = anchora.StepResult{ID: step.ID, Status: anchora.Pending}
	}
	if err := s.Store.Create(ctx, job); err != nil {
		return nil, err
	}
	if err := s.Store.AppendEvent(ctx, id, "job.queued", job); err != nil {
		return nil, err
	}
	if err := s.Queue.Push(ctx, id); err != nil {
		return nil, err
	}
	return job, nil
}
func (s *Service) Get(ctx context.Context, id string) (*Job, error) { return s.Store.Get(ctx, id) }
func (s *Service) Events(ctx context.Context, id string, after int64) ([]Event, error) {
	return s.Store.Events(ctx, id, after)
}
func (s *Service) RunWorker(ctx context.Context) error {
	for {
		id, err := s.Queue.Pop(ctx)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		err = s.runJob(ctx, id)
		if ackErr := s.Queue.Ack(ctx, id); ackErr != nil && err == nil {
			err = ackErr
		}
		if err != nil {
			continue
		}
	}
}
func (s *Service) runJob(ctx context.Context, id string) error {
	job, err := s.Store.Get(ctx, id)
	if err != nil {
		return err
	}
	if err := s.Store.SetStatus(ctx, id, anchora.Running); err != nil {
		return err
	}
	_ = s.Store.AppendEvent(ctx, id, "job.running", map[string]string{"status": string(anchora.Running)})
	steps, err := s.resolveSteps(job.Steps)
	if err != nil {
		return s.fail(ctx, id, err)
	}
	options := s.Options
	options.OnStepState = func(result anchora.StepResult) {
		_ = s.Store.UpdateStep(context.Background(), id, result)
		_ = s.Store.AppendEvent(context.Background(), id, "step.completed", result)
	}
	workflow, err := anchora.NewWorkflow(steps, options)
	if err != nil {
		return s.fail(ctx, id, err)
	}
	results, runErr := workflow.Run(ctx)
	for _, result := range results {
		if err := s.Store.UpdateStep(ctx, id, result); err != nil {
			return err
		}
	}
	status := anchora.Succeeded
	if runErr != nil {
		status = anchora.Failed
	}
	if err := s.Store.SetStatus(ctx, id, status); err != nil {
		return err
	}
	return s.Store.AppendEvent(ctx, id, "job.completed", map[string]any{"status": status, "results": results})
}
func (s *Service) fail(ctx context.Context, id string, err error) error {
	_ = s.Store.SetStatus(ctx, id, anchora.Failed)
	_ = s.Store.AppendEvent(ctx, id, "job.failed", map[string]string{"error": err.Error()})
	return err
}
func (s *Service) resolveSteps(inputs []Step) ([]anchora.Step, error) {
	steps := make([]anchora.Step, 0, len(inputs))
	for _, input := range inputs {
		agent, ok := s.Agents.Resolve(input.Agent)
		if !ok {
			return nil, fmt.Errorf("unknown agent: %s", input.Agent)
		}
		steps = append(steps, anchora.Step{ID: input.ID, Agent: agent, Prompt: input.Prompt, DependsOn: input.DependsOn})
	}
	return steps, nil
}
func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate job ID: %w", err)
	}
	return hex.EncodeToString(b), nil
}
