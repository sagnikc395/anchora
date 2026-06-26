package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"charm.land/fantasy"
)

type Step struct {
	ID     string
	Agent  fantasy.Agent
	Prompt string

	status   Status
	attempts int
	Output   string
	Err      error
}

type Workflow struct {
	Steps        []*Step
	MaxRetries   int
	RetryDelayMS int
}

func (w *Workflow) Run(ctx context.Context) error {
	for _, step := range w.Steps {
		if err := w.runStep(ctx, step); err != nil {
			return fmt.Errorf("workflow failed at step %q: %w", step.ID, err)
		}
		log.Printf("[%s] output: %s", step.ID, step.Output)
	}
	return nil
}

func (w *Workflow) runStep(ctx context.Context, s *Step) error {
	apply := func(e Event) error {
		next, err := transition(s.status, e)
		if err != nil {
			return err
		}
		log.Printf("[%s] %s --> %s", s.ID, s.status, next)
		s.status = next
		return nil
	}

	if err := apply(EventStart); err != nil {
		return err
	}

	for {
		result, err := s.Agent.Generate(ctx, fantasy.AgentCall{
			Prompt: s.Prompt,
		})

		if err == nil {
			s.Output = result.Response.Content.Text()
			return apply(EventSucceed)
		}

		s.Err = err

		if s.attempts >= w.MaxRetries {
			_ = apply(EventFail)
			return fmt.Errorf("step %q exhausted retries: %w", s.ID, err)
		}

		if err := apply(EventFail); err != nil {
			return err
		}
		s.attempts++
		if err := apply(EventRetry); err != nil {
			return err
		}
		if err := apply(EventStart); err != nil {
			return err
		}

		delay := time.Duration(s.attempts*w.RetryDelayMS) * time.Millisecond
		log.Printf("[%s] retrying in %s (attempt %d/%d)", s.ID, delay, s.attempts, w.MaxRetries)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
