package anchora

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mathrand "math/rand"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// StepCtx is the context handed to a step function. It embeds context.Context,
// so it can be passed to an HTTP client or SDK directly, and it cancels when
// the step's timeout, the workflow, or the engine stops.
type StepCtx interface {
	context.Context
	// WorkflowID is the workflow this step belongs to.
	WorkflowID() string
	// FunctionID is the step's slot in the workflow's history.
	FunctionID() int
	// Attempt is the 1-based attempt number.
	Attempt() int
	// IdempotencyKey is unique to this step slot. Pass it to providers that
	// support idempotency keys to turn at-least-once into effectively-once.
	IdempotencyKey() string
	// Heartbeat records liveness (and optional progress details) for this
	// attempt so operators can see a long-running step is still alive.
	Heartbeat(details any) error
}

// StepOptions is the retry, timeout, and heartbeat policy for a step. The zero
// value means one attempt, no timeout, and no heartbeats.
type StepOptions struct {
	// MaxAttempts is the total number of attempts, including the first.
	MaxAttempts int
	// InitialBackoff is the delay before the second attempt.
	InitialBackoff time.Duration
	// BackoffFactor multiplies the delay after every attempt.
	BackoffFactor float64
	// MaxBackoff caps the backoff delay.
	MaxBackoff time.Duration
	// Jitter randomizes the backoff between half and all of the delay.
	Jitter bool
	// NonRetryable ends the retry loop when it returns true for an error.
	NonRetryable func(error) bool
	// StartToClose bounds a single attempt.
	StartToClose time.Duration
	// ScheduleToClose bounds all attempts together.
	ScheduleToClose time.Duration
	// HeartbeatTimeout cancels an attempt that stops calling Heartbeat for
	// this long. Zero disables heartbeat enforcement.
	HeartbeatTimeout time.Duration
}

// StepOption adjusts a step's policy.
type StepOption func(*StepOptions)

// Retry sets the attempt count and exponential backoff.
func Retry(maxAttempts int, initialBackoff time.Duration, backoffFactor float64) StepOption {
	return func(o *StepOptions) {
		o.MaxAttempts = maxAttempts
		o.InitialBackoff = initialBackoff
		o.BackoffFactor = backoffFactor
	}
}

// Timeout bounds a single attempt.
func Timeout(d time.Duration) StepOption {
	return func(o *StepOptions) { o.StartToClose = d }
}

// HeartbeatTimeout cancels an attempt that stops heartbeating.
func HeartbeatTimeout(d time.Duration) StepOption {
	return func(o *StepOptions) { o.HeartbeatTimeout = d }
}

// NonRetryable stops retrying errors the predicate matches.
func NonRetryable(predicate func(error) bool) StepOption {
	return func(o *StepOptions) { o.NonRetryable = predicate }
}

// WithStepOptions replaces the whole policy, for sharing one policy across
// steps.
func WithStepOptions(opts StepOptions) StepOption {
	return func(o *StepOptions) { *o = opts }
}

func resolveStepOptions(opts []StepOption) StepOptions {
	options := StepOptions{MaxAttempts: 1, BackoffFactor: 2, MaxBackoff: 30 * time.Second}
	for _, opt := range opts {
		if opt != nil {
			opt(&options)
		}
	}
	if options.MaxAttempts < 1 {
		options.MaxAttempts = 1
	}
	if options.BackoffFactor < 1 {
		options.BackoffFactor = 1
	}
	if options.MaxBackoff <= 0 {
		options.MaxBackoff = 30 * time.Second
	}
	return options
}

// Step runs fn and checkpoints its result before returning it.
//
// The first execution runs fn and records the output. A replay returns the
// recorded output without calling fn again. Only the terminal outcome is
// recorded: retries happen inside one live execution, so a crash mid-retry
// repeats the step from its first attempt, as at-least-once semantics allow.
func Step[T any](c *Context, name string, fn func(StepCtx) (T, error), opts ...StepOption) (T, error) {
	var zero T
	if name == "" {
		return zero, errors.New("anchora: step name is required")
	}
	if fn == nil {
		return zero, errors.New("anchora: step function is nil")
	}
	functionID, prior, err := c.prepare(name, KindStep)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return decodeCheckpoint[T](c.eng.codec, prior)
	}
	output, attempts, runErr := c.runStepAttempts(name, functionID, resolveStepOptions(opts), func(sc StepCtx) ([]byte, error) {
		value, err := fn(sc)
		if err != nil {
			return nil, err
		}
		return c.eng.codec.Encode(value)
	})
	if !c.checkpointable(runErr) {
		return zero, runErr
	}
	// The side effect has happened; a crash here replays the step.
	crashPoint(name, "before_checkpoint")
	cp := &Checkpoint{
		FunctionID:   functionID,
		FunctionName: name,
		Kind:         KindStep,
		Attempts:     attempts,
		StartedAt:    time.Now(),
		CompletedAt:  time.Now(),
	}
	if runErr != nil {
		cp.Err = encodeError(runErr)
	} else {
		cp.Output = output
	}
	saved, err := c.persist(cp)
	if err != nil {
		return zero, err
	}
	crashPoint(name, "after_checkpoint")
	return decodeCheckpoint[T](c.eng.codec, saved)
}

// runStepAttempts runs a step with its retry policy and returns the encoded
// output, the number of attempts, and the terminal error.
func (c *Context) runStepAttempts(name string, functionID int, o StepOptions, fn func(StepCtx) ([]byte, error)) ([]byte, int, error) {
	var output []byte
	startedAt := time.Now()
	attempts, err := c.retry(o, func(attempt int) (bool, error) {
		attemptCtx := c.ctx
		cancel := context.CancelFunc(func() {})
		if o.StartToClose > 0 || o.HeartbeatTimeout > 0 {
			attemptCtx, cancel = context.WithCancel(attemptCtx)
		}
		if o.StartToClose > 0 {
			var cancelTimeout context.CancelFunc
			attemptCtx, cancelTimeout = context.WithTimeout(attemptCtx, o.StartToClose)
			defer cancelTimeout()
		}
		spanCtx, span := tracer.Start(attemptCtx, "anchora.step "+name, trace.WithAttributes(
			attribute.String("anchora.workflow_id", c.record.ID),
			attribute.Int("anchora.function_id", functionID),
			attribute.Int("anchora.attempt", attempt),
		))
		sc := &stepContext{
			Context:       spanCtx,
			eng:           c.eng,
			workflowID:    c.record.ID,
			functionID:    functionID,
			attempt:       attempt,
			lastHeartbeat: time.Now(),
		}
		stopWatchdog := make(chan struct{})
		defer close(stopWatchdog)
		if o.HeartbeatTimeout > 0 {
			go sc.watchHeartbeat(o.HeartbeatTimeout, cancel, stopWatchdog)
		}
		if err := c.eng.store.StartStepAttempt(c.ctx, c.record.ID, functionID, attempt, c.eng.executorID); err != nil {
			c.eng.logf("anchora: record step attempt %s/%d: %v", c.record.ID, functionID, err)
		}
		value, err := fn(sc)
		cancel()
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if err == nil {
			output = value
			return true, nil
		}
		switch {
		case sc.heartbeatExpired():
			err = fmt.Errorf("%w: step %q stopped heartbeating for %s", ErrTimeout, name, o.HeartbeatTimeout)
		case errors.Is(err, context.DeadlineExceeded) && o.StartToClose > 0:
			err = fmt.Errorf("%w: step %q exceeded %s", ErrTimeout, name, o.StartToClose)
		}
		return false, err
	})
	observeStep(name, attempts, time.Since(startedAt), err)
	return output, attempts, err
}

// retry runs attemptFn until it reports completion or the policy is exhausted.
func (c *Context) retry(o StepOptions, attemptFn func(attempt int) (bool, error)) (int, error) {
	started := time.Now()
	for attempt := 1; ; attempt++ {
		if err := c.check(); err != nil {
			return attempt, err
		}
		done, err := attemptFn(attempt)
		if done || err == nil {
			return attempt, nil
		}
		if errors.Is(err, ErrFenced) || errors.Is(err, ErrCancelled) {
			return attempt, err
		}
		if o.NonRetryable != nil && o.NonRetryable(err) {
			return attempt, err
		}
		if attempt >= o.MaxAttempts {
			return attempt, err
		}
		delay := backoff(o, attempt)
		if o.ScheduleToClose > 0 && time.Since(started)+delay > o.ScheduleToClose {
			return attempt, fmt.Errorf("%w: step exceeded its %s budget: %v", ErrTimeout, o.ScheduleToClose, err)
		}
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return attempt, fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
			case <-timer.C:
			}
		}
	}
}

func backoff(o StepOptions, attempt int) time.Duration {
	delay := o.InitialBackoff
	if delay <= 0 {
		return 0
	}
	for i := 1; i < attempt; i++ {
		delay = time.Duration(float64(delay) * o.BackoffFactor)
		if delay >= o.MaxBackoff {
			delay = o.MaxBackoff
			break
		}
	}
	if o.Jitter && delay > 0 {
		delay = time.Duration(float64(delay) * (0.5 + mathrand.Float64()*0.5))
	}
	return delay
}

// stepContext is the StepCtx implementation.
type stepContext struct {
	context.Context
	eng        *Engine
	workflowID string
	functionID int
	attempt    int

	mu            sync.Mutex
	lastHeartbeat time.Time
	expired       bool
}

func (s *stepContext) WorkflowID() string { return s.workflowID }

func (s *stepContext) FunctionID() int { return s.functionID }

func (s *stepContext) Attempt() int { return s.attempt }

func (s *stepContext) IdempotencyKey() string {
	return fmt.Sprintf("%s:%d", s.workflowID, s.functionID)
}

// Heartbeat records liveness for the attempt. Errors are returned so a step
// that depends on heartbeats can fail; ordinary steps can ignore them.
func (s *stepContext) Heartbeat(details any) error {
	s.mu.Lock()
	s.lastHeartbeat = time.Now()
	s.mu.Unlock()
	var payload []byte
	if details != nil {
		encoded, err := json.Marshal(details)
		if err != nil {
			return fmt.Errorf("anchora: encode heartbeat details: %w", err)
		}
		payload = encoded
	}
	return s.eng.store.HeartbeatStep(s.Context, s.workflowID, s.functionID, s.attempt, payload)
}

func (s *stepContext) watchHeartbeat(timeout time.Duration, cancel context.CancelFunc, stop <-chan struct{}) {
	interval := timeout / 3
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-s.Context.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			last := s.lastHeartbeat
			s.mu.Unlock()
			if time.Since(last) >= timeout {
				s.mu.Lock()
				s.expired = true
				s.mu.Unlock()
				cancel()
				return
			}
		}
	}
}

func (s *stepContext) heartbeatExpired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expired
}
