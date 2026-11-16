package anchora

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Future is the handle for a step started with Go. Every future must be
// waited on before the workflow returns: an unwaited future's result is not
// part of the workflow's output.
type Future[T any] struct {
	mu    sync.Mutex
	done  chan struct{}
	value T
	err   error
}

// Go starts fn in a goroutine, like a step that runs in parallel with the
// workflow. The function id is reserved synchronously at spawn time, so
// concurrent calls get deterministic slots in program order.
//
// Each Go runs its fn as one checkpointed unit: fn may block on I/O through
// its StepCtx but must not itself call workflow APIs. The engine waits for
// spawned futures before completing the workflow, so a crash mid-flight
// replays any slot that was not yet recorded.
func Go[T any](c *Context, name string, fn func(StepCtx) (T, error), opts ...StepOption) *Future[T] {
	f := &Future[T]{done: make(chan struct{})}
	resolve := func(value T, err error) {
		f.mu.Lock()
		f.value, f.err = value, err
		f.mu.Unlock()
		close(f.done)
	}
	if name == "" {
		var zero T
		resolve(zero, errors.New("anchora: step name is required"))
		return f
	}
	if fn == nil {
		var zero T
		resolve(zero, errors.New("anchora: step function is nil"))
		return f
	}
	functionID, prior, err := c.prepare(name, KindStep)
	if err != nil {
		var zero T
		resolve(zero, err)
		return f
	}
	if prior != nil {
		value, err := decodeCheckpoint[T](c.eng.codec, prior)
		resolve(value, err)
		return f
	}
	options := resolveStepOptions(opts)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		output, attempts, runErr := c.runStepAttempts(name, functionID, options, func(sc StepCtx) ([]byte, error) {
			value, err := fn(sc)
			if err != nil {
				return nil, err
			}
			return c.eng.codec.Encode(value)
		})
		if !c.checkpointable(runErr) {
			var zero T
			resolve(zero, runErr)
			return
		}
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
			var zero T
			resolve(zero, err)
			return
		}
		value, err := decodeCheckpoint[T](c.eng.codec, saved)
		resolve(value, err)
	}()
	return f
}

// Wait blocks until the parallel step records its outcome and returns it. If
// ctx finishes first, Wait returns ctx's error; the step keeps running and its
// checkpoint still lands, so a later replay observes it.
func (f *Future[T]) Wait(ctx context.Context) (T, error) {
	select {
	case <-f.done:
	case <-ctx.Done():
		// The future may have resolved in the same instant; prefer its result.
		select {
		case <-f.done:
		default:
			var zero T
			return zero, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.value, f.err
}
