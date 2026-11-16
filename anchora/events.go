package anchora

import (
	"errors"
	"fmt"
	"time"
)

// SetEvent publishes a key/value pair for this workflow, overwriting any
// earlier value. External callers and other workflows read it through
// Engine.WorkflowEvent with their own context, or through GetEvent.
//
// The write happens before the checkpoint is recorded: a crash in between
// rewrites the same value on replay, while the reverse order could lose it.
func SetEvent(c *Context, key string, value any) error {
	if key == "" {
		return errors.New("anchora: event key is required")
	}
	functionID, prior, err := c.prepare("setevent:"+key, KindSetEvent)
	if err != nil {
		return err
	}
	if prior != nil {
		return nil
	}
	encoded, err := c.eng.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("anchora: encode event %q: %w", key, err)
	}
	if err := c.eng.store.SetWorkflowEvent(c.ctx, c.record.ID, key, encoded); err != nil {
		return err
	}
	saved, err := c.persist(&Checkpoint{
		FunctionID:   functionID,
		FunctionName: "setevent:" + key,
		Kind:         KindSetEvent,
		Attempts:     1,
		StartedAt:    time.Now(),
		CompletedAt:  time.Now(),
	})
	if err != nil {
		return err
	}
	if saved.Err != nil {
		return saved.Err.Err()
	}
	return nil
}

// GetEvent waits until key has been published on workflowID (empty means this
// workflow) and returns it with its type. The read is recorded, so a replay
// returns the value the original execution saw even if the event changed
// since.
//
// A positive timeout records a timeout error when nothing is published, which
// replays identically. Zero checks once without waiting; a negative timeout
// waits indefinitely.
func GetEvent[T any](c *Context, workflowID, key string, timeout time.Duration) (T, error) {
	var zero T
	if workflowID == "" {
		workflowID = c.record.ID
	}
	functionID, prior, err := c.prepare("getevent:"+workflowID+":"+key, KindGetEvent)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return decodeCheckpoint[T](c.eng.codec, prior)
	}

	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
	}
	for {
		value, ok, err := c.eng.store.GetWorkflowEvent(c.ctx, workflowID, key)
		if err != nil {
			return zero, err
		}
		cp := &Checkpoint{
			FunctionID:   functionID,
			FunctionName: "getevent:" + workflowID + ":" + key,
			Kind:         KindGetEvent,
			Attempts:     1,
			StartedAt:    time.Now(),
		}
		if ok {
			cp.Output = value
			cp.CompletedAt = time.Now()
		} else if timeout >= 0 && (timeout == 0 || !time.Now().Before(deadline)) {
			cp.Err = encodeError(fmt.Errorf("%w: event %q of %s was not published within %s", ErrTimeout, key, workflowID, timeout))
			cp.CompletedAt = time.Now()
		} else {
			wait := 200 * time.Millisecond
			if !deadline.IsZero() && time.Until(deadline) < wait {
				wait = time.Until(deadline)
			}
			timer := time.NewTimer(wait)
			select {
			case <-c.ctx.Done():
				timer.Stop()
				return zero, fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
			case <-timer.C:
			}
			continue
		}
		saved, err := c.persist(cp)
		if err != nil {
			return zero, err
		}
		return decodeCheckpoint[T](c.eng.codec, saved)
	}
}
