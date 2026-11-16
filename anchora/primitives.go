package anchora

import (
	"fmt"
	mathrand "math/rand"
	"time"
)

// Now returns the current UTC time, recorded on first execution and replayed
// from the checkpoint afterwards. Workflow code must use this instead of
// time.Now.
func Now(c *Context) (time.Time, error) {
	return recordValue(c, "now", KindNow, func() (time.Time, error) {
		return time.Now().UTC(), nil
	})
}

// Random returns a random float in [0,1), recorded on first execution and
// replayed afterwards. Workflow code must use this instead of math/rand.
func Random(c *Context) (float64, error) {
	return recordValue(c, "random", KindRandom, func() (float64, error) {
		return mathrand.Float64(), nil
	})
}

// UUID returns a UUIDv7, recorded on first execution and replayed afterwards.
// Workflow code must use this instead of generating ids directly.
func UUID(c *Context) (string, error) {
	return recordValue(c, "uuid", KindUUID, newUUIDv7)
}

// Sleep is a durable timer. The first execution records an absolute wake time;
// a replay that resumes after a crash sleeps only the remaining duration, and a
// replay of a timer that already elapsed returns immediately.
func Sleep(c *Context, d time.Duration) error {
	if d < 0 {
		d = 0
	}
	functionID, prior, err := c.prepare("sleep", KindSleep)
	if err != nil {
		return err
	}
	var wakeAt time.Time
	if prior != nil {
		if prior.Err != nil {
			return prior.Err.Err()
		}
		if err := c.eng.codec.Decode(prior.Output, &wakeAt); err != nil {
			return fmt.Errorf("anchora: decode sleep checkpoint: %w", err)
		}
	} else {
		wakeAt = time.Now().Add(d).UTC()
		encoded, err := c.eng.codec.Encode(wakeAt)
		if err != nil {
			return fmt.Errorf("anchora: encode sleep deadline: %w", err)
		}
		saved, err := c.persist(&Checkpoint{
			FunctionID:   functionID,
			FunctionName: "sleep",
			Kind:         KindSleep,
			Output:       encoded,
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
		if err := c.eng.codec.Decode(saved.Output, &wakeAt); err != nil {
			return fmt.Errorf("anchora: decode sleep checkpoint: %w", err)
		}
	}

	remaining := time.Until(wakeAt)
	if remaining <= 0 {
		return nil
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-c.ctx.Done():
		return fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
	case <-timer.C:
		return nil
	}
}
