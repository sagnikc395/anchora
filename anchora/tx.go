package anchora

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// Tx runs fn inside a Postgres transaction that also writes the step's
// checkpoint. Either the business writes and the checkpoint commit together or
// neither does, which makes Tx steps exactly-once across crashes.
//
// fn must only use the transaction it is given: a write that escapes the
// transaction is outside the guarantee.
func Tx[T any](c *Context, name string, fn func(StepCtx, TxConn) (T, error), opts ...StepOption) (T, error) {
	var zero T
	if name == "" {
		return zero, errors.New("anchora: step name is required")
	}
	if fn == nil {
		return zero, errors.New("anchora: step function is nil")
	}
	functionID, prior, err := c.prepare(name, KindTx)
	if err != nil {
		return zero, err
	}
	if prior != nil {
		return decodeCheckpoint[T](c.eng.codec, prior)
	}

	options := resolveStepOptions(opts)
	var saved *Checkpoint
	var last *Checkpoint
	_, runErr := c.retry(options, func(attempt int) (bool, error) {
		txCtx := c.ctx
		if timeout := options.StartToClose; timeout > 0 {
			var cancel context.CancelFunc
			txCtx, cancel = context.WithTimeout(c.ctx, timeout)
			defer cancel()
		}
		cp := &Checkpoint{
			FunctionID:   functionID,
			FunctionName: name,
			Kind:         KindTx,
			Attempts:     attempt,
			StartedAt:    time.Now(),
		}
		last = cp
		created, existing, err := c.eng.store.TxStep(txCtx, c.record.ID, c.record.Epoch, cp, func(ctx context.Context, tx TxConn) error {
			sc := &stepContext{
				Context:       ctx,
				eng:           c.eng,
				workflowID:    c.record.ID,
				functionID:    functionID,
				attempt:       attempt,
				lastHeartbeat: time.Now(),
			}
			value, err := fn(sc, tx)
			if err != nil {
				return err
			}
			encoded, err := c.eng.codec.Encode(value)
			if err != nil {
				return fmt.Errorf("anchora: encode step %q: %w", name, err)
			}
			cp.Output = encoded
			return nil
		})
		if err != nil {
			return false, err
		}
		cp.CompletedAt = time.Now()
		if created {
			saved = cp
		} else {
			saved = existing
		}
		return true, nil
	})
	if runErr != nil {
		if !c.checkpointable(runErr) || last == nil {
			return zero, runErr
		}
		// The transaction rolled back and the retries are exhausted: record the
		// failure so a replay observes it instead of rerunning the step.
		last.Err = encodeError(runErr)
		last.CompletedAt = time.Now()
		recorded, err := c.persist(last)
		if err != nil {
			return zero, err
		}
		return decodeCheckpoint[T](c.eng.codec, recorded)
	}
	if saved == nil {
		return zero, fmt.Errorf("anchora: transaction step %q recorded nothing", name)
	}
	return decodeCheckpoint[T](c.eng.codec, saved)
}
