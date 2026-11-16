package pgflow

import (
	"errors"
	"fmt"
	"time"
)

// Send delivers a durable message to another workflow. The message id is
// derived from the sending workflow and the function slot, and the checkpoint
// is written in the same transaction as the message insert, so a send happens
// exactly once per slot even if the workflow is replayed after a crash.
func Send(c *Context, destinationID, topic string, payload any) error {
	if destinationID == "" {
		return errors.New("pgflow: send requires a destination workflow id")
	}
	functionID, prior, err := c.prepare("send:"+topic, KindSend)
	if err != nil {
		return err
	}
	if prior != nil {
		if prior.Err != nil {
			return prior.Err.Err()
		}
		return nil
	}
	encoded, err := c.eng.codec.Encode(payload)
	if err != nil {
		return fmt.Errorf("pgflow: encode message payload: %w", err)
	}
	cp := &Checkpoint{
		FunctionID:   functionID,
		FunctionName: "send:" + topic,
		Kind:         KindSend,
		Attempts:     1,
		StartedAt:    time.Now(),
		CompletedAt:  time.Now(),
	}
	message := Message{
		ID:            fmt.Sprintf("%s:%d", c.record.ID, functionID),
		DestinationID: destinationID,
		Topic:         topic,
		Payload:       encoded,
	}
	created, existing, err := c.eng.store.SendMessage(c.ctx, c.record.ID, c.record.Epoch, cp, message)
	if err != nil {
		return err
	}
	if !created && existing != nil && existing.Err != nil {
		return existing.Err.Err()
	}
	return nil
}

// Recv waits for a durable message on topic and checkpoints it. Consuming the
// message and recording the checkpoint happen in one transaction, so a message
// is never both delivered and lost.
//
// A positive timeout records a timeout error when no message arrives, which
// replays identically. Zero checks once without waiting; a negative timeout
// waits indefinitely.
func Recv[T any](c *Context, topic string, timeout time.Duration) (T, error) {
	var zero T
	functionID, prior, err := c.prepare("recv:"+topic, KindRecv)
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
		cp := &Checkpoint{
			FunctionID:   functionID,
			FunctionName: "recv:" + topic,
			Kind:         KindRecv,
			Attempts:     1,
			StartedAt:    time.Now(),
		}
		message, created, existing, err := c.eng.store.ReceiveMessage(c.ctx, c.record.ID, c.record.Epoch, cp, c.record.ID, topic)
		if err != nil {
			return zero, err
		}
		if message != nil {
			if created {
				cp.Output = message.Payload
				cp.CompletedAt = time.Now()
				return decodeCheckpoint[T](c.eng.codec, cp)
			}
			if existing == nil {
				return zero, fmt.Errorf("pgflow: message for %s was consumed but no checkpoint exists", c.record.ID)
			}
			return decodeCheckpoint[T](c.eng.codec, existing)
		}
		if timeout >= 0 && (timeout == 0 || !time.Now().Before(deadline)) {
			cp.Err = encodeError(fmt.Errorf("%w: no message on topic %q for workflow %s", ErrTimeout, topic, c.record.ID))
			cp.CompletedAt = time.Now()
			saved, err := c.persist(cp)
			if err != nil {
				return zero, err
			}
			return decodeCheckpoint[T](c.eng.codec, saved)
		}

		wait := 200 * time.Millisecond
		if !deadline.IsZero() && time.Until(deadline) < wait {
			wait = time.Until(deadline)
		}
		if wait <= 0 {
			continue
		}
		timer := time.NewTimer(wait)
		select {
		case <-c.ctx.Done():
			timer.Stop()
			return zero, fmt.Errorf("%w: %v", ErrCancelled, c.ctx.Err())
		case <-timer.C:
		}
	}
}
