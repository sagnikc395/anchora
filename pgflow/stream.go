package pgflow

import (
	"context"
	"errors"
	"fmt"
)

// WriteStream appends one chunk to a workflow's append-only stream, such as
// LLM token output or an agent trace, for external callers to tail through
// Engine.Stream (or SQL).
//
// Streams are a live side channel, not checkpoints: call WriteStream inside a
// step that streams (the step's final value becomes the durable record), and
// only while the step is running. A replay does not hide what an earlier
// execution streamed.
func WriteStream(sc StepCtx, key string, value any) error {
	ctx, ok := sc.(*stepContext)
	if !ok {
		return errors.New("pgflow: WriteStream requires the engine's StepCtx")
	}
	if key == "" {
		return errors.New("pgflow: stream key is required")
	}
	encoded, err := ctx.eng.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("pgflow: encode stream chunk: %w", err)
	}
	_, err = ctx.eng.store.AppendStream(sc, ctx.workflowID, key, encoded)
	return err
}

// Stream returns a stream's chunks after a sequence number, in order, for
// operator tooling or a read-only UI.
func (e *Engine) Stream(ctx context.Context, workflowID, key string, afterSeq int64, limit int) ([]StreamChunk, error) {
	return e.store.ReadStreams(ctx, workflowID, key, afterSeq, limit)
}
