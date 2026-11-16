package pgflow_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/pgflow"
	"github.com/sagnikc395/anchora/pgflow/memory"
)

// BenchmarkCheckpointOverhead compares a recorded step against the same
// function called directly: the per-step cost of durability.
func BenchmarkCheckpointOverhead(b *testing.B) {
	eng, err := pgflow.New(context.Background(), pgflow.Config{
		Store:      memory.New(),
		AppVersion: "bench",
		ExecutorID: "bench",
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Shutdown(ctx)
	}()

	steps := pgflow.RegisterWorkflow(eng, "bench_steps", func(ctx *pgflow.Context, n int) (int, error) {
		total := 0
		for i := 0; i < n; i++ {
			next, err := pgflow.Step(ctx, "inc", func(sc pgflow.StepCtx) (int, error) {
				return total + 1, nil
			})
			if err != nil {
				return 0, err
			}
			total = next
		}
		return total, nil
	})

	b.Run("plain-function", func(b *testing.B) {
		add := func(n int) (int, error) { return n + 1, nil }
		for i := 0; i < b.N; i++ {
			if _, err := add(1); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("pgflow-step", func(b *testing.B) {
		if b.N == 0 {
			b.Skip("no iterations")
		}
		h, err := steps.Start(context.Background(), b.N)
		if err != nil {
			b.Fatal(err)
		}
		if got, err := h.Result(context.Background()); err != nil || got != b.N {
			b.Fatalf("result = %d, %v", got, err)
		}
	})
}

// BenchmarkQueuedWorkflows measures how many queue-backed jobs one process
// drains per second at a local concurrency of 8.
func BenchmarkQueuedWorkflows(b *testing.B) {
	if b.N == 0 {
		b.Skip("no iterations")
	}
	store := memory.New()
	eng, err := pgflow.New(context.Background(), pgflow.Config{
		Store:             store,
		AppVersion:        "bench",
		ExecutorID:        "bench",
		QueuePollInterval: 5 * time.Millisecond,
		Lease:             30 * time.Second,
		RecoveryInterval:  200 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = eng.Shutdown(ctx)
	}()
	if err := eng.RegisterQueue(context.Background(), pgflow.Queue{Name: "bench", LocalConcurrency: 8}); err != nil {
		b.Fatal(err)
	}

	// A fresh id prefix per run so earlier benchmark rounds cannot satisfy it.
	prefix := "bench-job-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var finished, want atomic.Int64
	want.Store(int64(b.N))
	drain := make(chan struct{})
	job := pgflow.RegisterWorkflow(eng, "bench_job", func(ctx *pgflow.Context, in int) (int, error) {
		if finished.Add(1) == want.Load() {
			close(drain)
		}
		return in, nil
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := job.Start(context.Background(), i, pgflow.WithQueue("bench"), pgflow.WithWorkflowID(prefix+fmt.Sprint(i))); err != nil {
			b.Fatal(err)
		}
	}
	select {
	case <-drain:
	case <-time.After(60 * time.Second):
		b.Fatalf("drained %d of %d", finished.Load(), b.N)
	}
}
