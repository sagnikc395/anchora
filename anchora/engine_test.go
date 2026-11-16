package anchora_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/anchora"
	"github.com/sagnikc395/anchora/anchora/memory"
)

const testVersion = "test-version"

// newEngine builds an engine on the given store. Workflows must be registered
// before the first Start or Launch call.
func newEngine(t *testing.T, store *memory.Store, executorID string) *anchora.Engine {
	t.Helper()
	eng, err := anchora.New(context.Background(), anchora.Config{
		Store:               store,
		AppVersion:          testVersion,
		ExecutorID:          executorID,
		Lease:               500 * time.Millisecond,
		RecoveryInterval:    25 * time.Millisecond,
		MaxRecoveryAttempts: 3,
		Logf:                t.Logf,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := eng.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return eng
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func waitFor[T any](t *testing.T, h *anchora.Handle[T]) T {
	t.Helper()
	out, err := h.Result(testContext(t))
	if err != nil {
		t.Fatalf("workflow result: %v", err)
	}
	return out
}

func waitForError[T any](t *testing.T, h *anchora.Handle[T]) error {
	t.Helper()
	_, err := h.Result(testContext(t))
	if err == nil {
		t.Fatal("workflow succeeded, want an error")
	}
	return err
}

type identified interface{ ID() string }

// crashAfter waits until the workflow has recorded at least checkpoints
// operations and then shuts the engine down, which is a clean crash: in-flight
// steps are cancelled and the workflow stays PENDING. The workflow must be
// blocked on a step that returns when its context is cancelled.
func crashAfter(t *testing.T, eng *anchora.Engine, store *memory.Store, h identified, checkpoints int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		history, err := store.LoadCheckpoints(context.Background(), h.ID())
		if err != nil {
			t.Fatalf("load checkpoints: %v", err)
		}
		if len(history) >= checkpoints {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow %s reached %d checkpoints, want %d", h.ID(), len(history), checkpoints)
		}
		time.Sleep(2 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := eng.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown engine: %v", err)
	}
}

// parked blocks until its context is cancelled or a value is available on the
// release channel, which makes a workflow stop at a known point.
func parked(release <-chan string) func(anchora.StepCtx) (string, error) {
	return func(sc anchora.StepCtx) (string, error) {
		select {
		case value := <-release:
			return value, nil
		case <-sc.Done():
			return "", sc.Err()
		}
	}
}

func TestStepCheckpointsAndReturnsOutput(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	var calls atomic.Int32

	linear := anchora.RegisterWorkflow(eng, "linear", func(ctx *anchora.Context, input string) (string, error) {
		upper, err := anchora.Step(ctx, "upper", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return strings.ToUpper(input), nil
		})
		if err != nil {
			return "", err
		}
		return anchora.Step(ctx, "bang", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return upper + "!", nil
		})
	})

	h, err := linear.Start(testContext(t), "durable")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "DURABLE!" {
		t.Fatalf("output = %q, want DURABLE!", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("step calls = %d, want 2", got)
	}
	rec, err := eng.Workflow(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load workflow: %v", err)
	}
	if rec.Status != anchora.StatusSuccess {
		t.Fatalf("status = %s, want SUCCESS", rec.Status)
	}
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("checkpoints = %d, want 2", len(history))
	}
	if history[0].Kind != anchora.KindStep || history[0].FunctionName != "upper" || history[0].Attempts != 1 {
		t.Fatalf("unexpected first checkpoint: %+v", history[0])
	}
}

func TestStartIsIdempotent(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	var calls atomic.Int32

	once := anchora.RegisterWorkflow(eng, "once", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Step(ctx, "work", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return input, nil
		})
	})
	other := anchora.RegisterWorkflow(eng, "other", func(ctx *anchora.Context, input string) (string, error) {
		return input, nil
	})

	first, err := once.Start(testContext(t), "value", anchora.WithWorkflowID("fixed-id"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, first); got != "value" {
		t.Fatalf("output = %q, want value", got)
	}

	second, err := once.Start(testContext(t), "value", anchora.WithWorkflowID("fixed-id"))
	if err != nil {
		t.Fatalf("second start: %v", err)
	}
	if second.ID() != "fixed-id" {
		t.Fatalf("id = %q, want fixed-id", second.ID())
	}
	if got := waitFor(t, second); got != "value" {
		t.Fatalf("second output = %q, want value", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("step calls = %d, want 1", got)
	}

	if _, err := other.Start(testContext(t), "value", anchora.WithWorkflowID("fixed-id")); !errors.Is(err, anchora.ErrWorkflowConflict) {
		t.Fatalf("conflicting start error = %v, want ErrWorkflowConflict", err)
	}
}

func TestReplayResumesAfterCrash(t *testing.T) {
	store := memory.New()
	engA := newEngine(t, store, "exec-a")
	release := make(chan string, 1)
	var firstCalls atomic.Int32

	implementation := func(ctx *anchora.Context, input string) (string, error) {
		first, err := anchora.Step(ctx, "first", func(sc anchora.StepCtx) (string, error) {
			firstCalls.Add(1)
			return input + "-first", nil
		})
		if err != nil {
			return "", err
		}
		second, err := anchora.Step(ctx, "second", parked(release))
		if err != nil {
			return "", err
		}
		return first + "+" + second, nil
	}

	before := anchora.RegisterWorkflow(engA, "resume", implementation)
	h, err := before.Start(testContext(t), "input", anchora.WithWorkflowID("resume-1"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	crashAfter(t, engA, store, h, 1)
	if got := firstCalls.Load(); got != 1 {
		t.Fatalf("first step calls before crash = %d, want 1", got)
	}

	store.ExpireLeases()
	release <- "second"
	engB := newEngine(t, store, "exec-b")
	after := anchora.RegisterWorkflow(engB, "resume", implementation)
	recovered, err := after.Start(testContext(t), "input", anchora.WithWorkflowID("resume-1"))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := waitFor(t, recovered); got != "input-first+second" {
		t.Fatalf("output = %q, want input-first+second", got)
	}
	if got := firstCalls.Load(); got != 1 {
		t.Fatalf("first step calls after recovery = %d, want 1 (replayed)", got)
	}
	history, err := store.LoadCheckpoints(context.Background(), "resume-1")
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("checkpoints = %d, want 2", len(history))
	}
}

func TestReplayDetectsNondeterminism(t *testing.T) {
	store := memory.New()
	engA := newEngine(t, store, "exec-a")
	release := make(chan string, 1)

	original := anchora.RegisterWorkflow(engA, "changed", func(ctx *anchora.Context, input string) (string, error) {
		if _, err := anchora.Step(ctx, "original", func(sc anchora.StepCtx) (string, error) {
			return "a", nil
		}); err != nil {
			return "", err
		}
		_, err := anchora.Step(ctx, "wait", parked(release))
		return "", err
	})
	h, err := original.Start(testContext(t), "", anchora.WithWorkflowID("changed-1"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	crashAfter(t, engA, store, h, 1)

	store.ExpireLeases()
	engB := newEngine(t, store, "exec-b")
	modified := anchora.RegisterWorkflow(engB, "changed", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Step(ctx, "renamed", func(sc anchora.StepCtx) (string, error) {
			return "b", nil
		})
	})
	recovered, err := modified.Start(testContext(t), "", anchora.WithWorkflowID("changed-1"))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	err = waitForError(t, recovered)
	if !errors.Is(err, anchora.ErrNondeterminism) {
		t.Fatalf("error = %v, want ErrNondeterminism", err)
	}
	if !strings.Contains(err.Error(), "original") || !strings.Contains(err.Error(), "renamed") {
		t.Fatalf("error should name both operations, got %v", err)
	}
	rec, err := engB.Workflow(context.Background(), "changed-1")
	if err != nil {
		t.Fatalf("load workflow: %v", err)
	}
	if rec.Status != anchora.StatusError {
		t.Fatalf("status = %s, want ERROR", rec.Status)
	}
}

func TestStepRetriesThenSucceeds(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	var calls atomic.Int32

	flaky := anchora.RegisterWorkflow(eng, "flaky", func(ctx *anchora.Context, input string) (int, error) {
		return anchora.Step(ctx, "flaky", func(sc anchora.StepCtx) (int, error) {
			attempt := calls.Add(1)
			if attempt < 3 {
				return 0, errors.New("transient")
			}
			return int(attempt), nil
		}, anchora.Retry(3, time.Millisecond, 1))
	})
	h, err := flaky.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != 3 {
		t.Fatalf("output = %d, want 3", got)
	}
	if got := calls.Load(); got != 3 {
		t.Fatalf("calls = %d, want 3", got)
	}
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if history[0].Attempts != 3 {
		t.Fatalf("recorded attempts = %d, want 3", history[0].Attempts)
	}
}

func TestStepStopsRetryingNonRetryable(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	fatal := errors.New("do not retry")
	var calls atomic.Int32

	wf := anchora.RegisterWorkflow(eng, "fatal", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Step(ctx, "fatal", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return "", fmt.Errorf("provider rejected: %w", fatal)
		}, anchora.Retry(5, 0, 1), anchora.NonRetryable(func(err error) bool {
			return errors.Is(err, fatal)
		}))
	})
	h, err := wf.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	err = waitForError(t, h)
	if !strings.Contains(err.Error(), "do not retry") {
		t.Fatalf("error = %v, want the provider error", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}

func TestTxStepCommitsOnceAcrossReplay(t *testing.T) {
	store := memory.New()
	engA := newEngine(t, store, "exec-a")
	release := make(chan struct{}, 1)
	var writes atomic.Int32

	implementation := func(ctx *anchora.Context, input string) (string, error) {
		value, err := anchora.Tx(ctx, "write", func(sc anchora.StepCtx, tx anchora.TxConn) (int32, error) {
			writes.Add(1)
			return writes.Load(), nil
		})
		if err != nil {
			return "", err
		}
		if _, err := anchora.Step(ctx, "wait", func(sc anchora.StepCtx) (string, error) {
			select {
			case <-release:
				return "done", nil
			case <-sc.Done():
				return "", sc.Err()
			}
		}); err != nil {
			return "", err
		}
		return fmt.Sprintf("writes=%d", value), nil
	}

	before := anchora.RegisterWorkflow(engA, "tx", implementation)
	h, err := before.Start(testContext(t), "", anchora.WithWorkflowID("tx-1"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	crashAfter(t, engA, store, h, 1)

	store.ExpireLeases()
	release <- struct{}{}
	engB := newEngine(t, store, "exec-b")
	after := anchora.RegisterWorkflow(engB, "tx", implementation)
	recovered, err := after.Start(testContext(t), "", anchora.WithWorkflowID("tx-1"))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := waitFor(t, recovered); got != "writes=1" {
		t.Fatalf("output = %q, want writes=1", got)
	}
	if got := writes.Load(); got != 1 {
		t.Fatalf("transaction writes = %d, want 1", got)
	}
}

func TestSendAndRecv(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	receiver := anchora.RegisterWorkflow(eng, "receiver", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Recv[string](ctx, "approval", 5*time.Second)
	})
	sender := anchora.RegisterWorkflow(eng, "sender", func(ctx *anchora.Context, input string) (string, error) {
		if err := anchora.Send(ctx, "worker-1", "approval", "yes"); err != nil {
			return "", err
		}
		return "sent", nil
	})

	waiting, err := receiver.Start(testContext(t), "", anchora.WithWorkflowID("worker-1"))
	if err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	sent, err := sender.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start sender: %v", err)
	}
	if got := waitFor(t, sent); got != "sent" {
		t.Fatalf("sender output = %q, want sent", got)
	}
	if got := waitFor(t, waiting); got != "yes" {
		t.Fatalf("receiver output = %q, want yes", got)
	}

	history, err := store.LoadCheckpoints(context.Background(), waiting.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if history[0].Kind != anchora.KindRecv {
		t.Fatalf("checkpoint kind = %s, want RECV", history[0].Kind)
	}
}

func TestRecvTimeoutIsRecorded(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	wf := anchora.RegisterWorkflow(eng, "impatient", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Recv[string](ctx, "nobody", 40*time.Millisecond)
	})
	h, err := wf.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := waitForError(t, h); !errors.Is(err, anchora.ErrTimeout) {
		t.Fatalf("error = %v, want ErrTimeout", err)
	}
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if len(history) != 1 || history[0].Err == nil {
		t.Fatalf("want one recorded timeout checkpoint, got %+v", history)
	}
}

func TestChildWorkflow(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	child := anchora.RegisterWorkflow(eng, "child", func(ctx *anchora.Context, n int) (int, error) {
		return anchora.Step(ctx, "double", func(sc anchora.StepCtx) (int, error) {
			return n * 2, nil
		})
	})
	parent := anchora.RegisterWorkflow(eng, "parent", func(ctx *anchora.Context, n int) (int, error) {
		return anchora.Child(ctx, child, n)
	})

	h, err := parent.Start(testContext(t), 21)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != 42 {
		t.Fatalf("output = %d, want 42", got)
	}
	childRecord, err := eng.Workflow(context.Background(), h.ID()+"/0")
	if err != nil {
		t.Fatalf("load child: %v", err)
	}
	if childRecord.Status != anchora.StatusSuccess {
		t.Fatalf("child status = %s, want SUCCESS", childRecord.Status)
	}
	if childRecord.ParentWorkflowID != h.ID() {
		t.Fatalf("child parent = %q, want %q", childRecord.ParentWorkflowID, h.ID())
	}
}

func TestCancelStopsWorkflow(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	started := make(chan struct{}, 1)

	wf := anchora.RegisterWorkflow(eng, "cancellable", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Step(ctx, "block", func(sc anchora.StepCtx) (string, error) {
			select {
			case started <- struct{}{}:
			default:
			}
			<-sc.Done()
			return "", sc.Err()
		})
	})
	h, err := wf.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	<-started
	if err := eng.Cancel(context.Background(), h.ID()); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if err := waitForError(t, h); !errors.Is(err, anchora.ErrCancelled) {
		t.Fatalf("error = %v, want ErrCancelled", err)
	}
	rec, err := eng.Workflow(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load workflow: %v", err)
	}
	if rec.Status != anchora.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", rec.Status)
	}
}

func TestHeartbeatTimeoutFailsStep(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	wf := anchora.RegisterWorkflow(eng, "stalled", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Step(ctx, "stalls", func(sc anchora.StepCtx) (string, error) {
			if err := sc.Heartbeat(map[string]int{"progress": 1}); err != nil {
				return "", err
			}
			<-sc.Done()
			return "", sc.Err()
		}, anchora.HeartbeatTimeout(80*time.Millisecond))
	})
	h, err := wf.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := waitForError(t, h); !errors.Is(err, anchora.ErrTimeout) {
		t.Fatalf("error = %v, want ErrTimeout", err)
	}
}

func TestSleepIsDurable(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	wf := anchora.RegisterWorkflow(eng, "sleepy", func(ctx *anchora.Context, input string) (string, error) {
		if err := anchora.Sleep(ctx, 30*time.Millisecond); err != nil {
			return "", err
		}
		return "slept", nil
	})
	started := time.Now()
	h, err := wf.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "slept" {
		t.Fatalf("output = %q, want slept", got)
	}
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond {
		t.Fatalf("elapsed = %s, want at least 30ms", elapsed)
	}
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if len(history) != 1 || history[0].Kind != anchora.KindSleep {
		t.Fatalf("want one SLEEP checkpoint, got %+v", history)
	}
}

func TestRecordedReadsReplayIdentically(t *testing.T) {
	store := memory.New()
	engA := newEngine(t, store, "exec-a")
	release := make(chan string, 1)
	capturedCh := make(chan string, 1)
	var captured string

	implementation := func(ctx *anchora.Context, input string) (string, error) {
		now, err := anchora.Now(ctx)
		if err != nil {
			return "", err
		}
		random, err := anchora.Random(ctx)
		if err != nil {
			return "", err
		}
		id, err := anchora.UUID(ctx)
		if err != nil {
			return "", err
		}
		if err := anchora.Sleep(ctx, 5*time.Millisecond); err != nil {
			return "", err
		}
		captured = fmt.Sprintf("%d|%v|%s", now.UnixNano(), random, id)
		select {
		case capturedCh <- captured:
		default:
		}
		if _, err := anchora.Step(ctx, "wait", parked(release)); err != nil {
			return "", err
		}
		return captured, nil
	}

	before := anchora.RegisterWorkflow(engA, "recorded", implementation)
	h, err := before.Start(testContext(t), "", anchora.WithWorkflowID("recorded-1"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	var expected string
	select {
	case expected = <-capturedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("workflow did not reach its recorded reads")
	}
	crashAfter(t, engA, store, h, 4)

	store.ExpireLeases()
	release <- "go"
	engB := newEngine(t, store, "exec-b")
	after := anchora.RegisterWorkflow(engB, "recorded", implementation)
	recovered, err := after.Start(testContext(t), "", anchora.WithWorkflowID("recorded-1"))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := waitFor(t, recovered); got != expected {
		t.Fatalf("replayed output = %q, want %q", got, expected)
	}
}

func TestSaveCheckpointRejectsStaleEpoch(t *testing.T) {
	ctx := testContext(t)
	store := memory.New()
	created, _, err := store.CreateWorkflow(ctx, anchora.WorkflowRecord{
		ID:         "wf",
		Name:       "workflow",
		AppVersion: testVersion,
		ExecutorID: "exec-a",
	}, time.Minute)
	if err != nil || !created {
		t.Fatalf("create workflow: created=%t err=%v", created, err)
	}
	checkpoint := &anchora.Checkpoint{FunctionID: 0, FunctionName: "step", Kind: anchora.KindStep}
	if _, _, err := store.SaveCheckpoint(ctx, "wf", 2, checkpoint); !errors.Is(err, anchora.ErrFenced) {
		t.Fatalf("stale epoch error = %v, want ErrFenced", err)
	}
	created, _, err = store.SaveCheckpoint(ctx, "wf", 1, checkpoint)
	if err != nil || !created {
		t.Fatalf("current epoch checkpoint: created=%t err=%v", created, err)
	}
}
