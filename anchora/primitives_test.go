package anchora_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/anchora"
	"github.com/sagnikc395/anchora/anchora/anchoratest"
	"github.com/sagnikc395/anchora/anchora/memory"
)

func TestGoRunsParallelSteps(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	var calls atomic.Int32

	parallel := anchora.RegisterWorkflow(eng, "parallel", func(ctx *anchora.Context, in string) (string, error) {
		a := anchora.Go(ctx, "go-a", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return in + "-a", nil
		})
		b := anchora.Go(ctx, "go-b", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return in + "-b", nil
		})
		left, err := a.Wait(testContext(t))
		if err != nil {
			return "", err
		}
		right, err := b.Wait(testContext(t))
		if err != nil {
			return "", err
		}
		return left + "|" + right, nil
	})

	h, err := parallel.Start(testContext(t), "x")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "x-a|x-b" {
		t.Fatalf("output = %q, want x-a|x-b", got)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if len(history) != 2 || history[0].FunctionName != "go-a" || history[1].FunctionName != "go-b" {
		t.Fatalf("history = %+v, want two deterministic slots go-a, go-b", history)
	}
}

func TestGoReplaysRecordedSlotWithoutRerunning(t *testing.T) {
	store := memory.New()
	engA := newEngine(t, store, "exec-a")
	release := make(chan string, 1)
	var calls atomic.Int32

	implementation := func(ctx *anchora.Context, in string) (string, error) {
		a := anchora.Go(ctx, "side", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return "effect", nil
		})
		if _, err := anchora.Step(ctx, "wait", parked(release)); err != nil {
			return "", err
		}
		return a.Wait(testContext(t))
	}

	before := anchora.RegisterWorkflow(engA, "futurist", implementation)
	h, err := before.Start(testContext(t), "", anchora.WithWorkflowID("future-1"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	crashAfter(t, engA, store, h, 1) // side recorded, the parked step never completes

	store.ExpireLeases()
	release <- "go"
	engB := newEngine(t, store, "exec-b")
	after := anchora.RegisterWorkflow(engB, "futurist", implementation)
	recovered, err := after.Start(testContext(t), "", anchora.WithWorkflowID("future-1"))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := waitFor(t, recovered); got != "effect" {
		t.Fatalf("output = %q, want the recorded effect", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("side effect calls = %d, want 1 (replay must consume the recording)", got)
	}
}

func TestSetEventIsVisibleExternallyAndReplays(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	reporter := anchora.RegisterWorkflow(eng, "reporter", func(ctx *anchora.Context, in string) (string, error) {
		if err := anchora.SetEvent(ctx, "stage", "halfway"); err != nil {
			return "", err
		}
		seen, err := anchora.GetEvent[string](ctx, "", "stage", 0)
		if err != nil {
			return "", err
		}
		return seen, nil
	})
	h, err := reporter.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "halfway" {
		t.Fatalf("output = %q, want halfway", got)
	}
	value, ok, err := eng.WorkflowEvent(context.Background(), h.ID(), "stage")
	if err != nil || !ok || string(value) != `"halfway"` {
		t.Fatalf("external event = %q ok=%t err=%v, want encoded halfway", value, ok, err)
	}
	history, err := store.LoadCheckpoints(context.Background(), h.ID())
	if err != nil {
		t.Fatalf("load checkpoints: %v", err)
	}
	if len(history) != 2 || history[0].Kind != anchora.KindSetEvent || history[1].Kind != anchora.KindGetEvent {
		t.Fatalf("history kinds = %+v, want SET_EVENT then GET_EVENT", history)
	}
}

func TestGetEventWaitsForPublisher(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	waiter := anchora.RegisterWorkflow(eng, "waiter", func(ctx *anchora.Context, in string) (string, error) {
		return anchora.GetEvent[string](ctx, "external-1", "approval", time.Second)
	})
	h, err := waiter.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := store.SetWorkflowEvent(context.Background(), "external-1", "approval", []byte(`"yes"`)); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := waitFor(t, h); got != "yes" {
		t.Fatalf("output = %q, want the published yes", got)
	}
}

func TestGetEventTimeoutIsRecorded(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	wf := anchora.RegisterWorkflow(eng, " impatient", func(ctx *anchora.Context, in string) (string, error) {
		return anchora.GetEvent[string](ctx, "nobody", "answer", 30*time.Millisecond)
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
		t.Fatalf("want one recorded timeout, got %+v", history)
	}
}

func TestStreamsAreReadableInOrder(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	streamer := anchora.RegisterWorkflow(eng, "streamer", func(ctx *anchora.Context, in string) (string, error) {
		return anchora.Step(ctx, "llm", func(sc anchora.StepCtx) (string, error) {
			for _, token := range []string{"hel", "lo", "!"} {
				if err := anchora.WriteStream(sc, "tokens", token); err != nil {
					return "", err
				}
			}
			return "hello!", nil
		})
	})
	h, err := streamer.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "hello!" {
		t.Fatalf("output = %q, want hello!", got)
	}
	chunks, err := eng.Stream(context.Background(), h.ID(), "tokens", -1, 10)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	var text strings.Builder
	for _, c := range chunks {
		var token string
		if err := json.Unmarshal(c.Chunk, &token); err != nil {
			t.Fatalf("decode chunk %q: %v", c.Chunk, err)
		}
		text.WriteString(token)
	}
	if text.String() != "hello!" {
		t.Fatalf("stream = %q, want hello!", text.String())
	}
}

func TestForkReplaysCopiedStepsAndRunsLiveOnward(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	var ones, twos atomic.Int32

	pipeline := anchora.RegisterWorkflow(eng, "pipeline", func(ctx *anchora.Context, in string) (string, error) {
		one, err := anchora.Step(ctx, "one", func(sc anchora.StepCtx) (string, error) {
			ones.Add(1)
			return in + "-one", nil
		})
		if err != nil {
			return "", err
		}
		return anchora.Step(ctx, "two", func(sc anchora.StepCtx) (string, error) {
			twos.Add(1)
			return one + "-two", nil
		})
	})
	original, err := pipeline.Start(testContext(t), "seed")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, original); got != "seed-one-two" {
		t.Fatalf("original output = %q", got)
	}

	forked, err := anchora.Fork(testContext(t), pipeline, original.ID(), 1, anchora.WithWorkflowID("fork-1"))
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if got := waitFor(t, forked); got != "seed-one-two" {
		t.Fatalf("forked output = %q, want seed-one-two", got)
	}
	// The fork replays step one from the copied history and re-runs step two.
	if got := ones.Load(); got != 1 {
		t.Fatalf("step one calls = %d, want 1 (copied, not re-run)", got)
	}
	if got := twos.Load(); got != 2 {
		t.Fatalf("step two calls = %d, want 2 (live in the fork)", got)
	}
}

func TestPatchedKeepsOldHistoryOnOldBranch(t *testing.T) {
	unpatched := func(ctx *anchora.Context, in string) (string, error) {
		return anchora.Step(ctx, "base", func(sc anchora.StepCtx) (string, error) {
			return "old", nil
		})
	}
	patched := func(ctx *anchora.Context, in string) (string, error) {
		if anchora.Patched(ctx, "v2") {
			return "new", nil
		}
		return unpatched(ctx, in)
	}

	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	orig := anchora.RegisterWorkflow(eng, "patchable", unpatched)
	h, err := orig.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "old" {
		t.Fatalf("original output = %q, want old", got)
	}
	history, err := anchoratest.History(context.Background(), store, h.ID())
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	// Replaying the old history against the patched code must keep the old
	// branch: the marker finds a step at its slot, so Patched reports false.
	out, err := anchoratest.Replay(t, patched, "patchable", history, "")
	if err != nil {
		t.Fatalf("replay old history with patched code: %v", err)
	}
	if out != "old" {
		t.Fatalf("replayed output = %q, want old", out)
	}

	// A fresh execution of the patched code records the marker and takes the
	// new branch.
	eng2 := newEngine(t, memory.New(), "exec-b")
	fresh := anchora.RegisterWorkflow(eng2, "patchable", patched)
	fh, err := fresh.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start patched: %v", err)
	}
	if got := waitFor(t, fh); got != "new" {
		t.Fatalf("patched output = %q, want new", got)
	}
}

func TestReplayDetectsDivergence(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	var calls atomic.Int32

	original := anchora.RegisterWorkflow(eng, "stable", func(ctx *anchora.Context, in string) (string, error) {
		return anchora.Step(ctx, "first", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return "A", nil
		})
	})
	h, err := original.Start(testContext(t), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "A" {
		t.Fatalf("output = %q", got)
	}
	history, err := anchoratest.History(context.Background(), store, h.ID())
	if err != nil {
		t.Fatalf("history: %v", err)
	}

	_, err = anchoratest.Replay(t, func(ctx *anchora.Context, in string) (string, error) {
		return anchora.Step(ctx, "renamed", func(sc anchora.StepCtx) (string, error) {
			return "B", nil
		})
	}, "stable", history, "")
	if !errors.Is(err, anchora.ErrNondeterminism) {
		t.Fatalf("replay error = %v, want ErrNondeterminism", err)
	}
	if !strings.Contains(err.Error(), "first") || !strings.Contains(err.Error(), "renamed") {
		t.Fatalf("error should name both operations, got %v", err)
	}

	// The happy path: the unchanged workflow replays its recordings without
	// calling any step function.
	out, err := anchoratest.Replay(t, func(ctx *anchora.Context, in string) (string, error) {
		return anchora.Step(ctx, "first", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return "A", nil
		})
	}, "stable", history, "")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if out != "A" || calls.Load() != 1 {
		t.Fatalf("replayed output = %q calls = %d, want A and no re-run", out, calls.Load())
	}
}

func TestContinueAsNewFollowsChain(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")

	var loop *anchora.Workflow[int, int]
	loop = anchora.RegisterWorkflow(eng, "cycler", func(ctx *anchora.Context, n int) (int, error) {
		if n >= 3 {
			return n, nil
		}
		return 0, anchora.ContinueAsNew(ctx, loop, n+1)
	})
	h, err := loop.Start(testContext(t), 1, anchora.WithWorkflowID("cycle-1"))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != 3 {
		t.Fatalf("output = %d, want 3 after two continuations", got)
	}
	rec, err := eng.Workflow(context.Background(), "cycle-1")
	if err != nil {
		t.Fatalf("load original: %v", err)
	}
	if rec.Status != anchora.StatusSuccess || rec.ContinuedTo == "" {
		t.Fatalf("original = %+v, want SUCCESS with a continuation", rec)
	}
	final, err := eng.Workflow(context.Background(), rec.ContinuedTo)
	if err != nil {
		t.Fatalf("load continuation: %v", err)
	}
	if final.Status != anchora.StatusSuccess {
		t.Fatalf("continuation status = %s, want SUCCESS", final.Status)
	}
}
