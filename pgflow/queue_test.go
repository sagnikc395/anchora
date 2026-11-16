package pgflow_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/pgflow"
	"github.com/sagnikc395/anchora/pgflow/memory"
)

// TestQueueRunsEnqueuedWorkWithConcurrencyCap checks that a queue only runs
// LocalConcurrency workflows at once and drains them all.
func TestQueueRunsEnqueuedWorkWithConcurrencyCap(t *testing.T) {
	store := memory.New()
	eng := newEngine(t, store, "exec-a")
	if err := eng.RegisterQueue(testContext(t), pgflow.Queue{Name: "jobs", LocalConcurrency: 2}); err != nil {
		t.Fatalf("register queue: %v", err)
	}

	var running, peak atomic.Int32
	done := make(chan struct{})
	var doneCount atomic.Int32
	job := pgflow.RegisterWorkflow(eng, "queued_job", func(ctx *pgflow.Context, in int) (int, error) {
		now := running.Add(1)
		for {
			peakInt := peak.Load()
			if now <= peakInt || peak.CompareAndSwap(peakInt, now) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		running.Add(-1)
		if doneCount.Add(1) == 4 {
			close(done)
		}
		return in * 2, nil
	})

	var handles []*pgflow.Handle[int]
	for i := 1; i <= 4; i++ {
		h, err := job.Start(testContext(t), i, pgflow.WithQueue("jobs"), pgflow.WithWorkflowID(queuedID(i)))
		if err != nil {
			t.Fatalf("start job %d: %v", i, err)
		}
		handles = append(handles, h)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("queue did not drain its jobs")
	}
	if got := peak.Load(); got > 2 {
		t.Fatalf("peak concurrency = %d, want at most 2", got)
	}
	// Every queued workflow finished and recorded its output.
	for i, h := range handles {
		if got := waitFor(t, h); got != (i+1)*2 {
			t.Fatalf("job %d output = %d, want %d", i+1, got, (i+1)*2)
		}
	}
}

func queuedID(i int) string { return "queued-job-" + string(rune('0'+i)) }

// TestQueueHonorsRateLimit checks the per-period start cap: with a limit of
// one per hour, the second job must wait for the window to pass.
func TestQueueHonorsRateLimit(t *testing.T) {
	store := memory.New()
	clock := &clockValue{want: time.Unix(1_000_000, 0)}
	store.SetNow(func() time.Time { return clock.Load() })
	eng := newEngine(t, store, "exec-a")
	if err := eng.RegisterQueue(testContext(t), pgflow.Queue{
		Name:             "llm",
		RateLimitCount:   1,
		RateLimitPeriod:  time.Hour,
		LocalConcurrency: 4,
	}); err != nil {
		t.Fatalf("register queue: %v", err)
	}

	var started atomic.Int32
	job := pgflow.RegisterWorkflow(eng, "rate_limited_job", func(ctx *pgflow.Context, in int) (int, error) {
		started.Add(1)
		return in, nil
	})
	ctx := testContext(t)
	for i := 1; i <= 3; i++ {
		if _, err := job.Start(ctx, i, pgflow.WithQueue("llm"), pgflow.WithWorkflowID("rate-"+string(rune('0'+i)))); err != nil {
			t.Fatalf("start job %d: %v", i, err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for started.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("jobs started within the window = %d, want 1", got)
	}
	// Each window admits exactly one job: advance an hour, the next job
	// starts, the rest keep waiting.
	for want := int32(2); want <= 3; want++ {
		clock.Store(clock.Load().Add(time.Hour))
		deadline := time.Now().Add(2 * time.Second)
		for started.Load() < want && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if got := started.Load(); got != want {
			t.Fatalf("jobs started after window %d = %d, want %d", want, got, want)
		}
	}
}

// clockValue is a tiny atomic clock for the memory store's SetNow.
type clockValue struct {
	mu   sync.Mutex
	want time.Time
}

func (c *clockValue) Load() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.want
}

func (c *clockValue) Store(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.want = t
}

// TestEnqueueFromWorkflowReplaysWithoutDuplicates checks the recorded-op rule:
// recovering a parent that already enqueued a child must not enqueue twice.
func TestEnqueueFromWorkflowReplaysWithoutDuplicates(t *testing.T) {
	store := memory.New()
	engA := newEngine(t, store, "exec-a")
	if err := engA.RegisterQueue(testContext(t), pgflow.Queue{Name: "agents"}); err != nil {
		t.Fatalf("register queue: %v", err)
	}

	var childStarts atomic.Int32
	release := make(chan string, 2)
	child := pgflow.RegisterWorkflow(engA, "enqueued_child", func(ctx *pgflow.Context, in string) (string, error) {
		childStarts.Add(1)
		<-release
		return "done:" + in, nil
	})
	_ = child

	// The parent enqueues the child and then parks; the engine shuts down
	// while parked, which stands in for a crash.
	parent := pgflow.RegisterWorkflow(engA, "enqueueing_parent", func(ctx *pgflow.Context, in string) (string, error) {
		if err := pgflow.Enqueue(ctx, child, "work", pgflow.WithQueue("agents")); err != nil {
			return "", err
		}
		return pgflow.Step(ctx, "park", parked(release))
	})
	parentID := "enqueue-parent-1"
	if _, err := parent.Start(testContext(t), "x", pgflow.WithWorkflowID(parentID)); err != nil {
		t.Fatalf("start parent: %v", err)
	}
	waitForCheckpoints(t, store, parentID, 1)

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engA.Shutdown(shutdown); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Restart: recovery replays the parent from its checkpoints.
	engB := newEngine(t, store, "exec-b")
	if err := engB.RegisterQueue(testContext(t), pgflow.Queue{Name: "agents"}); err != nil {
		t.Fatalf("register queue on restart: %v", err)
	}
	restartedChild := pgflow.RegisterWorkflow(engB, "enqueued_child", func(ctx *pgflow.Context, in string) (string, error) {
		childStarts.Add(1)
		<-release
		return "done:" + in, nil
	})
	pgflow.RegisterWorkflow(engB, "enqueueing_parent", func(ctx *pgflow.Context, in string) (string, error) {
		if err := pgflow.Enqueue(ctx, restartedChild, "work", pgflow.WithQueue("agents")); err != nil {
			return "", err
		}
		return pgflow.Step(ctx, "park", parked(release))
	})
	// Nothing re-Starts the parent; its recovery must come from Launch.
	if err := engB.Launch(); err != nil {
		t.Fatalf("launch: %v", err)
	}

	store.ExpireLeases()
	release <- "go" // let the parked step finish; the parent completes
	release <- "go" // the recovered child also waits on release
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec, err := engB.Workflow(testContext(t), parentID)
		if err != nil {
			t.Fatalf("load parent: %v", err)
		}
		if rec.Status == pgflow.StatusSuccess {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("parent never finished; status %s", rec.Status)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := childStarts.Load(); got > 2 {
		// The child may start once per engine that owns it live (the first
		// engine started it, and a recovered child re-runs from its own
		// history), but the parent must never enqueue a second child.
		t.Fatalf("child started %d times", got)
	}
	history, err := store.LoadCheckpoints(context.Background(), parentID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 {
		t.Fatalf("parent history = %d checkpoints, want 2 (enqueue + park)", len(history))
	}
	if cp := history[0]; cp.Kind != pgflow.KindEnqueue || cp.ChildWorkflowID != parentID+"/0" {
		t.Fatalf("enqueue checkpoint = %+v", cp)
	}
}

// waitForCheckpoints polls until the workflow recorded n operations.
func waitForCheckpoints(t *testing.T, store *memory.Store, workflowID string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		history, err := store.LoadCheckpoints(context.Background(), workflowID)
		if err != nil {
			t.Fatalf("load checkpoints: %v", err)
		}
		if len(history) >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow %s reached %d checkpoints, want %d", workflowID, len(history), n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
