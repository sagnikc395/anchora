package pgflow_test

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagnikc395/anchora/pgflow"
)

func countRows(t *testing.T, url, query string, args ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	var n int
	if err := pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %s: %v", query, err)
	}
	return n
}

// deadline2 is a ten-second context for integration waits.
func deadline2(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestPostgresQueueDrainsWithConcurrencyCap(t *testing.T) {
	url := postgresURL(t)
	ids := make([]string, 0, 4)
	handles := make([]*pgflow.Handle[int], 0, 4)
	for range 4 {
		ids = append(ids, uniqueID("it-queue"))
	}
	t.Cleanup(func() { cleanupWorkflows(t, url, ids...) })

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	if err := eng.RegisterQueue(testContext(t), pgflow.Queue{Name: "it-jobs", LocalConcurrency: 2}); err != nil {
		t.Fatalf("register queue: %v", err)
	}

	var running, peak atomic.Int32
	var doneCount atomic.Int32
	done := make(chan struct{})
	job := pgflow.RegisterWorkflow(eng, "queued", func(ctx *pgflow.Context, in int) (int, error) {
		now := running.Add(1)
		for {
			seen := peak.Load()
			if now <= seen || peak.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		running.Add(-1)
		if doneCount.Add(1) == int32(len(ids)) {
			close(done)
		}
		return in * 2, nil
	})
	for i, id := range ids {
		h, err := job.Start(testContext(t), i, pgflow.WithQueue("it-jobs"), pgflow.WithWorkflowID(id))
		if err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
		handles = append(handles, h)
	}
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("queue did not drain")
	}
	if got := peak.Load(); got > 2 {
		t.Fatalf("peak concurrency = %d, want at most 2", got)
	}
	for i, h := range handles {
		if got := waitFor(t, h); got != i*2 {
			t.Fatalf("job %d output = %d, want %d", i, got, i*2)
		}
	}
}

func TestPostgresEnqueueIsIdempotentAcrossRestart(t *testing.T) {
	url := postgresURL(t)
	parentID := uniqueID("it-enq-parent")
	childID := parentID + "/0"
	t.Cleanup(func() { cleanupWorkflows(t, url, parentID, childID) })

	engA := newPostgresEngine(t, url, "it-exec-a", time.Second)
	if err := engA.RegisterQueue(testContext(t), pgflow.Queue{Name: "it-agents", LocalConcurrency: 1}); err != nil {
		t.Fatalf("register queue: %v", err)
	}
	release := make(chan string, 2)

	child := pgflow.RegisterWorkflow(engA, "enqueued_child", func(ctx *pgflow.Context, in string) (string, error) {
		<-release
		return "done", nil
	})
	parent := pgflow.RegisterWorkflow(engA, "enqueueing_parent", func(ctx *pgflow.Context, in string) (string, error) {
		if err := pgflow.Enqueue(ctx, child, "work", pgflow.WithQueue("it-agents")); err != nil {
			return "", err
		}
		return pgflow.Step(ctx, "park", parked(release))
	})
	if _, err := parent.Start(testContext(t), "x", pgflow.WithWorkflowID(parentID)); err != nil {
		t.Fatalf("start parent: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := countRows(t, url, `SELECT count(*) FROM pgflow.operation_outputs WHERE workflow_id = $1`, parentID)
		if got >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("parent never recorded its enqueue")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := engA.Shutdown(deadline2(t)); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// Wait out the lease, then restart: recovery replays the parent, which
	// must reattach to the same queue row instead of enqueueing a second child.
	time.Sleep(1500 * time.Millisecond)
	engB := newPostgresEngine(t, url, "it-exec-b", time.Second)
	if err := engB.RegisterQueue(testContext(t), pgflow.Queue{Name: "it-agents", LocalConcurrency: 1}); err != nil {
		t.Fatalf("re-register queue: %v", err)
	}
	childB := pgflow.RegisterWorkflow(engB, "enqueued_child", func(ctx *pgflow.Context, in string) (string, error) {
		<-release
		return "done", nil
	})
	pgflow.RegisterWorkflow(engB, "enqueueing_parent", func(ctx *pgflow.Context, in string) (string, error) {
		if err := pgflow.Enqueue(ctx, childB, "work", pgflow.WithQueue("it-agents")); err != nil {
			return "", err
		}
		return pgflow.Step(ctx, "park", parked(release))
	})
	if err := engB.Launch(); err != nil {
		t.Fatalf("relaunch: %v", err)
	}
	release <- "go" // the parked parent
	release <- "go" // the recovered child
	final := countRows(t, url, `SELECT count(*) FROM pgflow.workflow_status WHERE parent_workflow_id = $1`, parentID)
	if final != 1 {
		t.Fatalf("children = %d, want exactly 1", final)
	}
}

func TestPostgresForkReusesRecordedSteps(t *testing.T) {
	url := postgresURL(t)
	originalID := uniqueID("it-fork")
	forkID := originalID + "-fork"
	t.Cleanup(func() { cleanupWorkflows(t, url, originalID, forkID) })

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	var ones, twos atomic.Int32
	pipeline := pgflow.RegisterWorkflow(eng, "fork_pipeline", func(ctx *pgflow.Context, in string) (string, error) {
		one, err := pgflow.Step(ctx, "one", func(sc pgflow.StepCtx) (string, error) {
			ones.Add(1)
			return in + "-one", nil
		})
		if err != nil {
			return "", err
		}
		return pgflow.Step(ctx, "two", func(sc pgflow.StepCtx) (string, error) {
			twos.Add(1)
			return one + "-two", nil
		})
	})
	original, err := pipeline.Start(testContext(t), "seed", pgflow.WithWorkflowID(originalID))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, original); got != "seed-one-two" {
		t.Fatalf("original output = %q", got)
	}

	forked, err := pgflow.Fork(testContext(t), pipeline, originalID, 1, pgflow.WithWorkflowID(forkID))
	if err != nil {
		t.Fatalf("fork: %v", err)
	}
	if got := waitFor(t, forked); got != "seed-one-two" {
		t.Fatalf("forked output = %q, want seed-one-two", got)
	}
	if got := ones.Load(); got != 1 {
		t.Fatalf("step one runs = %d, want 1 (copied, not re-run)", got)
	}
	if got := twos.Load(); got != 2 {
		t.Fatalf("step two runs = %d, want 2", got)
	}
	if got := countRows(t, url, `SELECT count(*) FROM pgflow.operation_outputs WHERE workflow_id = $1 AND function_id < 1`, forkID); got != 1 {
		t.Fatalf("copied rows = %d, want 1", got)
	}
	if rec, err := eng.Workflow(testContext(t), forkID); err != nil || rec.ForkedFrom != originalID {
		t.Fatalf("forked record = %+v err %v, want forked_from = %s", rec, err, originalID)
	}
}

func TestPostgresEventsAndStreams(t *testing.T) {
	url := postgresURL(t)
	id := uniqueID("it-events")
	t.Cleanup(func() { cleanupWorkflows(t, url, id) })

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	reporter := pgflow.RegisterWorkflow(eng, "event_reporter", func(ctx *pgflow.Context, in string) (string, error) {
		if err := pgflow.SetEvent(ctx, "stage", "halfway"); err != nil {
			return "", err
		}
		seen, err := pgflow.GetEvent[string](ctx, "", "stage", time.Second)
		if err != nil {
			return "", err
		}
		_, err = pgflow.Step(ctx, "stream", func(sc pgflow.StepCtx) (string, error) {
			for _, tok := range []string{"a", "b"} {
				if err := pgflow.WriteStream(sc, "tokens", tok); err != nil {
					return "", err
				}
			}
			return "ab", nil
		})
		if err != nil {
			return "", err
		}
		return seen, nil
	})
	h, err := reporter.Start(testContext(t), "", pgflow.WithWorkflowID(id))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != "halfway" {
		t.Fatalf("output = %q", got)
	}
	value, ok, err := eng.WorkflowEvent(testContext(t), id, "stage")
	if err != nil || !ok || !json.Valid(value) {
		t.Fatalf("event = %q ok=%t err=%v", value, ok, err)
	}
	chunks, err := eng.Stream(testContext(t), id, "tokens", -1, 10)
	if err != nil {
		t.Fatalf("stream: %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	var text string
	for _, c := range chunks {
		var tok string
		if err := json.Unmarshal(c.Chunk, &tok); err != nil {
			t.Fatalf("chunk %s: %v", c.Chunk, err)
		}
		text += tok
	}
	if text != "ab" {
		t.Fatalf("stream text = %q, want ab", text)
	}
}

func TestPostgresContinueAsNewFollowsChain(t *testing.T) {
	url := postgresURL(t)
	first := uniqueID("it-can")
	t.Cleanup(func() { cleanupWorkflows(t, url, first, first+"/0", first+"/0"+"/0") })

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	var cycler *pgflow.Workflow[int, int]
	cycler = pgflow.RegisterWorkflow(eng, "it_cycler", func(ctx *pgflow.Context, n int) (int, error) {
		if n >= 3 {
			return n, nil
		}
		return 0, pgflow.ContinueAsNew(ctx, cycler, n+1)
	})
	h, err := cycler.Start(testContext(t), 1, pgflow.WithWorkflowID(first))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if got := waitFor(t, h); got != 3 {
		t.Fatalf("output = %d, want 3", got)
	}
	rec, err := eng.Workflow(testContext(t), first)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if rec.Status != pgflow.StatusSuccess || rec.ContinuedTo == "" {
		t.Fatalf("record = %+v, want SUCCESS with a continuation", rec)
	}
	final, err := eng.Workflow(testContext(t), rec.ContinuedTo)
	if err != nil {
		t.Fatalf("load continuation: %v", err)
	}
	if final.Status != pgflow.StatusSuccess {
		t.Fatalf("continuation status = %s", final.Status)
	}
}
