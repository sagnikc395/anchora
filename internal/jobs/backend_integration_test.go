package jobs

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/internal/workflow"
)

// These tests exercise the Lua scripts and SQL against real servers. They are
// skipped unless both URLs are set:
//
//	ANCHORA_TEST_DATABASE_URL=postgres://anchora:anchora@localhost:5432/anchora \
//	ANCHORA_TEST_REDIS_URL=redis://localhost:6379/1 \
//	go test ./jobs/ -run Integration
//
// The Redis database given is flushed, so point it at a scratch one.
func testQueue(t *testing.T) *Queue {
	t.Helper()
	url := os.Getenv("ANCHORA_TEST_REDIS_URL")
	if url == "" {
		t.Skip("set ANCHORA_TEST_REDIS_URL to run the Redis integration tests")
	}
	queue, err := NewQueue(context.Background(), url, "anchora:test:"+randomSuffix(t))
	if err != nil {
		t.Fatalf("connect Redis: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	return queue
}

func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("ANCHORA_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set ANCHORA_TEST_DATABASE_URL to run the PostgreSQL integration tests")
	}
	store, err := NewStore(context.Background(), url)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func randomSuffix(t *testing.T) string {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return id[:12]
}

func seedJob(t *testing.T, store *Store) *Job {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	job := &Job{
		ID: id, Status: workflow.Pending, CreatedAt: time.Now().UTC(),
		Steps:   []Step{{ID: "one", Agent: "a", Prompt: "first"}},
		Results: []workflow.StepResult{{ID: "one", Status: workflow.Pending}},
	}
	if err := store.Create(context.Background(), job); err != nil {
		t.Fatalf("create job: %v", err)
	}
	return job
}

func TestIntegrationQueueClaimAckRoundTrip(t *testing.T) {
	queue := testQueue(t)
	ctx := context.Background()

	if err := queue.Push(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	id, err := queue.Claim(ctx, "worker-1", time.Minute)
	if err != nil || id != "job-1" {
		t.Fatalf("claim = %q, %v; want job-1", id, err)
	}
	ready, inFlight, err := queue.Depth(ctx)
	if err != nil || ready != 0 || inFlight != 1 {
		t.Fatalf("depth = %d ready, %d in flight, %v; want 0/1", ready, inFlight, err)
	}
	// A second worker must not see a leased job.
	if other, err := queue.Claim(ctx, "worker-2", time.Minute); err != nil || other != "" {
		t.Fatalf("second claim = %q, %v; want empty", other, err)
	}
	if err := queue.Ack(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if ready, inFlight, err = queue.Depth(ctx); err != nil || ready != 0 || inFlight != 0 {
		t.Fatalf("depth after ack = %d/%d, %v; want 0/0", ready, inFlight, err)
	}
}

func TestIntegrationQueueReclaimsExpiredLease(t *testing.T) {
	queue := testQueue(t)
	ctx := context.Background()

	if err := queue.Push(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(ctx, "dead-worker", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	recovered, err := queue.Reclaim(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(recovered) != 1 || recovered[0] != "job-1" {
		t.Fatalf("reclaimed = %v; want [job-1]", recovered)
	}
	id, err := queue.Claim(ctx, "worker-2", time.Minute)
	if err != nil || id != "job-1" {
		t.Fatalf("claim after reclaim = %q, %v; want job-1", id, err)
	}
}

func TestIntegrationQueueExtendIsOwnerFenced(t *testing.T) {
	queue := testQueue(t)
	ctx := context.Background()

	if err := queue.Push(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(ctx, "worker-1", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	held, err := queue.Extend(ctx, "job-1", "worker-1", time.Minute)
	if err != nil || !held {
		t.Fatalf("owner extend = %t, %v; want true", held, err)
	}
	held, err = queue.Extend(ctx, "job-1", "worker-2", time.Minute)
	if err != nil || held {
		t.Fatalf("non-owner extend = %t, %v; want false", held, err)
	}
	// Once reclaimed, even the original owner must lose its lease.
	if _, err := queue.Reclaim(ctx, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(ctx, "worker-3", time.Minute); err != nil {
		t.Fatal(err)
	}
	if held, err = queue.Extend(ctx, "job-1", "worker-1", time.Minute); err != nil || held {
		t.Fatalf("stale owner extend = %t, %v; want false", held, err)
	}
}

func TestIntegrationQueueRequeue(t *testing.T) {
	queue := testQueue(t)
	ctx := context.Background()
	if err := queue.Push(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(ctx, "worker-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := queue.Requeue(ctx, "job-1"); err != nil {
		t.Fatal(err)
	}
	id, err := queue.Claim(ctx, "worker-2", time.Minute)
	if err != nil || id != "job-1" {
		t.Fatalf("claim after requeue = %q, %v; want job-1", id, err)
	}
}

func TestIntegrationStoreClaimIsExclusive(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	job := seedJob(t, store)

	attempts, claimed, err := store.ClaimJob(ctx, job.ID, "worker-1", time.Minute)
	if err != nil || !claimed || attempts != 1 {
		t.Fatalf("first claim = %d, %t, %v; want 1, true", attempts, claimed, err)
	}
	if _, claimed, err = store.ClaimJob(ctx, job.ID, "worker-2", time.Minute); err != nil || claimed {
		t.Fatalf("second claim = %t, %v; want false while the lease is live", claimed, err)
	}
	// The owner may re-claim, which is what a redelivery to the same worker does.
	if attempts, claimed, err = store.ClaimJob(ctx, job.ID, "worker-1", time.Minute); err != nil || !claimed || attempts != 2 {
		t.Fatalf("owner re-claim = %d, %t, %v; want 2, true", attempts, claimed, err)
	}
}

func TestIntegrationStoreLeaseExpiryAllowsTakeover(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	job := seedJob(t, store)

	if _, claimed, err := store.ClaimJob(ctx, job.ID, "worker-1", 10*time.Millisecond); err != nil || !claimed {
		t.Fatalf("setup claim failed: %t %v", claimed, err)
	}
	time.Sleep(60 * time.Millisecond)

	if _, claimed, err := store.ClaimJob(ctx, job.ID, "worker-2", time.Minute); err != nil || !claimed {
		t.Fatalf("takeover after lease expiry = %t, %v; want true", claimed, err)
	}
	// The evicted worker must no longer be able to write.
	held, err := store.ExtendLease(ctx, job.ID, "worker-1", time.Minute)
	if err != nil || held {
		t.Fatalf("evicted worker extend = %t, %v; want false", held, err)
	}
	if held, err = store.FinishJob(ctx, job.ID, "worker-1", workflow.Succeeded, ""); err != nil || held {
		t.Fatalf("evicted worker finish = %t, %v; want false", held, err)
	}
	fetched, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Status == workflow.Succeeded {
		t.Fatal("an evicted worker overwrote the new owner's job state")
	}
}

func TestIntegrationStoreTerminalJobsAreNotReclaimable(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	job := seedJob(t, store)

	if _, _, err := store.ClaimJob(ctx, job.ID, "worker-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	if held, err := store.FinishJob(ctx, job.ID, "worker-1", workflow.Succeeded, ""); err != nil || !held {
		t.Fatalf("finish = %t, %v; want true", held, err)
	}
	if _, claimed, err := store.ClaimJob(ctx, job.ID, "worker-2", time.Minute); err != nil || claimed {
		t.Fatalf("claim of a finished job = %t, %v; want false", claimed, err)
	}
}

func TestIntegrationStoreReclaimsExpiredJobs(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	job := seedJob(t, store)

	if _, _, err := store.ClaimJob(ctx, job.ID, "dead-worker", 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)

	// The grace period holds the sweep back until the lease is well past due.
	ids, err := store.ReclaimExpiredJobs(ctx, time.Hour, 100)
	if err != nil {
		t.Fatal(err)
	}
	if containsID(ids, job.ID) {
		t.Fatal("the grace period did not hold back the sweep")
	}
	if ids, err = store.ReclaimExpiredJobs(ctx, 0, 100); err != nil {
		t.Fatal(err)
	}
	if !containsID(ids, job.ID) {
		t.Fatalf("reclaimed = %v; want the expired job", ids)
	}
	fetched, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Status != workflow.Pending || fetched.Owner != "" {
		t.Fatalf("reclaimed job = %q owned by %q; want pending and unowned", fetched.Status, fetched.Owner)
	}
}

func TestIntegrationStoreFencesStepWrites(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	job := seedJob(t, store)

	if _, _, err := store.ClaimJob(ctx, job.ID, "worker-1", time.Minute); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimStep(ctx, job.ID, "one", "worker-1")
	if err != nil || !claimed {
		t.Fatalf("owner step claim = %t, %v; want true", claimed, err)
	}
	if claimed, err = store.ClaimStep(ctx, job.ID, "one", "worker-2"); err != nil || claimed {
		t.Fatalf("non-owner step claim = %t, %v; want false", claimed, err)
	}
	// A write from a worker that does not hold the lease must not land.
	if err := store.UpdateStep(ctx, job.ID, "worker-2", workflow.StepResult{ID: "one", Status: workflow.Succeeded, Output: "stale"}); err != nil {
		t.Fatal(err)
	}
	fetched, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Results[0].Output == "stale" {
		t.Fatal("an unfenced write from a superseded worker landed")
	}
	// The owner's write must land, and a succeeded step must then refuse a claim.
	if err := store.UpdateStep(ctx, job.ID, "worker-1", workflow.StepResult{ID: "one", Status: workflow.Succeeded, Output: "real"}); err != nil {
		t.Fatal(err)
	}
	if claimed, err = store.ClaimStep(ctx, job.ID, "one", "worker-1"); err != nil || claimed {
		t.Fatalf("claim of a succeeded step = %t, %v; want false", claimed, err)
	}
}

func TestIntegrationStoreWorkerRegistry(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	id := "worker-" + randomSuffix(t)

	if err := store.RegisterWorker(ctx, WorkerInfo{ID: id, Hostname: "host", PID: 1234, Queue: "q"}); err != nil {
		t.Fatal(err)
	}
	if err := store.HeartbeatWorker(ctx, id, 3); err != nil {
		t.Fatal(err)
	}
	workers, err := store.Workers(ctx, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, w := range workers {
		if w.ID == id {
			found = true
			if w.JobsClaimed != 3 || w.PID != 1234 {
				t.Fatalf("worker = %#v; want 3 jobs claimed and PID 1234", w)
			}
		}
	}
	if !found {
		t.Fatal("registered worker is missing from the live roster")
	}
	// A worker silent for longer than the TTL is pruned.
	if _, err := store.PruneWorkers(ctx, time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	if workers, err = store.Workers(ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	for _, w := range workers {
		if w.ID == id {
			t.Fatal("a silent worker survived pruning")
		}
	}
}

// The headline guarantee: a worker that dies mid-job loses it to another
// worker, which resumes rather than re-running the completed steps.
func TestIntegrationCrashRecoveryResumesJob(t *testing.T) {
	store, queue := testStore(t), testQueue(t)
	ctx := context.Background()
	agent := newCountingAgent()
	service := &Service{
		Store: store, Queue: queue, Agents: registry{"a": agent},
		Config: Config{Lease: 200 * time.Millisecond, Heartbeat: 50 * time.Millisecond, MaxAttempts: 5},
		Logf:   func(format string, args ...any) { t.Logf(format, args...) },
	}
	job, err := service.Submit(ctx, []Step{
		{ID: "one", Agent: "a", Prompt: "first"},
		{ID: "two", Agent: "a", Prompt: "second", DependsOn: []string{"one"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// A worker claims the job, completes the first step, then dies.
	if _, claimed, err := store.ClaimJob(ctx, job.ID, "dead-worker", 100*time.Millisecond); err != nil || !claimed {
		t.Fatalf("setup claim: %t %v", claimed, err)
	}
	if err := store.UpdateStep(ctx, job.ID, "dead-worker", workflow.StepResult{ID: "one", Status: workflow.Succeeded, Output: "out:first"}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Claim(ctx, "dead-worker", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)

	// The reaper hands it to a live worker.
	service.reap(ctx)
	id, err := queue.Claim(ctx, "live-worker", time.Minute)
	if err != nil || id != job.ID {
		t.Fatalf("claim after reap = %q, %v; want the recovered job", id, err)
	}
	service.process(ctx, "live-worker", job.ID)

	if got := agent.count("first"); got != 0 {
		t.Fatalf("the completed step re-ran %d time(s); resume did not take effect", got)
	}
	if got := agent.count("second"); got != 1 {
		t.Fatalf("the remaining step ran %d time(s); want 1", got)
	}
	fetched, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fetched.Status != workflow.Succeeded {
		t.Fatalf("recovered job status = %q (%s); want succeeded", fetched.Status, fetched.Error)
	}
}

func containsID(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
