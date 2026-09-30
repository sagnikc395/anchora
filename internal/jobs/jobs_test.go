package jobs

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/internal/workflow"
)

// memStore is an in-memory JobStore with the same lease and fencing rules as
// the PostgreSQL implementation, so the worker semantics can be tested without
// standing up a database.
type memStore struct {
	mu      sync.Mutex
	jobs    map[string]*memJob
	events  map[string][]Event
	workers map[string]WorkerInfo
	nextID  int64
	// onExtend is consulted before each ExtendLease; returning false simulates
	// the lease having been reclaimed by another worker.
	onExtend func(call int) bool
	extends  int
	// refuseSteps simulates a step claim losing a race with the new owner.
	refuseSteps bool
}

type memJob struct {
	job        *Job
	owner      string
	leaseUntil time.Time
	attempts   int
}

func newMemStore() *memStore {
	return &memStore{jobs: map[string]*memJob{}, events: map[string][]Event{}, workers: map[string]WorkerInfo{}}
}

func (m *memStore) Create(_ context.Context, job *Job) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	clone := *job
	m.jobs[job.ID] = &memJob{job: &clone}
	return nil
}

func (m *memStore) Get(_ context.Context, id string) (*Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.jobs[id]
	if !ok {
		return nil, nil
	}
	clone := *entry.job
	clone.Results = append([]workflow.StepResult(nil), entry.job.Results...)
	clone.Steps = append([]Step(nil), entry.job.Steps...)
	clone.Owner, clone.Attempts = entry.owner, entry.attempts
	return &clone, nil
}

func (m *memStore) ClaimJob(_ context.Context, id, owner string, lease time.Duration) (int, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.jobs[id]
	if !ok {
		return 0, false, nil
	}
	if entry.job.Status == workflow.Succeeded || entry.job.Status == workflow.Failed {
		return 0, false, nil
	}
	if entry.owner != "" && entry.owner != owner && time.Now().Before(entry.leaseUntil) {
		return 0, false, nil
	}
	entry.attempts++
	entry.owner, entry.leaseUntil = owner, time.Now().Add(lease)
	entry.job.Status = workflow.Running
	return entry.attempts, true, nil
}

func (m *memStore) ExtendLease(_ context.Context, id, owner string, lease time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extends++
	if m.onExtend != nil && !m.onExtend(m.extends) {
		if entry, ok := m.jobs[id]; ok {
			entry.owner = "someone-else"
		}
		return false, nil
	}
	entry, ok := m.jobs[id]
	if !ok || entry.owner != owner {
		return false, nil
	}
	entry.leaseUntil = time.Now().Add(lease)
	return true, nil
}

func (m *memStore) FinishJob(_ context.Context, id, owner string, status workflow.Status, jobErr string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.jobs[id]
	if !ok || entry.owner != owner {
		return false, nil
	}
	entry.job.Status, entry.job.Error = status, jobErr
	entry.owner = ""
	return true, nil
}

func (m *memStore) ReleaseJob(_ context.Context, id, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry, ok := m.jobs[id]; ok && entry.owner == owner {
		entry.owner, entry.job.Status = "", workflow.Pending
	}
	return nil
}

func (m *memStore) ReclaimExpiredJobs(_ context.Context, grace time.Duration, limit int) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ids []string
	for id, entry := range m.jobs {
		if entry.job.Status != workflow.Running || entry.leaseUntil.IsZero() {
			continue
		}
		if time.Now().After(entry.leaseUntil.Add(grace)) && len(ids) < limit {
			entry.owner, entry.job.Status, entry.leaseUntil = "", workflow.Pending, time.Time{}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func (m *memStore) ClaimStep(_ context.Context, jobID, stepID, owner string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.jobs[jobID]
	if !ok || entry.owner != owner || m.refuseSteps {
		return false, nil
	}
	for _, result := range entry.job.Results {
		if result.ID == stepID && result.Status == workflow.Succeeded {
			return false, nil
		}
	}
	return true, nil
}

func (m *memStore) UpdateStep(_ context.Context, jobID, owner string, result workflow.StepResult) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	entry, ok := m.jobs[jobID]
	if !ok || (owner != "" && entry.owner != owner) {
		return nil
	}
	for i, existing := range entry.job.Results {
		if existing.ID == result.ID {
			entry.job.Results[i] = result
		}
	}
	return nil
}

func (m *memStore) AppendEvent(_ context.Context, jobID, typ string, _ any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nextID++
	m.events[jobID] = append(m.events[jobID], Event{ID: m.nextID, Type: typ})
	return nil
}

func (m *memStore) Events(_ context.Context, jobID string, after int64) ([]Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Event
	for _, e := range m.events[jobID] {
		if e.ID > after {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *memStore) eventTypes(jobID string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	types := make([]string, 0, len(m.events[jobID]))
	for _, e := range m.events[jobID] {
		types = append(types, e.Type)
	}
	return types
}

func (m *memStore) status(jobID string) workflow.Status {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.jobs[jobID].job.Status
}

func (m *memStore) RegisterWorker(_ context.Context, w WorkerInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers[w.ID] = w
	return nil
}
func (m *memStore) HeartbeatWorker(context.Context, string, int64) error { return nil }

func (m *memStore) UnregisterWorker(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.workers, id)
	return nil
}
func (m *memStore) PruneWorkers(context.Context, time.Duration) (int64, error) { return 0, nil }

func (m *memStore) Workers(context.Context, time.Duration) ([]WorkerInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]WorkerInfo, 0, len(m.workers))
	for _, w := range m.workers {
		out = append(out, w)
	}
	return out, nil
}

// memQueue records the dispatch calls a test cares about.
type memQueue struct {
	mu                      sync.Mutex
	ready                   []string
	acked, requeued, pushed []string
	inFlight                map[string]string
	extendOK                bool
	reclaimed               []string
}

func newMemQueue() *memQueue {
	return &memQueue{inFlight: map[string]string{}, extendOK: true}
}

func (q *memQueue) Push(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.ready, q.pushed = append(q.ready, id), append(q.pushed, id)
	return nil
}
func (q *memQueue) Claim(_ context.Context, owner string, _ time.Duration) (string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.ready) == 0 {
		return "", nil
	}
	id := q.ready[0]
	q.ready = q.ready[1:]
	q.inFlight[id] = owner
	return id, nil
}
func (q *memQueue) Extend(_ context.Context, _, _ string, _ time.Duration) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.extendOK, nil
}
func (q *memQueue) Ack(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, id)
	q.acked = append(q.acked, id)
	return nil
}
func (q *memQueue) Requeue(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.inFlight, id)
	q.ready, q.requeued = append(q.ready, id), append(q.requeued, id)
	return nil
}
func (q *memQueue) Reclaim(context.Context, int) ([]string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.reclaimed, nil
}
func (q *memQueue) Depth(context.Context) (int64, int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.ready)), int64(len(q.inFlight)), nil
}
func (q *memQueue) snapshot(field *[]string) []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), *field...)
}

type countingAgent struct {
	mu     sync.Mutex
	calls  map[string]int
	block  chan struct{}
	fail   map[string]bool
	prefix string
}

func newCountingAgent() *countingAgent {
	return &countingAgent{calls: map[string]int{}, fail: map[string]bool{}, prefix: "out:"}
}

func (a *countingAgent) Run(ctx context.Context, prompt string) (string, error) {
	a.mu.Lock()
	a.calls[prompt]++
	block, fail := a.block, a.fail[prompt]
	a.mu.Unlock()
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if fail {
		return "", errors.New("agent failed")
	}
	return a.prefix + prompt, nil
}

func (a *countingAgent) count(prompt string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.calls[prompt]
}

type registry map[string]workflow.Agent

func (r registry) Resolve(name string) (workflow.Agent, bool) {
	agent, ok := r[name]
	return agent, ok
}

func newService(t *testing.T, store *memStore, queue *memQueue, agent workflow.Agent) *Service {
	t.Helper()
	return &Service{
		Store:   store,
		Queue:   queue,
		Agents:  registry{"a": agent},
		Options: workflow.Options{},
		Config:  Config{Lease: 2 * time.Second, Heartbeat: 20 * time.Millisecond, MaxAttempts: 3},
		Logf:    func(format string, args ...any) { t.Logf(format, args...) },
	}
}

func seed(t *testing.T, service *Service, steps []Step) *Job {
	t.Helper()
	job, err := service.Submit(context.Background(), steps)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	return job
}

func TestProcessRunsAndAcksJob(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{
		{ID: "one", Agent: "a", Prompt: "first"},
		{ID: "two", Agent: "a", Prompt: "{{steps.one.output}} then", DependsOn: []string{"one"}},
	})

	service.process(context.Background(), "worker-1", job.ID)

	if got := store.status(job.ID); got != workflow.Succeeded {
		t.Fatalf("status = %q, want succeeded", got)
	}
	if acked := queue.snapshot(&queue.acked); len(acked) != 1 || acked[0] != job.ID {
		t.Fatalf("acked = %v, want [%s]", acked, job.ID)
	}
	if agent.count("out:first then") != 1 {
		t.Fatalf("second step did not receive the first step's output: %v", agent.calls)
	}
}

// A crashed worker's job is redelivered. Steps that already succeeded must be
// replayed from the store rather than sent to an agent a second time.
func TestProcessResumesWithoutRerunningCompletedSteps(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{
		{ID: "one", Agent: "a", Prompt: "first"},
		{ID: "two", Agent: "a", Prompt: "second", DependsOn: []string{"one"}},
	})
	// Simulate the first delivery having completed step "one" before dying.
	store.mu.Lock()
	store.jobs[job.ID].job.Results[0] = workflow.StepResult{ID: "one", Status: workflow.Succeeded, Output: "out:first"}
	store.mu.Unlock()

	service.process(context.Background(), "worker-2", job.ID)

	if got := agent.count("first"); got != 0 {
		t.Fatalf("completed step re-ran %d time(s), want 0", got)
	}
	if got := agent.count("second"); got != 1 {
		t.Fatalf("remaining step ran %d time(s), want 1", got)
	}
	if got := store.status(job.ID); got != workflow.Succeeded {
		t.Fatalf("status = %q, want succeeded", got)
	}
	if !contains(store.eventTypes(job.ID), "job.resumed") {
		t.Fatalf("expected a job.resumed event, got %v", store.eventTypes(job.ID))
	}
}

// At-least-once dispatch means the same job can arrive twice. The second
// arrival must be dropped, not executed.
func TestProcessDropsDuplicateDelivery(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})

	// A live worker already holds the lease.
	if _, claimed, err := store.ClaimJob(context.Background(), job.ID, "worker-1", time.Minute); err != nil || !claimed {
		t.Fatalf("setup claim failed: claimed=%t err=%v", claimed, err)
	}

	service.process(context.Background(), "worker-2", job.ID)

	if got := agent.count("first"); got != 0 {
		t.Fatalf("duplicate delivery executed the job %d time(s), want 0", got)
	}
	if acked := queue.snapshot(&queue.acked); len(acked) != 1 {
		t.Fatalf("duplicate delivery should be acked once, got %v", acked)
	}
}

func TestProcessDeadLettersAfterMaxAttempts(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	service.Config.MaxAttempts = 2
	job := seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})
	store.mu.Lock()
	store.jobs[job.ID].attempts = 2 // the next claim is attempt 3
	store.mu.Unlock()

	service.process(context.Background(), "worker-1", job.ID)

	if got := store.status(job.ID); got != workflow.Failed {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := agent.count("first"); got != 0 {
		t.Fatalf("dead-lettered job still ran the agent %d time(s)", got)
	}
	if !contains(store.eventTypes(job.ID), "job.dead_lettered") {
		t.Fatalf("expected a job.dead_lettered event, got %v", store.eventTypes(job.ID))
	}
}

// Losing the lease mid-run must cancel the work and leave the job for its new
// owner, not record a bogus failure.
func TestProcessAbandonsJobWhenLeaseIsLost(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	agent.block = make(chan struct{}) // hold the agent until the lease is pulled
	store.onExtend = func(int) bool { return false }
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})

	done := make(chan struct{})
	go func() {
		defer close(done)
		service.process(context.Background(), "worker-1", job.ID)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not return after losing its lease")
	}

	if got := store.status(job.ID); got == workflow.Succeeded || got == workflow.Failed {
		t.Fatalf("status = %q, want a non-terminal state after losing the lease", got)
	}
	if acked := queue.snapshot(&queue.acked); len(acked) != 0 {
		t.Fatalf("a worker that lost its lease must not ack, got %v", acked)
	}
	if !contains(store.eventTypes(job.ID), "job.abandoned") {
		t.Fatalf("expected a job.abandoned event, got %v", store.eventTypes(job.ID))
	}
}

// A shutdown mid-job releases the claim so the job is retried immediately
// instead of waiting out the full visibility timeout.
func TestProcessRequeuesOnShutdown(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	agent.block = make(chan struct{})
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		service.process(ctx, "worker-1", job.ID)
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("process did not return after shutdown")
	}
	if requeued := queue.snapshot(&queue.requeued); len(requeued) != 1 || requeued[0] != job.ID {
		t.Fatalf("requeued = %v, want [%s]", requeued, job.ID)
	}
	if got := store.status(job.ID); got != workflow.Pending {
		t.Fatalf("status = %q, want pending so another worker retries", got)
	}
}

func TestProcessFailsJobWhenAStepFails(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	agent.fail["first"] = true
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{
		{ID: "one", Agent: "a", Prompt: "first"},
		{ID: "two", Agent: "a", Prompt: "second", DependsOn: []string{"one"}},
	})

	service.process(context.Background(), "worker-1", job.ID)

	if got := store.status(job.ID); got != workflow.Failed {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := agent.count("second"); got != 0 {
		t.Fatalf("dependent step ran despite a failed dependency")
	}
	// A genuine workflow failure is terminal: the delivery is acked, not retried.
	if acked := queue.snapshot(&queue.acked); len(acked) != 1 {
		t.Fatalf("acked = %v, want the failed job acked once", acked)
	}
}

// The store reaper covers jobs whose queue entry vanished entirely.
func TestReaperReEnqueuesOrphanedJobs(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	service.Config.Lease = time.Millisecond
	job := seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})
	if _, _, err := store.ClaimJob(context.Background(), job.ID, "dead-worker", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	queue.mu.Lock()
	queue.ready, queue.pushed = nil, nil // the queue entry is gone
	queue.mu.Unlock()
	time.Sleep(20 * time.Millisecond)

	service.reap(context.Background())

	if pushed := queue.snapshot(&queue.pushed); len(pushed) != 1 || pushed[0] != job.ID {
		t.Fatalf("pushed = %v, want the orphaned job re-enqueued", pushed)
	}
	if got := store.status(job.ID); got != workflow.Pending {
		t.Fatalf("status = %q, want pending", got)
	}
}

// A refused step claim means another worker owns the job. That is a lease
// loss, not a workflow failure: the job must not be marked failed.
func TestProcessTreatsRefusedStepClaimAsLeaseLoss(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	job := seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})
	store.mu.Lock()
	store.refuseSteps = true
	store.mu.Unlock()

	service.process(context.Background(), "worker-1", job.ID)

	if got := store.status(job.ID); got == workflow.Failed {
		t.Fatal("a refused step claim must not fail the job; the new owner is still running it")
	}
	if got := agent.count("first"); got != 0 {
		t.Fatalf("the agent ran %d time(s) despite the claim being refused", got)
	}
	if acked := queue.snapshot(&queue.acked); len(acked) != 0 {
		t.Fatalf("acked = %v, want no ack after losing the lease", acked)
	}
	if !contains(store.eventTypes(job.ID), "job.abandoned") {
		t.Fatalf("expected a job.abandoned event, got %v", store.eventTypes(job.ID))
	}
}

func TestSubmitRejectsUnknownAgent(t *testing.T) {
	service := newService(t, newMemStore(), newMemQueue(), newCountingAgent())
	if _, err := service.Submit(context.Background(), []Step{{ID: "one", Agent: "missing", Prompt: "x"}}); err == nil {
		t.Fatal("expected an unknown-agent error")
	}
}

func TestStatsReportsQueueAndWorkers(t *testing.T) {
	store, queue, agent := newMemStore(), newMemQueue(), newCountingAgent()
	service := newService(t, store, queue, agent)
	seed(t, service, []Step{{ID: "one", Agent: "a", Prompt: "first"}})
	if err := store.RegisterWorker(context.Background(), WorkerInfo{ID: "worker-1"}); err != nil {
		t.Fatal(err)
	}
	stats, err := service.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if stats.QueueReady != 1 || len(stats.Workers) != 1 {
		t.Fatalf("stats = %#v", stats)
	}
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
