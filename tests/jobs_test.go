package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/internal/httpapi"
	"github.com/sagnikc395/anchora/internal/jobs"
	"github.com/sagnikc395/anchora/internal/workflow"
)

// stubStore implements jobs.JobStore with just enough behaviour to drive the
// HTTP handlers: the worker lifecycle is covered by the jobs package's own
// tests, so everything the handlers never reach is a no-op.
type stubStore struct {
	mu      sync.Mutex
	jobs    map[string]*jobs.Job
	events  map[string][]jobs.Event
	nextID  int64
	workers []jobs.WorkerInfo
	failGet error
}

func newStubStore() *stubStore {
	return &stubStore{jobs: map[string]*jobs.Job{}, events: map[string][]jobs.Event{}}
}

func (s *stubStore) Create(_ context.Context, job *jobs.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copied := *job
	s.jobs[job.ID] = &copied
	return nil
}

func (s *stubStore) Get(_ context.Context, id string) (*jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failGet != nil {
		return nil, s.failGet
	}
	job, ok := s.jobs[id]
	if !ok {
		return nil, nil
	}
	return job, nil
}

func (s *stubStore) AppendEvent(_ context.Context, jobID, typ string, data any) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	s.events[jobID] = append(s.events[jobID], jobs.Event{ID: s.nextID, Type: typ, Data: encoded, CreatedAt: time.Now()})
	return nil
}

func (s *stubStore) Events(_ context.Context, jobID string, after int64) ([]jobs.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []jobs.Event
	for _, event := range s.events[jobID] {
		if event.ID > after {
			out = append(out, event)
		}
	}
	return out, nil
}

func (s *stubStore) Workers(context.Context, time.Duration) ([]jobs.WorkerInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers, nil
}

func (*stubStore) ClaimJob(context.Context, string, string, time.Duration) (int, bool, error) {
	return 0, false, nil
}

func (*stubStore) ExtendLease(context.Context, string, string, time.Duration) (bool, error) {
	return false, nil
}

func (*stubStore) FinishJob(context.Context, string, string, workflow.Status, string) (bool, error) {
	return false, nil
}
func (*stubStore) ReleaseJob(context.Context, string, string) error { return nil }
func (*stubStore) ReclaimExpiredJobs(context.Context, time.Duration, int) ([]string, error) {
	return nil, nil
}
func (*stubStore) ClaimStep(context.Context, string, string, string) (bool, error) { return true, nil }
func (*stubStore) UpdateStep(context.Context, string, string, workflow.StepResult) error {
	return nil
}
func (*stubStore) RegisterWorker(context.Context, jobs.WorkerInfo) error      { return nil }
func (*stubStore) HeartbeatWorker(context.Context, string, int64) error       { return nil }
func (*stubStore) UnregisterWorker(context.Context, string) error             { return nil }
func (*stubStore) PruneWorkers(context.Context, time.Duration) (int64, error) { return 0, nil }

// stubQueue implements jobs.JobQueue. Only Push and Depth are reached by the
// HTTP layer.
type stubQueue struct {
	mu       sync.Mutex
	pushed   []string
	pushErr  error
	inFlight int64
}

func (q *stubQueue) Push(_ context.Context, id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pushErr != nil {
		return q.pushErr
	}
	q.pushed = append(q.pushed, id)
	return nil
}

func (q *stubQueue) Depth(context.Context) (int64, int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return int64(len(q.pushed)), q.inFlight, nil
}

func (*stubQueue) Claim(context.Context, string, time.Duration) (string, error) { return "", nil }
func (*stubQueue) Extend(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (*stubQueue) Ack(context.Context, string) error              { return nil }
func (*stubQueue) Requeue(context.Context, string) error          { return nil }
func (*stubQueue) Reclaim(context.Context, int) ([]string, error) { return nil, nil }

func newJobRouter(t *testing.T) (http.Handler, *stubStore, *stubQueue) {
	t.Helper()
	store, queue := newStubStore(), &stubQueue{}
	agents := httpapi.AgentRegistry{"research": fakeAgent{}}
	service := &jobs.Service{
		Store:  store,
		Queue:  queue,
		Agents: agents,
		Logf:   func(format string, args ...any) { t.Logf(format, args...) },
	}
	return httpapi.NewRouterWithJobs(agents, httpapi.Options{}, service), store, queue
}

func TestSubmitJobAcceptsAndEnqueues(t *testing.T) {
	handler, store, queue := newJobRouter(t)
	recorder := post(t, handler, "/v1/jobs", `{"steps":[{"id":"a","agent":"research","prompt":"hello"}]}`)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202: %s", recorder.Code, recorder.Body.String())
	}
	var job jobs.Job
	if err := json.NewDecoder(recorder.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	if job.ID == "" || job.Status != workflow.Pending {
		t.Fatalf("unexpected job: %#v", job)
	}
	if got, _, _ := queue.Depth(context.Background()); got != 1 {
		t.Fatalf("queue depth = %d, want 1", got)
	}
	if stored, _ := store.Get(context.Background(), job.ID); stored == nil {
		t.Fatal("the job was not recorded in the store")
	}
}

func TestSubmitJobRejectsInvalidWorkflows(t *testing.T) {
	handler, _, _ := newJobRouter(t)
	for name, body := range map[string]string{
		"unknown agent": `{"steps":[{"id":"a","agent":"ghost","prompt":"hello"}]}`,
		"no steps":      `{"steps":[]}`,
		"cycle":         `{"steps":[{"id":"a","agent":"research","prompt":"p","depends_on":["a"]}]}`,
		"bad JSON":      `{"steps":`,
	} {
		t.Run(name, func(t *testing.T) {
			if recorder := post(t, handler, "/v1/jobs", body); recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

func TestGetJob(t *testing.T) {
	handler, _, _ := newJobRouter(t)
	submitted := post(t, handler, "/v1/jobs", `{"steps":[{"id":"a","agent":"research","prompt":"hello"}]}`)
	var job jobs.Job
	if err := json.NewDecoder(submitted.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}

	missing := httptest.NewRecorder()
	handler.ServeHTTP(missing, httptest.NewRequest(http.MethodGet, "/v1/jobs/nope", nil))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing job: status = %d, want 404", missing.Code)
	}
}

func TestGetJobReportsStoreFailures(t *testing.T) {
	handler, store, _ := newJobRouter(t)
	store.failGet = errors.New("database is down")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/jobs/anything", nil))
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
}

// The stream replays from Last-Event-ID and closes itself once a terminal
// event arrives, so a client is not left holding an open connection.
func TestEventStreamResumesAndClosesOnTerminalEvent(t *testing.T) {
	handler, store, _ := newJobRouter(t)
	submitted := post(t, handler, "/v1/jobs", `{"steps":[{"id":"a","agent":"research","prompt":"hello"}]}`)
	var job jobs.Job
	if err := json.NewDecoder(submitted.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := store.AppendEvent(ctx, job.ID, jobs.EventRunning, map[string]string{"worker": "w1"}); err != nil {
		t.Fatal(err)
	}
	if err := store.AppendEvent(ctx, job.ID, jobs.EventCompleted, map[string]string{"status": "succeeded"}); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
	// Event 1 is job.queued, recorded by the submission above.
	request.Header.Set("Last-Event-ID", "1")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request) // Returns because a terminal event is pending.

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}
	body := recorder.Body.String()
	if strings.Contains(body, jobs.EventQueued) {
		t.Errorf("event 1 was replayed despite Last-Event-ID: %s", body)
	}
	for _, want := range []string{"event: " + jobs.EventRunning, "event: " + jobs.EventCompleted, "id: 2", "id: 3"} {
		if !strings.Contains(body, want) {
			t.Errorf("stream is missing %q:\n%s", want, body)
		}
	}
}

func TestEventStreamRejectsBadLastEventID(t *testing.T) {
	handler, _, _ := newJobRouter(t)
	submitted := post(t, handler, "/v1/jobs", `{"steps":[{"id":"a","agent":"research","prompt":"hello"}]}`)
	var job jobs.Job
	if err := json.NewDecoder(submitted.Body).Decode(&job); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"not-a-number", "-1"} {
		request := httptest.NewRequest(http.MethodGet, "/v1/jobs/"+job.ID+"/events", nil)
		request.Header.Set("Last-Event-ID", value)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("Last-Event-ID %q: status = %d, want 400", value, recorder.Code)
		}
		// The error must be reported as JSON, not as a half-open SSE stream.
		if got := recorder.Header().Get("Content-Type"); got != "application/json" {
			t.Errorf("Last-Event-ID %q: Content-Type = %q, want application/json", value, got)
		}
	}
}

func TestEventStreamRequiresAnExistingJob(t *testing.T) {
	handler, _, _ := newJobRouter(t)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/jobs/nope/events", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", recorder.Code)
	}
}

func TestClusterReportsQueueDepthAndWorkers(t *testing.T) {
	handler, store, queue := newJobRouter(t)
	store.workers = []jobs.WorkerInfo{{ID: "node-a-1", Hostname: "node-a", PID: 7, Queue: "anchora:jobs", JobsClaimed: 3}}
	queue.inFlight = 2
	queue.pushed = []string{"x"}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/cluster", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	var stats jobs.Stats
	if err := json.NewDecoder(recorder.Body).Decode(&stats); err != nil {
		t.Fatal(err)
	}
	if stats.QueueReady != 1 || stats.QueueInFlight != 2 {
		t.Errorf("depth = %d ready / %d in flight, want 1/2", stats.QueueReady, stats.QueueInFlight)
	}
	if len(stats.Workers) != 1 || stats.Workers[0].ID != "node-a-1" {
		t.Errorf("unexpected workers: %#v", stats.Workers)
	}
}
