package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sagnikc395/anchora/internal/httpapi"
	"github.com/sagnikc395/anchora/internal/workflow"
)

type fakeAgent struct{}

func (fakeAgent) Run(_ context.Context, prompt string) (string, error) { return "done: " + prompt, nil }

type failingAgent struct{}

func (failingAgent) Run(context.Context, string) (string, error) {
	return "", errors.New("provider unavailable")
}

func newRouter(agents httpapi.AgentRegistry) http.Handler {
	return httpapi.NewRouter(agents, httpapi.Options{})
}

func post(t *testing.T, handler http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return recorder
}

func TestRunWorkflow(t *testing.T) {
	handler := newRouter(httpapi.AgentRegistry{"research": fakeAgent{}})
	recorder := post(t, handler, "/v1/workflows/run", `{"steps":[{"id":"research","agent":"research","prompt":"hello"}]}`)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Steps []workflow.StepResult `json:"steps"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Steps) != 1 || response.Steps[0].Output != "done: hello" {
		t.Fatalf("unexpected response: %#v", response)
	}
}

// A failing step is a bad gateway, and the partial results still come back so
// a caller can see how far the workflow got.
func TestRunWorkflowReturnsPartialResultsOnStepFailure(t *testing.T) {
	handler := newRouter(httpapi.AgentRegistry{"ok": fakeAgent{}, "bad": failingAgent{}})
	body := `{"steps":[
		{"id":"first","agent":"ok","prompt":"hello"},
		{"id":"second","agent":"bad","prompt":"use {{steps.first.output}}","depends_on":["first"]}
	]}`
	recorder := post(t, handler, "/v1/workflows/run", body)
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var response struct {
		Steps []workflow.StepResult `json:"steps"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Steps) != 2 {
		t.Fatalf("got %d results, want 2: %#v", len(response.Steps), response.Steps)
	}
	if response.Steps[0].Status != workflow.Succeeded {
		t.Errorf("first: status = %q, want %q", response.Steps[0].Status, workflow.Succeeded)
	}
	if response.Steps[1].Status != workflow.Failed {
		t.Errorf("second: status = %q, want %q", response.Steps[1].Status, workflow.Failed)
	}
}

func TestRunWorkflowRejectsBadRequests(t *testing.T) {
	handler := newRouter(httpapi.AgentRegistry{"research": fakeAgent{}})
	tests := map[string]string{
		"malformed JSON":  `{"steps":`,
		"unknown agent":   `{"steps":[{"id":"a","agent":"ghost","prompt":"hello"}]}`,
		"unknown field":   `{"steps":[{"id":"a","agent":"research","prompt":"hello","retries":3}]}`,
		"no steps":        `{"steps":[]}`,
		"missing prompt":  `{"steps":[{"id":"a","agent":"research"}]}`,
		"dependency loop": `{"steps":[{"id":"a","agent":"research","prompt":"p","depends_on":["a"]}]}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if recorder := post(t, handler, "/v1/workflows/run", body); recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d: %s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	recorder := httptest.NewRecorder()
	newRouter(nil).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if got := strings.TrimSpace(recorder.Body.String()); got != `{"status":"ok"}` {
		t.Fatalf("body = %s", got)
	}
}

// Without a job backend the durable routes must not exist at all, rather than
// reporting a confusing server error.
func TestJobRoutesAbsentWithoutBackend(t *testing.T) {
	handler := newRouter(httpapi.AgentRegistry{"research": fakeAgent{}})
	for _, path := range []string{"/v1/jobs/abc", "/v1/cluster", "/v1/jobs/abc/events"} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusNotFound {
			t.Errorf("GET %s: status = %d, want 404", path, recorder.Code)
		}
	}
	if recorder := post(t, handler, "/v1/jobs", `{"steps":[]}`); recorder.Code != http.StatusNotFound {
		t.Errorf("POST /v1/jobs: status = %d, want 404", recorder.Code)
	}
}
