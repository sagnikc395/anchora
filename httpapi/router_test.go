package httpapi_test

import (
	"context"
	"encoding/json"
	"github.com/sagnikc395/anchora"
	"github.com/sagnikc395/anchora/httpapi"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeAgent struct{}

func (fakeAgent) Run(_ context.Context, prompt string) (string, error) { return "done: " + prompt, nil }
func TestRunWorkflow(t *testing.T) {
	h := httpapi.NewRouter(httpapi.AgentRegistry{"research": fakeAgent{}}, httpapi.Options{})
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/run", strings.NewReader(`{"steps":[{"id":"research","agent":"research","prompt":"hello"}]}`))
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Steps []anchora.StepResult `json:"steps"`
	}
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Steps) != 1 || response.Steps[0].Output != "done: hello" {
		t.Fatalf("unexpected response: %#v", response)
	}
}
