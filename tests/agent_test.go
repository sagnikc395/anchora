package tests

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/sagnikc395/anchora/lib/remoteagent"
)

func newTestAgent(t *testing.T, handler http.HandlerFunc) *remoteagent.Agent {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	agent, err := remoteagent.New(remoteagent.Config{Name: "test", URL: server.URL, Instruction: "be terse"})
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func TestAgentSendsPromptAndReturnsOutput(t *testing.T) {
	agent := newTestAgent(t, func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Prompt      string `json:"prompt"`
			Instruction string `json:"instruction"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if request.Prompt != "hello" || request.Instruction != "be terse" {
			t.Errorf("request = %+v, want the prompt and instruction", request)
		}
		_, _ = w.Write([]byte(`{"output":"done: hello"}`))
	})
	output, err := agent.Run(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if output != "done: hello" {
		t.Errorf("output = %q, want %q", output, "done: hello")
	}
}

func TestAgentSurfacesServiceErrors(t *testing.T) {
	agent := newTestAgent(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"detail":"model returned no text"}`))
	})
	_, err := agent.Run(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "model returned no text") {
		t.Fatalf("err = %v, want the service's detail", err)
	}
}

func TestAgentRejectsEmptyOutput(t *testing.T) {
	agent := newTestAgent(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"output":""}`))
	})
	if _, err := agent.Run(context.Background(), "hello"); err == nil {
		t.Fatal("expected an error for an empty completion")
	}
}

func TestAgentNewRejectsInvalidConfig(t *testing.T) {
	if _, err := remoteagent.New(remoteagent.Config{URL: "http://localhost:8000"}); err == nil {
		t.Fatal("expected an error for a missing name")
	}
	if _, err := remoteagent.New(remoteagent.Config{Name: "research"}); err == nil {
		t.Fatal("expected an error for a missing URL")
	}
	if _, err := remoteagent.New(remoteagent.Config{Name: "research", URL: "localhost:8000"}); err == nil {
		t.Fatal("expected an error for a URL with no scheme")
	}
}
