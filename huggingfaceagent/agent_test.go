package huggingfaceagent

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestRunUsesHuggingFaceRouter(t *testing.T) {
	t.Setenv("TEST_HF_TOKEN", "secret")
	client := &http.Client{Transport: roundTripper(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != chatCompletionsURL {
			t.Fatalf("URL = %q", req.URL)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"model":"HuggingFaceTB/SmolLM3-3B:fastest","messages":[{"role":"system","content":"Be concise."},{"role":"user","content":"Hello"}],"max_tokens":64}`
		if got := string(body); got != want {
			t.Fatalf("body = %s", got)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"choices":[{"message":{"role":"assistant","content":"Hi"}}]}`)), Header: make(http.Header)}, nil
	})}
	agent, err := New(context.Background(), Config{Name: "assistant", ModelID: "HuggingFaceTB/SmolLM3-3B:fastest", TokenEnv: "TEST_HF_TOKEN", Instruction: "Be concise.", MaxTokens: 64, HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	output, err := agent.Run(context.Background(), "Hello")
	if err != nil || output != "Hi" {
		t.Fatalf("Run() = %q, %v", output, err)
	}
}

func TestRunReturnsProviderError(t *testing.T) {
	t.Setenv("TEST_HF_TOKEN", "secret")
	client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(strings.NewReader(`{"error":{"message":"invalid token"}}`)), Header: make(http.Header)}, nil
	})}
	agent, err := New(context.Background(), Config{Name: "assistant", ModelID: "model", TokenEnv: "TEST_HF_TOKEN", HTTPClient: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Run(context.Background(), "Hello"); err == nil || !strings.Contains(err.Error(), "invalid token") {
		t.Fatalf("Run() error = %v", err)
	}
}
