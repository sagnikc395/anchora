package einoagent

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// respondWith builds a client whose transport always returns body, so the
// adapter's handling of a completion can be checked without a provider.
func respondWith(body string) *http.Client {
	return &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader(body)),
			Header:     make(http.Header),
		}, nil
	})}
}

func TestRunUsesEinoOpenAICompatibleModel(t *testing.T) {
	t.Setenv("TEST_HF_TOKEN", "secret")
	client := &http.Client{Transport: roundTripper(func(req *http.Request) (*http.Response, error) {
		if req.URL.String() != defaultBaseURL+"/chat/completions" {
			t.Fatalf("URL = %q", req.URL)
		}
		if got := req.Header.Get("Authorization"); got != "Bearer secret" {
			t.Fatalf("Authorization = %q", got)
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		got := string(body)
		for _, want := range []string{`"model":"HuggingFaceTB/SmolLM3-3B:fastest"`, `"role":"system"`, `"content":"Be concise."`, `"role":"user"`, `"content":"Hello"`, `"max_tokens":64`} {
			if !strings.Contains(got, want) {
				t.Fatalf("request body %s does not contain %s", got, want)
			}
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

func TestNewRejectsIncompleteConfig(t *testing.T) {
	t.Setenv("TEST_HF_TOKEN", "secret")
	tests := map[string]Config{
		"no name":     {ModelID: "m", TokenEnv: "TEST_HF_TOKEN"},
		"no model ID": {Name: "a", TokenEnv: "TEST_HF_TOKEN"},
	}
	for name, config := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(context.Background(), config); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

// A missing token must fail at construction, so a misconfigured agent is
// caught at startup rather than on the first workflow step.
func TestNewRequiresTheConfiguredToken(t *testing.T) {
	t.Setenv("TEST_HF_TOKEN", "")
	_, err := New(context.Background(), Config{Name: "a", ModelID: "m", TokenEnv: "TEST_HF_TOKEN"})
	if err == nil {
		t.Fatal("expected an error when the token variable is unset")
	}
	if !strings.Contains(err.Error(), "TEST_HF_TOKEN") {
		t.Fatalf("error = %v, want it to name the missing variable", err)
	}
}

func TestNewDefaultsToHuggingFaceToken(t *testing.T) {
	t.Setenv("HF_TOKEN", "secret")
	if _, err := New(context.Background(), Config{Name: "a", ModelID: "m"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// A completion with no text cannot be passed to a downstream step, so it is an
// error rather than an empty success.
func TestRunRejectsEmptyCompletion(t *testing.T) {
	t.Setenv("TEST_HF_TOKEN", "secret")
	for name, body := range map[string]string{
		"empty content":      `{"choices":[{"message":{"role":"assistant","content":""}}]}`,
		"whitespace content": `{"choices":[{"message":{"role":"assistant","content":"  \n "}}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			agent, err := New(context.Background(), Config{
				Name: "a", ModelID: "m", TokenEnv: "TEST_HF_TOKEN", HTTPClient: respondWith(body),
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := agent.Run(context.Background(), "Hello"); err == nil {
				t.Fatal("expected an error for a completion with no text")
			}
		})
	}
}

func TestRunOnUninitializedAgent(t *testing.T) {
	var agent *Agent
	if _, err := agent.Run(context.Background(), "Hello"); err == nil {
		t.Fatal("expected an error from a nil agent")
	}
	if _, err := (&Agent{}).Run(context.Background(), "Hello"); err == nil {
		t.Fatal("expected an error from an agent with no model")
	}
}
