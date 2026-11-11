// Package remoteagent adapts the Python agent service to workflow.Agent.
//
// The service (agent/main.py) holds the model: a Hugging Face transformers
// pipeline that any name can point at, so models load in one place and anchors
// configuration names routes, not weights.
package remoteagent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// maxIdleConnsPerHost is sized for the widest fan-out a single workflow
	// is likely to produce, so steps in one wave share warm connections.
	maxIdleConnsPerHost = 64
	// idleConnTimeout is long enough that a connection survives the pause
	// between two waves of the same job.
	idleConnTimeout = 90 * time.Second
)

// Config configures a workflow.Agent backed by the agent service.
type Config struct {
	// Name identifies the agent in error messages. It is required.
	Name string
	// URL is the agent service's base URL, e.g. http://localhost:8000. It is
	// required.
	URL string
	// Instruction is sent with every prompt; the service applies it to the
	// chat template, because the model server is where templates are known.
	Instruction string
	// Timeout bounds a single generation. Zero leaves the client unbounded.
	Timeout time.Duration
}

// Agent calls the Python agent service over HTTP.
type Agent struct {
	name, url, instruction string
	client                 *http.Client
}

// New validates the configuration and returns an agent for the configured
// service, so a bad URL is a startup failure rather than a failed step.
func New(config Config) (*Agent, error) {
	if config.Name == "" || config.URL == "" {
		return nil, fmt.Errorf("agent %q requires a name and a service URL", config.Name)
	}
	if !strings.HasPrefix(config.URL, "http://") && !strings.HasPrefix(config.URL, "https://") {
		return nil, fmt.Errorf("agent %q URL %q must start with http:// or https://", config.Name, config.URL)
	}
	// Every step of a workflow hits the same sidecar, and a fan-out starts
	// many at once. The default transport keeps two idle connections per
	// host, so a wide wave would open and close a TCP connection per step;
	// keeping a pool the size of a large wave lets them all be reused.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = maxIdleConnsPerHost
	transport.MaxIdleConns = maxIdleConnsPerHost
	transport.IdleConnTimeout = idleConnTimeout
	client := &http.Client{Transport: transport}
	if config.Timeout > 0 {
		client.Timeout = config.Timeout
	}
	return &Agent{
		name:        config.Name,
		url:         strings.TrimSuffix(config.URL, "/"),
		instruction: config.Instruction,
		client:      client,
	}, nil
}

type runRequest struct {
	Prompt      string `json:"prompt"`
	Instruction string `json:"instruction"`
}

type runResponse struct {
	Output string `json:"output"`
}

type errorResponse struct {
	Detail string `json:"detail"`
}

// Run sends one prompt and returns the model's output. The service's error
// detail, status, and empty completions all surface as step failures, which is
// what workflow retries and skip cascades are driven by.
func (a *Agent) Run(ctx context.Context, prompt string) (string, error) {
	body, err := json.Marshal(runRequest{Prompt: prompt, Instruction: a.instruction})
	if err != nil {
		return "", fmt.Errorf("encode prompt: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url+"/run", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("call agent service %q: %w", a.name, err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read agent service response: %w", err)
	}
	if response.StatusCode != http.StatusOK {
		detail := errorResponse{}
		_ = json.Unmarshal(payload, &detail)
		if detail.Detail == "" {
			detail.Detail = strings.TrimSpace(string(payload))
		}
		return "", fmt.Errorf("agent service %q: HTTP %d: %s", a.name, response.StatusCode, detail.Detail)
	}
	var result runResponse
	if err := json.Unmarshal(payload, &result); err != nil {
		return "", fmt.Errorf("decode agent service response: %w", err)
	}
	if result.Output == "" {
		return "", fmt.Errorf("agent service %q returned no text output", a.name)
	}
	return result.Output, nil
}
