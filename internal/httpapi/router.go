// Package httpapi exposes Anchora workflows through a Chi router.
//
// Two shapes of execution share one request body: /v1/workflows/run executes a
// workflow inside the request, and /v1/jobs hands it to the durable backend and
// returns immediately. The job routes are registered only when a backend is
// configured.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/sagnikc395/anchora/internal/jobs"
	"github.com/sagnikc395/anchora/internal/workflow"
)

// maxRequestBytes bounds a workflow submission so an oversized body is
// rejected before it is buffered.
const maxRequestBytes = 1 << 20

// pollInterval is how often the SSE handler re-reads a job's event log.
const pollInterval = 500 * time.Millisecond

// AgentResolver maps a step's agent name to a runnable agent.
type AgentResolver interface {
	Resolve(string) (workflow.Agent, bool)
}

// AgentRegistry is an AgentResolver backed by a map of named agents.
type AgentRegistry map[string]workflow.Agent

// Resolve looks up an agent by the name used in a step.
func (r AgentRegistry) Resolve(name string) (workflow.Agent, bool) {
	agent, ok := r[name]
	return agent, ok
}

// Options configures the retry behaviour applied to every workflow the router
// runs.
type Options struct {
	MaxRetries int
	RetryDelay time.Duration
}

// NewRouter returns a router serving only the synchronous workflow API.
func NewRouter(agents AgentResolver, options Options) http.Handler {
	return NewRouterWithJobs(agents, options, nil)
}

// NewRouterWithJobs returns a router that also serves the durable job API. A
// nil service omits the job routes entirely.
func NewRouterWithJobs(agents AgentResolver, options Options, service *jobs.Service) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Post("/v1/workflows/run", runHandler(agents, options))
	if service != nil {
		r.Post("/v1/jobs", submitJobHandler(service))
		r.Get("/v1/jobs/{id}", getJobHandler(service))
		r.Get("/v1/jobs/{id}/events", eventsHandler(service))
		r.Get("/v1/cluster", clusterHandler(service))
	}
	return r
}

// runRequest is the request body shared by both execution endpoints.
type runRequest struct {
	Steps []stepRequest `json:"steps"`
}

type stepRequest struct {
	ID        string   `json:"id"`
	Agent     string   `json:"agent"`
	Prompt    string   `json:"prompt"`
	DependsOn []string `json:"depends_on,omitempty"`
}

type runResponse struct {
	Steps []workflow.StepResult `json:"steps"`
}

// decodeRunRequest reads a size-limited workflow body, rejecting unknown fields
// so a typo in a step key is reported rather than silently dropped.
func decodeRunRequest(w http.ResponseWriter, r *http.Request) (runRequest, bool) {
	defer r.Body.Close()
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	decoder.DisallowUnknownFields()
	var request runRequest
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return runRequest{}, false
	}
	return request, true
}

// runHandler executes a workflow inside the request. A step failure is a 502:
// the request itself was valid, but an upstream agent did not deliver.
func runHandler(agents AgentResolver, options Options) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		request, ok := decodeRunRequest(w, r)
		if !ok {
			return
		}
		steps := make([]workflow.Step, 0, len(request.Steps))
		for _, input := range request.Steps {
			agent, ok := agents.Resolve(input.Agent)
			if !ok {
				writeError(w, http.StatusBadRequest, "unknown agent: "+input.Agent)
				return
			}
			steps = append(steps, workflow.Step{ID: input.ID, Agent: agent, Prompt: input.Prompt, DependsOn: input.DependsOn})
		}
		wf, err := workflow.NewWorkflow(steps, workflow.Options{MaxRetries: options.MaxRetries, RetryDelay: options.RetryDelay})
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		results, err := wf.Run(r.Context())
		if err != nil {
			var workflowErr *workflow.WorkflowError
			if errors.As(err, &workflowErr) {
				// The partial results are more useful than the error alone.
				writeJSON(w, http.StatusBadGateway, runResponse{Steps: results})
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, runResponse{Steps: results})
	}
}

// submitJobHandler records a workflow for asynchronous execution.
func submitJobHandler(service *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		request, ok := decodeRunRequest(w, r)
		if !ok {
			return
		}
		steps := make([]jobs.Step, len(request.Steps))
		for i, step := range request.Steps {
			steps[i] = jobs.Step{ID: step.ID, Agent: step.Agent, Prompt: step.Prompt, DependsOn: step.DependsOn}
		}
		job, err := service.Submit(r.Context(), steps)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusAccepted, job)
	}
}

func getJobHandler(service *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		job, err := service.Get(r.Context(), chi.URLParam(r, "id"))
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if job == nil {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

// eventsHandler streams a job's event log as Server-Sent Events by polling the
// store, and closes the stream once the job reaches a terminal state.
func eventsHandler(service *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		job, err := service.Get(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if job == nil {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}
		// Last-Event-ID is absent on a first connection and carries the last
		// delivered event ID on a reconnect; only a present-but-unparseable
		// value is an error.
		var after int64
		if resume := r.Header.Get("Last-Event-ID"); resume != "" {
			parsed, err := strconv.ParseInt(resume, 10, 64)
			if err != nil || parsed < 0 {
				writeError(w, http.StatusBadRequest, "invalid Last-Event-ID")
				return
			}
			after = parsed
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			events, err := service.Events(r.Context(), id, after)
			if err != nil {
				return
			}
			terminal := false
			for _, event := range events {
				_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.ID, event.Type, event.Data)
				after = event.ID
				terminal = terminal || jobs.IsTerminal(event.Type)
			}
			flusher.Flush()
			if terminal {
				return
			}
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
	}
}

// clusterHandler reports queue depth and the live worker roster, which is how
// an operator confirms that workers are heartbeating and work is draining.
func clusterHandler(service *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := service.Stats(r.Context())
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, stats)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
