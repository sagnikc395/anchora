// Package httpapi exposes Anchora workflows through a Chi router.
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sagnikc395/anchora"
	"github.com/sagnikc395/anchora/jobs"
	"net/http"
	"time"
)

type AgentResolver interface {
	Resolve(string) (anchora.Agent, bool)
}
type AgentRegistry map[string]anchora.Agent

func (r AgentRegistry) Resolve(name string) (anchora.Agent, bool) {
	agent, ok := r[name]
	return agent, ok
}

type Options struct {
	MaxRetries int
	RetryDelay time.Duration
}

func NewRouter(agents AgentResolver, options Options) http.Handler {
	return NewRouterWithJobs(agents, options, nil)
}
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
	}
	return r
}

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
	Steps []anchora.StepResult `json:"steps"`
}

func runHandler(agents AgentResolver, options Options) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		var request runRequest
		if err := decoder.Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON request")
			return
		}
		steps := make([]anchora.Step, 0, len(request.Steps))
		for _, input := range request.Steps {
			agent, ok := agents.Resolve(input.Agent)
			if !ok {
				writeError(w, http.StatusBadRequest, "unknown agent: "+input.Agent)
				return
			}
			steps = append(steps, anchora.Step{ID: input.ID, Agent: agent, Prompt: input.Prompt, DependsOn: input.DependsOn})
		}
		workflow, err := anchora.NewWorkflow(steps, anchora.Options{MaxRetries: options.MaxRetries, RetryDelay: options.RetryDelay})
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		results, err := workflow.Run(r.Context())
		if err != nil {
			var workflowErr *anchora.WorkflowError
			if errors.As(err, &workflowErr) {
				writeJSON(w, http.StatusBadGateway, runResponse{Steps: results})
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, runResponse{Steps: results})
	}
}
func submitJobHandler(service *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
		decoder.DisallowUnknownFields()
		var request runRequest
		if err := decoder.Decode(&request); err != nil {
			writeError(w, http.StatusBadRequest, "invalid JSON request")
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
func eventsHandler(service *jobs.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if job, err := service.Get(r.Context(), id); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		} else if job == nil {
			writeError(w, http.StatusNotFound, "job not found")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported")
			return
		}
		var after int64
		if _, err := fmt.Sscan(r.Header.Get("Last-Event-ID"), &after); err != nil {
			writeError(w, http.StatusBadRequest, "invalid Last-Event-ID")
			return
		}
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			events, err := service.Events(r.Context(), id, after)
			if err != nil {
				return
			}
			for _, event := range events {
				_, _ = fmt.Fprintf(w, "id: %d\nevent: %s\ndata: %s\n\n", event.ID, event.Type, event.Data)
				after = event.ID
			}
			flusher.Flush()
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
			}
		}
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
