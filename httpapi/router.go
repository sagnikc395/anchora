// Package httpapi exposes Anchora workflows through a Chi router.
package httpapi

import (
	"encoding/json"
	"errors"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/sagnikc395/anchora"
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
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Post("/v1/workflows/run", runHandler(agents, options))
	return r
}

type runRequest struct {
	Steps []stepRequest `json:"steps"`
}
type stepRequest struct {
	ID     string `json:"id"`
	Agent  string `json:"agent"`
	Prompt string `json:"prompt"`
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
			steps = append(steps, anchora.Step{ID: input.ID, Agent: agent, Prompt: input.Prompt})
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
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
