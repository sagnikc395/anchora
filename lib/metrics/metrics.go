// Package metrics is a minimal Prometheus text-format metrics surface with no
// dependencies. Counters and gauges are held in one guarded map; each scrape
// renders every known family's HELP/TYPE header directly before its series, as
// the text format requires.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Metric families. The type and help text travel with the name.
type family struct {
	typ  string
	help string
}

var defs = map[string]family{
	"anchora_http_responses_total": {"counter", "Responses served by the HTTP API."},
	"anchora_steps_total":          {"counter", "Terminal step outcomes."},
	"anchora_workflows_total":      {"counter", "Workflow runs by result."},
	"anchora_jobs_submitted_total": {"counter", "Jobs accepted by the durable backend."},
	"anchora_jobs_total":           {"counter", "Jobs that reached a terminal state."},
	"anchora_queue_ready":          {"gauge", "Jobs waiting in the Redis ready list."},
	"anchora_jobs_in_flight":       {"gauge", "Jobs currently leased from the queue."},
	"anchora_workers":              {"gauge", "Workers in the live roster."},
}

var (
	mu     sync.Mutex
	series = map[string]*int64{} // key is "name{label}" or a bare name.
)

func bump(key string, delta int64) {
	mu.Lock()
	defer mu.Unlock()
	value, ok := series[key]
	if !ok {
		value = new(int64)
		series[key] = value
	}
	*value += delta
}

func set(key string, value int64) {
	mu.Lock()
	defer mu.Unlock()
	series[key] = &value
}

// AddHTTP records one served response by status code.
func AddHTTP(status int) { bump(`anchora_http_responses_total{code="`+strconv.Itoa(status)+`"}`, 1) }

// AddStep records one terminal step outcome.
func AddStep(status string) { bump(`anchora_steps_total{status="`+status+`"}`, 1) }

// AddWorkflow records a finished run as succeeded or failed.
func AddWorkflow(status string) { bump(`anchora_workflows_total{result="`+status+`"}`, 1) }

// AddJobSubmitted records one accepted durable job.
func AddJobSubmitted() { bump("anchora_jobs_submitted_total", 1) }

// AddJob records a durable job that reached a terminal state.
func AddJob(status string) { bump(`anchora_jobs_total{status="`+status+`"}`, 1) }

// SetQueue publishes the backend's current depth numbers. A scrape sees the
// last observed values; the poller that calls this owns the refresh cadence.
func SetQueue(ready, inFlight, workers int64) {
	set("anchora_queue_ready", ready)
	set("anchora_jobs_in_flight", inFlight)
	set("anchora_workers", workers)
}

// Handler serves the Prometheus text format.
func Handler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	mu.Lock()
	defer mu.Unlock()

	familyNames := make([]string, 0, len(defs))
	for name := range defs {
		familyNames = append(familyNames, name)
	}
	sort.Strings(familyNames)
	for _, name := range familyNames {
		def := defs[name]
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, def.help, name, def.typ)
		keys := make([]string, 0)
		for key := range series {
			if strings.HasPrefix(key, name) && (len(key) == len(name) || key[len(name)] == '{') {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(w, "%s %d\n", key, *series[key])
		}
	}
}
