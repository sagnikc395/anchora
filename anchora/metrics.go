package anchora

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are registered in the default Prometheus registry, so an
// application exposing promhttp pulls anchora's numbers with no extra wiring.
// Names follow the anchora_ prefix.
//
// Label cardinality is bounded by the code: workflow and step names come from
// Register and from Step call sites, not from user input.
var (
	checkpointWrites = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anchora_checkpoint_writes_total",
		Help: "Engine operations recorded in operation_outputs.",
	})
	checkpointDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name: "anchora_checkpoint_duration_seconds",
		Help: "Latency of durable checkpoint writes.",
		// From 50µs, so an in-memory or same-host Postgres write lands in a
		// real bucket instead of saturating the first one, out to ~26s.
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 20),
	})
	recoveries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anchora_recoveries_total",
		Help: "Workflows claimed from executors that stopped renewing.",
	})
	dequeued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anchora_dequeued_total",
		Help: "Workflows started from a queue.",
	}, []string{"queue"})

	workflowsStarted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anchora_workflows_started_total",
		Help: "Workflow executions dispatched on this executor, including recovered replays.",
	}, []string{"workflow"})
	workflowsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anchora_workflows_completed_total",
		Help: "Workflow executions that reached a terminal status.",
	}, []string{"workflow", "status"})
	workflowDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "anchora_workflow_duration_seconds",
		Help:    "Wall time of one workflow execution attempt, from dispatch to terminal write.",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 18),
	}, []string{"workflow"})
	workflowsRunning = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "anchora_workflows_running",
		Help: "Workflow executions currently running on this executor.",
	})

	// operations is the headline durability metric: the split between calls
	// that ran live and calls that were served from recorded history. A
	// healthy fleet replays only after a crash or a recovery.
	operations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anchora_operations_total",
		Help: "Engine calls by kind and whether they ran live or replayed from history.",
	}, []string{"kind", "source"})
	stepAttempts = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anchora_step_attempts_total",
		Help: "Step attempts, counting every retry.",
	}, []string{"step", "outcome"})
	stepDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "anchora_step_duration_seconds",
		Help:    "Wall time of a step's attempts, including backoff between them.",
		Buckets: prometheus.ExponentialBuckets(0.00005, 2, 20),
	}, []string{"step"})
	nondeterminism = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anchora_nondeterminism_total",
		Help: "Replays that hit a recorded slot the code no longer matches.",
	})
	fenced = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anchora_fenced_total",
		Help: "Checkpoint writes rejected because a newer epoch owns the workflow.",
	})
)

// observeCheckpoint records one durable checkpoint write.
func observeCheckpoint(fn func()) {
	timer := prometheus.NewTimer(checkpointDuration)
	defer timer.ObserveDuration()
	checkpointWrites.Inc()
	fn()
}

// observeOperation records one engine call at its slot: "replay" when the slot
// came back from history, "live" when it ran.
func observeOperation(kind Kind, replayed bool) {
	source := "live"
	if replayed {
		source = "replay"
	}
	operations.WithLabelValues(string(kind), source).Inc()
}

// observeStep records the attempts and wall time of one live step.
func observeStep(name string, attempts int, elapsed time.Duration, err error) {
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if attempts < 1 {
		attempts = 1
	}
	// Every attempt but the last one failed to produce the terminal outcome.
	if attempts > 1 {
		stepAttempts.WithLabelValues(name, "error").Add(float64(attempts - 1))
	}
	stepAttempts.WithLabelValues(name, outcome).Inc()
	stepDuration.WithLabelValues(name).Observe(elapsed.Seconds())
}
