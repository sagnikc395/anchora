package anchora

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics are registered in the default Prometheus registry, so an
// application exposing promhttp pulls anchora's numbers with no extra wiring.
// Names follow the anchora_ prefix.
var (
	checkpointWrites = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anchora_checkpoint_writes_total",
		Help: "Engine operations recorded in operation_outputs.",
	})
	checkpointDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "anchora_checkpoint_duration_seconds",
		Help:    "Latency of durable checkpoint writes.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
	})
	recoveries = promauto.NewCounter(prometheus.CounterOpts{
		Name: "anchora_recoveries_total",
		Help: "Workflows claimed from executors that stopped renewing.",
	})
	dequeued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "anchora_dequeued_total",
		Help: "Workflows started from a queue.",
	}, []string{"queue"})
)

// observeCheckpoint records one durable checkpoint write.
func observeCheckpoint(fn func()) {
	timer := prometheus.NewTimer(checkpointDuration)
	defer timer.ObserveDuration()
	checkpointWrites.Inc()
	fn()
}
