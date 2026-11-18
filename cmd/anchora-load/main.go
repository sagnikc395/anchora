// Command anchora-load drives a real workload through the engine and reports
// what it measured: throughput, latency percentiles, and the per-step cost of
// durability. It serves /metrics the whole time, so Prometheus and Grafana can
// scrape the same run the summary is computed from.
//
//	docker compose up -d --wait
//	go run ./cmd/anchora-load -workflows 2000 -concurrency 32 -steps 5
//
// The workload is deliberately plain: each workflow runs -steps checkpointed
// steps whose bodies do nothing. What it measures is the engine, not the user
// code, so the numbers are an upper bound on what the engine can push.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"

	"github.com/sagnikc395/anchora/anchora"
	"github.com/sagnikc395/anchora/anchora/memory"
)

func main() {
	var (
		databaseURL = flag.String("database-url", os.Getenv("DATABASE_URL"), "Postgres URL; empty uses the in-memory store")
		workflows   = flag.Int("workflows", 1000, "workflows to run")
		concurrency = flag.Int("concurrency", 32, "workflows started in parallel")
		steps       = flag.Int("steps", 5, "checkpointed steps per workflow")
		queue       = flag.Bool("queue", false, "start through a queue instead of inline")
		metricsAddr = flag.String("metrics-addr", ":9095", "address serving /metrics")
		warmup      = flag.Int("warmup", 50, "workflows to run and discard before measuring")
		jsonOut     = flag.String("json", "", "also write the summary to this file as JSON")
		failRate    = flag.Float64("fail-rate", 0, "fraction of step attempts that fail transiently, to exercise the retry path")
		errRate     = flag.Float64("error-rate", 0, "fraction of workflows that end in a terminal error")
		await       = flag.Bool("await", false, "wait on each workflow with Handle.Result; measures caller-observed latency, but caps throughput at concurrency/poll-interval. Off measures engine throughput.")
		runID       = flag.String("run-id", "", "fixed id prefix for the workflows. Reusing it after a kill -9 makes the restarted engine recover and replay those executions, which is how the replay and recovery panels get data.")
		stepSleep   = flag.Duration("step-sleep", 0, "sleep inside each step, to hold executions open long enough to kill the process mid-flight")
		hold        = flag.Duration("hold", 0, "keep serving /metrics this long after finishing, so Prometheus can scrape the tail")
	)
	flag.Parse()

	if *workflows < 1 || *concurrency < 1 || *steps < 0 {
		log.Fatal("workflows and concurrency must be >= 1, steps >= 0")
	}

	// Bind before any work starts: a run whose /metrics never came up is a
	// run Prometheus did not scrape, and silently producing an empty
	// dashboard is worse than refusing to start.
	listener, err := net.Listen("tcp", *metricsAddr)
	if err != nil {
		log.Fatalf("metrics server: %v", err)
	}
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("metrics server: %v", err)
		}
	}()

	ctx := context.Background()
	cfg := anchora.Config{
		AppVersion:        "load",
		ExecutorID:        fmt.Sprintf("load-%d", time.Now().UnixNano()),
		QueuePollInterval: 5 * time.Millisecond,
		Lease:             30 * time.Second,
		RecoveryInterval:  time.Second,
	}
	store := "postgres"
	if *databaseURL == "" {
		cfg.Store = memory.New()
		store = "memory"
	} else {
		cfg.DatabaseURL = *databaseURL
	}

	eng, err := anchora.New(ctx, cfg)
	if err != nil {
		log.Fatalf("new engine: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = eng.Shutdown(shutdownCtx)
	}()

	// done fires once per workflow body, so queue-backed runs can be timed
	// from enqueue to execution rather than from enqueue to enqueue.
	var completed atomic.Int64
	errTransient := errors.New("injected transient failure")
	errTerminal := errors.New("injected terminal failure")
	bench := anchora.RegisterWorkflow(eng, "load", func(wctx *anchora.Context, n int) (int, error) {
		total := 0
		for i := 0; i < n; i++ {
			next, err := anchora.Step(wctx, fmt.Sprintf("step-%d", i), func(sc anchora.StepCtx) (int, error) {
				// Fail early attempts only, so the step still succeeds and the
				// retry shows up as attempts rather than as a dead workflow.
				if *failRate > 0 && sc.Attempt() == 1 && rand.Float64() < *failRate {
					return 0, errTransient
				}
				if *stepSleep > 0 {
					select {
					case <-time.After(*stepSleep):
					case <-sc.Done():
						return 0, sc.Err()
					}
				}
				return total + 1, nil
			}, anchora.Retry(4, 10*time.Millisecond, 2))
			if err != nil {
				return 0, err
			}
			total = next
		}
		completed.Add(1)
		if *errRate > 0 && rand.Float64() < *errRate {
			return 0, errTerminal
		}
		return total, nil
	})

	if *queue {
		if err := eng.RegisterQueue(ctx, anchora.Queue{Name: "load", LocalConcurrency: *concurrency}); err != nil {
			log.Fatalf("register queue: %v", err)
		}
	}

	// run starts count workflows and returns per-workflow latencies (only in
	// -await mode) and the wall time to drain them all.
	//
	// Two modes, because they answer different questions:
	//
	//   -await=true   each goroutine calls Handle.Result, so a latency is the
	//                 caller-observed Start-to-answer time. Handle.Result
	//                 polls, so throughput here cannot exceed
	//                 concurrency / poll-interval no matter how fast the
	//                 engine is. Use it to read latency, not throughput.
	//
	//   -await=false  starts are the only thing in the critical path and the
	//                 run ends when the last workflow body has executed. This
	//                 is the engine's throughput ceiling. It counts bodies run
	//                 by THIS process, so it cannot be used over workflow ids
	//                 that already finished elsewhere: nothing would re-run
	//                 and the drain would never complete. Use -await for a
	//                 recovery pass over an existing -run-id.
	run := func(idPrefix string, count int) ([]time.Duration, time.Duration, error) {
		if *await {
			latencies := make([]time.Duration, count)
			failures := make([]error, count)
			sem := make(chan struct{}, *concurrency)
			var wg sync.WaitGroup
			start := time.Now()
			for i := 0; i < count; i++ {
				wg.Add(1)
				sem <- struct{}{}
				go func(i int) {
					defer wg.Done()
					defer func() { <-sem }()
					began := time.Now()
					h, err := start1(ctx, bench, *steps, *queue, fmt.Sprintf("%s-%d", idPrefix, i))
					if err != nil {
						failures[i] = err
						return
					}
					if _, err := h.Result(ctx); err != nil && !errors.Is(err, errTerminal) {
						failures[i] = err
						return
					}
					latencies[i] = time.Since(began)
				}(i)
			}
			wg.Wait()
			elapsed := time.Since(start)
			for _, err := range failures {
				if err != nil {
					return nil, elapsed, err
				}
			}
			return latencies, elapsed, nil
		}

		target := completed.Load() + int64(count)
		failures := make([]error, count)
		sem := make(chan struct{}, *concurrency)
		var wg sync.WaitGroup
		start := time.Now()
		for i := 0; i < count; i++ {
			wg.Add(1)
			sem <- struct{}{}
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				if _, err := start1(ctx, bench, *steps, *queue, fmt.Sprintf("%s-%d", idPrefix, i)); err != nil {
					failures[i] = err
				}
			}(i)
		}
		wg.Wait()
		for _, err := range failures {
			if err != nil {
				return nil, time.Since(start), err
			}
		}
		// Every start is in; wait for the last body to run.
		// Fail fast rather than sit at the 10-minute deadline: if no body has
		// run for a while and the count is short, these ids almost certainly
		// completed in an earlier process and -await is the flag to use.
		deadline := time.After(10 * time.Minute)
		stall := time.NewTimer(30 * time.Second)
		defer stall.Stop()
		last := completed.Load()
		for completed.Load() < target {
			select {
			case <-deadline:
				return nil, time.Since(start), fmt.Errorf("drained %d of %d workflows", completed.Load(), target)
			case <-stall.C:
				if now := completed.Load(); now == last {
					return nil, time.Since(start), fmt.Errorf(
						"no workflow body ran for 30s at %d of %d: these ids may already be terminal, in which case use -await",
						now, target)
				}
				last = completed.Load()
				stall.Reset(30 * time.Second)
			case <-time.After(time.Millisecond):
			}
		}
		return nil, time.Since(start), nil
	}

	prefix := *runID
	if prefix == "" {
		prefix = fmt.Sprint(time.Now().UnixNano())
	}
	if *warmup > 0 {
		// Pays for schema migration, connection-pool fill and plan caching
		// before the clock that matters starts.
		if _, _, err := run("warmup-"+prefix, *warmup); err != nil {
			log.Fatalf("warmup: %v", err)
		}
	}

	before := completed.Load()
	histBefore := gatherHistogram("anchora_checkpoint_duration_seconds")
	latencies, elapsed, err := run("load-"+prefix, *workflows)
	if err != nil {
		log.Fatalf("load: %v", err)
	}
	executed := completed.Load() - before
	// Delta of the engine's own histogram over just the measured run: this is
	// the durable-write cost, with no await polling in it.
	checkpoint := histogramDelta(histBefore, gatherHistogram("anchora_checkpoint_duration_seconds"))

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	hasLatency := len(latencies) > 0
	// Each workflow writes one checkpoint per step plus one terminal row.
	checkpoints := int64(*workflows) * int64(*steps)
	sum := summary{
		Store:              store,
		Queue:              *queue,
		Workflows:          *workflows,
		Concurrency:        *concurrency,
		StepsPerWorkflow:   *steps,
		Executed:           int(executed),
		Elapsed:            elapsed.String(),
		WorkflowsPerSecond: round(float64(*workflows) / elapsed.Seconds()),
		StepsPerSecond:     round(float64(checkpoints) / elapsed.Seconds()),
		Awaited:            *await,
		CheckpointCount:    checkpoint.count,
		CheckpointP50:      checkpoint.quantile(0.50).String(),
		CheckpointP95:      checkpoint.quantile(0.95).String(),
		CheckpointP99:      checkpoint.quantile(0.99).String(),
		CheckpointMean:     checkpoint.mean().String(),
	}
	if hasLatency {
		sum.LatencyP50 = pct(latencies, 0.50).String()
		sum.LatencyP95 = pct(latencies, 0.95).String()
		sum.LatencyP99 = pct(latencies, 0.99).String()
		sum.LatencyMax = latencies[len(latencies)-1].Round(time.Microsecond).String()
	}

	fmt.Printf(`
anchora load: %d workflows x %d steps, concurrency %d, store=%s queue=%v
  elapsed              %s
  workflows/sec        %.0f
  checkpoints/sec      %.0f
  bodies executed      %d of %d (any excess would mean duplicate execution)

  checkpoint write     p50 %s   p95 %s   p99 %s   mean %s   (n=%d)
    the engine's own histogram: one durable write, no polling in it.
`, *workflows, *steps, *concurrency, store, *queue,
		sum.Elapsed, sum.WorkflowsPerSecond, sum.StepsPerSecond,
		executed, *workflows,
		sum.CheckpointP50, sum.CheckpointP95, sum.CheckpointP99, sum.CheckpointMean, sum.CheckpointCount)

	if hasLatency {
		fmt.Printf(`  end-to-end           p50 %s   p95 %s   p99 %s   max %s
    Start to Result, so it includes Handle.Result's poll backoff (20ms first
    tick). It bounds how fast a caller LEARNS the answer, not how fast the
    engine produced it, and it caps throughput above at concurrency/20ms.
`, sum.LatencyP50, sum.LatencyP95, sum.LatencyP99, sum.LatencyMax)
	}

	// Fewer bodies than workflows is normal for a recovery pass over an
	// existing -run-id: the ids that already finished do not run again. More
	// bodies than workflows is the invariant violation worth shouting about.
	if executed > int64(*workflows) {
		fmt.Printf("\nWARNING: %d bodies ran for %d workflows — duplicate execution\n", executed, *workflows)
	} else if executed < int64(*workflows) {
		fmt.Printf("\n  %d of %d bodies ran here; the rest were already terminal\n", executed, *workflows)
	}

	if *jsonOut != "" {
		blob, err := json.MarshalIndent(sum, "", "  ")
		if err != nil {
			log.Fatalf("encode summary: %v", err)
		}
		if err := os.WriteFile(*jsonOut, append(blob, '\n'), 0o644); err != nil {
			log.Fatalf("write summary: %v", err)
		}
		fmt.Printf("wrote %s\n", *jsonOut)
	}

	if *hold > 0 {
		fmt.Printf("holding /metrics on %s for %s\n", *metricsAddr, *hold)
		time.Sleep(*hold)
	}
}

// start1 starts one workflow under a fixed id, inline or through the queue.
func start1(ctx context.Context, w *anchora.Workflow[int, int], steps int, queue bool, id string) (*anchora.Handle[int], error) {
	opts := []anchora.StartOption{anchora.WithWorkflowID(id)}
	if queue {
		opts = append(opts, anchora.WithQueue("load"))
	}
	return w.Start(ctx, steps, opts...)
}

type summary struct {
	Store              string  `json:"store"`
	Queue              bool    `json:"queue"`
	Workflows          int     `json:"workflows"`
	Concurrency        int     `json:"concurrency"`
	StepsPerWorkflow   int     `json:"steps_per_workflow"`
	Awaited            bool    `json:"awaited"`
	Executed           int     `json:"bodies_executed"`
	Elapsed            string  `json:"elapsed"`
	WorkflowsPerSecond float64 `json:"workflows_per_second"`
	StepsPerSecond     float64 `json:"checkpoints_per_second"`
	LatencyP50         string  `json:"latency_p50"`
	LatencyP95         string  `json:"latency_p95"`
	LatencyP99         string  `json:"latency_p99"`
	LatencyMax         string  `json:"latency_max"`
	CheckpointCount    uint64  `json:"checkpoint_writes"`
	CheckpointP50      string  `json:"checkpoint_p50"`
	CheckpointP95      string  `json:"checkpoint_p95"`
	CheckpointP99      string  `json:"checkpoint_p99"`
	CheckpointMean     string  `json:"checkpoint_mean"`
}

// hist is a snapshot of one Prometheus histogram: cumulative bucket counts
// keyed by upper bound, plus the count and sum.
type hist struct {
	bounds []float64
	counts []uint64
	count  uint64
	sum    float64
}

// gatherHistogram pulls one histogram out of the default registry by name.
func gatherHistogram(name string) hist {
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return hist{}
	}
	for _, f := range families {
		if f.GetName() != name || f.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		for _, m := range f.GetMetric() {
			h := m.GetHistogram()
			out := hist{count: h.GetSampleCount(), sum: h.GetSampleSum()}
			for _, b := range h.GetBucket() {
				out.bounds = append(out.bounds, b.GetUpperBound())
				out.counts = append(out.counts, b.GetCumulativeCount())
			}
			return out
		}
	}
	return hist{}
}

// histogramDelta subtracts a before-snapshot from an after-snapshot, so the
// result covers only the measured run and not the warmup.
func histogramDelta(before, after hist) hist {
	out := hist{bounds: after.bounds, count: after.count - before.count, sum: after.sum - before.sum}
	out.counts = make([]uint64, len(after.counts))
	for i := range after.counts {
		if i < len(before.counts) {
			out.counts[i] = after.counts[i] - before.counts[i]
		} else {
			out.counts[i] = after.counts[i]
		}
	}
	return out
}

// quantile interpolates within the bucket the quantile falls in, the same way
// Prometheus histogram_quantile does. Bucket boundaries bound the error.
func (h hist) quantile(q float64) time.Duration {
	if h.count == 0 {
		return 0
	}
	want := q * float64(h.count)
	for i, c := range h.counts {
		if float64(c) < want {
			continue
		}
		upper := h.bounds[i]
		if math.IsInf(upper, 1) {
			// The top bucket is unbounded; the last finite bound is all we know.
			if i > 0 {
				upper = h.bounds[i-1]
			} else {
				return 0
			}
		}
		lower := 0.0
		priorCount := 0.0
		if i > 0 {
			lower = h.bounds[i-1]
			priorCount = float64(h.counts[i-1])
		}
		span := float64(c) - priorCount
		if span <= 0 {
			return seconds(upper)
		}
		return seconds(lower + (upper-lower)*(want-priorCount)/span)
	}
	return seconds(h.bounds[len(h.bounds)-1])
}

func (h hist) mean() time.Duration {
	if h.count == 0 {
		return 0
	}
	return seconds(h.sum / float64(h.count))
}

func seconds(f float64) time.Duration {
	return (time.Duration(f * float64(time.Second))).Round(time.Microsecond)
}

// pct returns the q-quantile of a sorted slice, rounded to microseconds so the
// printed numbers do not imply more precision than the clock has.
func pct(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i].Round(time.Microsecond)
}

func round(f float64) float64 { return math.Round(f*10) / 10 }
