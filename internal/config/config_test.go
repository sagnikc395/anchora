package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sagnikc395/anchora/internal/config"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAsyncDefaults(t *testing.T) {
	cfg, err := config.Load(write(t, "server:\n  address: \":9000\"\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Async.Lease(); got != 30*time.Second {
		t.Fatalf("lease = %s, want 30s", got)
	}
	if got := cfg.Async.Heartbeat(); got != 10*time.Second {
		t.Fatalf("heartbeat = %s, want a third of the lease", got)
	}
	if got := cfg.Async.WorkerTTL(); got != 2*time.Minute {
		t.Fatalf("worker TTL = %s, want four leases", got)
	}
	if !cfg.Async.ReaperEnabled() {
		t.Fatal("the reaper must default to on so a single node recovers its own work")
	}
}

func TestAsyncRejectsHeartbeatLongerThanLease(t *testing.T) {
	_, err := config.Load(write(t, "async:\n  lease_ms: 1000\n  heartbeat_ms: 5000\n"))
	if err == nil {
		t.Fatal("expected an error: a heartbeat slower than the lease guarantees lease loss")
	}
}

func TestAsyncEnabledRequiresBackendURLs(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("REDIS_URL", "")
	if _, err := config.Load(write(t, "async:\n  enabled: true\n")); err == nil {
		t.Fatal("expected async to fail fast when the backend URLs are unset")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/anchora")
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	if _, err := config.Load(write(t, "async:\n  enabled: true\n")); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReaperCanBeDisabled(t *testing.T) {
	cfg, err := config.Load(write(t, "async:\n  reaper: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Async.ReaperEnabled() {
		t.Fatal("reaper: false must disable the sweep")
	}
}

func TestLoadAppliesDefaultsAndExplicitValues(t *testing.T) {
	cfg, err := config.Load(write(t, `
server:
  address: ":9000"
workflow:
  max_retries: 4
  retry_delay_ms: 250
async:
  lease_ms: 60000
  heartbeat_ms: 5000
  reaper_interval_ms: 1000
  worker_ttl_ms: 90000
  workers: 8
agents:
  research:
    model_id: some/model
    timeout_ms: 15000
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Address != ":9000" {
		t.Errorf("address = %q, want %q", cfg.Server.Address, ":9000")
	}
	if got := cfg.Workflow.RetryDelay(); got != 250*time.Millisecond {
		t.Errorf("retry delay = %s, want 250ms", got)
	}
	if got := cfg.Async.Lease(); got != time.Minute {
		t.Errorf("lease = %s, want 1m", got)
	}
	if got := cfg.Async.Heartbeat(); got != 5*time.Second {
		t.Errorf("heartbeat = %s, want 5s", got)
	}
	if got := cfg.Async.ReaperInterval(); got != time.Second {
		t.Errorf("reaper interval = %s, want 1s", got)
	}
	if got := cfg.Async.WorkerTTL(); got != 90*time.Second {
		t.Errorf("worker TTL = %s, want 90s", got)
	}
	if got := cfg.Async.WorkerCount(); got != 8 {
		t.Errorf("workers = %d, want 8", got)
	}
	if got := cfg.Agents["research"].Timeout(); got != 15*time.Second {
		t.Errorf("agent timeout = %s, want 15s", got)
	}
}

func TestLoadDefaultsAddressAndWorkerCount(t *testing.T) {
	cfg, err := config.Load(write(t, "workflow:\n  max_retries: 1\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Address != ":8080" {
		t.Errorf("address = %q, want %q", cfg.Server.Address, ":8080")
	}
	// A configured zero must still start one worker, or async would accept
	// jobs that nothing ever runs.
	if got := cfg.Async.WorkerCount(); got != 1 {
		t.Errorf("worker count = %d, want 1", got)
	}
	if got := cfg.Async.ReaperInterval(); got != 5*time.Second {
		t.Errorf("reaper interval = %s, want the 5s default", got)
	}
}

func TestLoadRejectsMissingAndMalformedFiles(t *testing.T) {
	if _, err := config.Load(filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Error("expected an error for a missing file")
	}
	if _, err := config.Load(write(t, "server: [this is not a mapping]\n")); err == nil {
		t.Error("expected an error for malformed YAML")
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := map[string]string{
		"negative max_retries":    "workflow:\n  max_retries: -1\n",
		"negative retry_delay_ms": "workflow:\n  retry_delay_ms: -1\n",
		"negative workers":        "async:\n  workers: -1\n",
		"negative lease_ms":       "async:\n  lease_ms: -1\n",
		"negative worker_ttl_ms":  "async:\n  worker_ttl_ms: -1\n",
		"negative max_attempts":   "async:\n  max_attempts: -1\n",
		"negative reclaim_batch":  "async:\n  reclaim_batch: -1\n",
		"agent without model_id":  "agents:\n  research: {}\n",
		"negative max_tokens":     "agents:\n  research:\n    model_id: m\n    max_tokens: -1\n",
		"negative timeout_ms":     "agents:\n  research:\n    model_id: m\n    timeout_ms: -1\n",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := config.Load(write(t, body)); err == nil {
				t.Fatal("expected a validation error")
			}
		})
	}
}

func TestAsyncURLsComeFromNamedVariables(t *testing.T) {
	t.Setenv("CUSTOM_DB", "postgres://localhost/custom")
	t.Setenv("CUSTOM_REDIS", "redis://localhost:6379/2")
	cfg, err := config.Load(write(t, "async:\n  enabled: true\n  database_url_env: CUSTOM_DB\n  redis_url_env: CUSTOM_REDIS\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Async.DatabaseURL(); got != "postgres://localhost/custom" {
		t.Errorf("database URL = %q", got)
	}
	if got := cfg.Async.RedisURL(); got != "redis://localhost:6379/2" {
		t.Errorf("redis URL = %q", got)
	}
}

// The checked-in configuration must always load, since it is what the README
// tells a reader to start with.
func TestCheckedInConfigLoads(t *testing.T) {
	if _, err := config.Load(filepath.Join("..", "..", "config.yaml")); err != nil {
		t.Fatalf("config.yaml does not load: %v", err)
	}
}
