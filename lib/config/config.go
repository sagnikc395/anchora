// Package config loads Anchora's YAML configuration.
//
// Durations are configured in milliseconds so the file stays free of unit
// suffixes; the accessor methods on each section apply the defaults and return
// [time.Duration]. Secrets are never stored in the file: a URL or token is
// named by the environment variable that holds it.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Default values applied to any setting left at its zero value.
const (
	defaultAddress        = ":8080"
	defaultDatabaseURLEnv = "DATABASE_URL"
	defaultRedisURLEnv    = "REDIS_URL"
	defaultLease          = 30 * time.Second
	defaultReaperInterval = 5 * time.Second
)

// Config is a complete Anchora configuration file.
type Config struct {
	Server   Server           `yaml:"server"`
	Workflow Workflow         `yaml:"workflow"`
	Async    Async            `yaml:"async"`
	Agents   map[string]Agent `yaml:"agents"`
}

// Server configures the HTTP listener.
type Server struct {
	// Address is the listen address, defaulting to ":8080".
	Address string `yaml:"address"`
}

// Workflow configures retry behaviour shared by every workflow run.
type Workflow struct {
	// MaxRetries is how many additional attempts a failing step gets.
	MaxRetries int `yaml:"max_retries"`
	// RetryDelayMS scales the linear backoff between attempts.
	RetryDelayMS int `yaml:"retry_delay_ms"`
}

// Async configures the durable job backend and its worker pool. It is inert
// unless Enabled is set.
type Async struct {
	// Enabled exposes the job API and starts the local worker pool.
	Enabled bool `yaml:"enabled"`
	// DatabaseURLEnv names the variable holding the PostgreSQL URL.
	DatabaseURLEnv string `yaml:"database_url_env"`
	// RedisURLEnv names the variable holding the Redis URL.
	RedisURLEnv string `yaml:"redis_url_env"`
	// QueueName is the Redis key prefix for the queue.
	QueueName string `yaml:"queue_name"`
	// Workers is the number of worker goroutines. Zero means one.
	Workers int `yaml:"workers"`
	// LeaseMS is the visibility timeout: how long a worker's claim on a job
	// survives without a heartbeat before another worker may take it over.
	LeaseMS int `yaml:"lease_ms"`
	// HeartbeatMS is the lease renewal interval. Zero derives it from LeaseMS.
	HeartbeatMS int `yaml:"heartbeat_ms"`
	// MaxAttempts caps job deliveries before dead-lettering. Zero is unlimited.
	MaxAttempts int `yaml:"max_attempts"`
	// ReaperIntervalMS is how often expired leases are swept.
	ReaperIntervalMS int `yaml:"reaper_interval_ms"`
	// WorkerTTLMS is how long a silent worker stays in the registry. Zero
	// derives it from LeaseMS.
	WorkerTTLMS int `yaml:"worker_ttl_ms"`
	// ReclaimBatch caps how many jobs a single reaper sweep recovers.
	ReclaimBatch int `yaml:"reclaim_batch"`
	// Reaper runs the recovery sweep on this node. Leave it on unless you run
	// a dedicated reaper process; the sweeps are atomic and safe to duplicate.
	Reaper *bool `yaml:"reaper"`
}

// Agent configures one named OpenAI-compatible chat model.
type Agent struct {
	// ModelID is the provider's model name. It is required.
	ModelID string `yaml:"model_id"`
	// BaseURL is the OpenAI-compatible endpoint. Zero uses Hugging Face's
	// Inference Providers router.
	BaseURL string `yaml:"base_url"`
	// TokenEnv names the variable holding the bearer token, default HF_TOKEN.
	TokenEnv string `yaml:"token_env"`
	// Instruction is sent as a system message before each prompt.
	Instruction string `yaml:"instruction"`
	// MaxTokens caps the response length. Zero omits the field entirely.
	MaxTokens int `yaml:"max_tokens"`
	// TimeoutMS bounds a single generation. Zero leaves the client unbounded.
	TimeoutMS int `yaml:"timeout_ms"`
}

// Load reads, decodes, and validates the configuration at path. It fails
// rather than starting with a setting that could only misbehave later, so a
// bad lease window or a missing backend URL is caught at startup.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	if cfg.Server.Address == "" {
		cfg.Server.Address = defaultAddress
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.Workflow.MaxRetries < 0 || c.Workflow.RetryDelayMS < 0 {
		return errors.New("workflow retry values must be non-negative")
	}
	if err := c.Async.validate(); err != nil {
		return err
	}
	for name, agent := range c.Agents {
		if name == "" || agent.ModelID == "" {
			return errors.New("agents must have a name and model_id")
		}
		if agent.MaxTokens < 0 || agent.TimeoutMS < 0 {
			return fmt.Errorf("agent %q values must be non-negative", name)
		}
	}
	return nil
}

func (a Async) validate() error {
	if a.Workers < 0 {
		return errors.New("async workers must be non-negative")
	}
	if a.LeaseMS < 0 || a.HeartbeatMS < 0 || a.ReaperIntervalMS < 0 || a.WorkerTTLMS < 0 {
		return errors.New("async lease, heartbeat, reaper, and worker TTL values must be non-negative")
	}
	if a.MaxAttempts < 0 || a.ReclaimBatch < 0 {
		return errors.New("async max_attempts and reclaim_batch must be non-negative")
	}
	// A heartbeat at or past the lease window guarantees the lease expires
	// before it is ever renewed, so every job would be reclaimed mid-run.
	if a.HeartbeatMS > 0 && a.LeaseMS > 0 && a.HeartbeatMS >= a.LeaseMS {
		return errors.New("async heartbeat_ms must be shorter than lease_ms")
	}
	if !a.Enabled {
		return nil
	}
	if a.DatabaseURL() == "" {
		return fmt.Errorf("async is enabled but %s is not set", a.databaseEnv())
	}
	if a.RedisURL() == "" {
		return fmt.Errorf("async is enabled but %s is not set", a.redisEnv())
	}
	return nil
}

func (a Async) databaseEnv() string {
	if a.DatabaseURLEnv == "" {
		return defaultDatabaseURLEnv
	}
	return a.DatabaseURLEnv
}

func (a Async) redisEnv() string {
	if a.RedisURLEnv == "" {
		return defaultRedisURLEnv
	}
	return a.RedisURLEnv
}

// DatabaseURL reads the PostgreSQL URL from the configured environment
// variable, which defaults to DATABASE_URL.
func (a Async) DatabaseURL() string { return os.Getenv(a.databaseEnv()) }

// RedisURL reads the Redis URL from the configured environment variable, which
// defaults to REDIS_URL.
func (a Async) RedisURL() string { return os.Getenv(a.redisEnv()) }

// WorkerCount is the number of worker goroutines to start, at least one.
func (a Async) WorkerCount() int {
	if a.Workers <= 0 {
		return 1
	}
	return a.Workers
}

// Lease is the visibility timeout for a claimed job.
func (a Async) Lease() time.Duration {
	if a.LeaseMS <= 0 {
		return defaultLease
	}
	return time.Duration(a.LeaseMS) * time.Millisecond
}

// Heartbeat is the lease renewal interval, defaulting to a third of the lease.
func (a Async) Heartbeat() time.Duration {
	if a.HeartbeatMS <= 0 {
		return a.Lease() / 3
	}
	return time.Duration(a.HeartbeatMS) * time.Millisecond
}

// ReaperInterval is how often expired leases are swept.
func (a Async) ReaperInterval() time.Duration {
	if a.ReaperIntervalMS <= 0 {
		return defaultReaperInterval
	}
	return time.Duration(a.ReaperIntervalMS) * time.Millisecond
}

// WorkerTTL is how long a silent worker remains in the registry.
func (a Async) WorkerTTL() time.Duration {
	if a.WorkerTTLMS <= 0 {
		return 4 * a.Lease()
	}
	return time.Duration(a.WorkerTTLMS) * time.Millisecond
}

// ReaperEnabled reports whether this node runs the recovery sweep. It defaults
// to true so a single-node deployment recovers without extra configuration.
func (a Async) ReaperEnabled() bool { return a.Reaper == nil || *a.Reaper }

// RetryDelay is the linear backoff unit between step attempts.
func (w Workflow) RetryDelay() time.Duration {
	return time.Duration(w.RetryDelayMS) * time.Millisecond
}

// Timeout bounds a single generation. Zero leaves the client unbounded.
func (a Agent) Timeout() time.Duration { return time.Duration(a.TimeoutMS) * time.Millisecond }
