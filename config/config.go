package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Config struct {
	Server   Server                      `yaml:"server"`
	Workflow Workflow                    `yaml:"workflow"`
	Async    Async                       `yaml:"async"`
	Agents   map[string]HuggingFaceAgent `yaml:"agents"`
}
type Async struct {
	Enabled        bool   `yaml:"enabled"`
	DatabaseURLEnv string `yaml:"database_url_env"`
	RedisURLEnv    string `yaml:"redis_url_env"`
	QueueName      string `yaml:"queue_name"`
	Workers        int    `yaml:"workers"`
}
type Server struct {
	Address string `yaml:"address"`
}
type Workflow struct {
	MaxRetries   int `yaml:"max_retries"`
	RetryDelayMS int `yaml:"retry_delay_ms"`
}
type HuggingFaceAgent struct {
	ModelID     string `yaml:"model_id"`
	TokenEnv    string `yaml:"token_env"`
	Instruction string `yaml:"instruction"`
	MaxTokens   int    `yaml:"max_tokens"`
	TimeoutMS   int    `yaml:"timeout_ms"`
}

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
		cfg.Server.Address = ":8080"
	}
	if cfg.Workflow.MaxRetries < 0 || cfg.Workflow.RetryDelayMS < 0 {
		return Config{}, fmt.Errorf("workflow retry values must be non-negative")
	}
	if cfg.Async.Workers < 0 {
		return Config{}, fmt.Errorf("async workers must be non-negative")
	}
	for name, agent := range cfg.Agents {
		if name == "" || agent.ModelID == "" {
			return Config{}, fmt.Errorf("agents must have a name and model_id")
		}
		if agent.MaxTokens < 0 || agent.TimeoutMS < 0 {
			return Config{}, fmt.Errorf("agent %q values must be non-negative", name)
		}
	}
	return cfg, nil
}
func (a Async) DatabaseURL() string {
	if a.DatabaseURLEnv == "" {
		a.DatabaseURLEnv = "DATABASE_URL"
	}
	return os.Getenv(a.DatabaseURLEnv)
}
func (a Async) RedisURL() string {
	if a.RedisURLEnv == "" {
		a.RedisURLEnv = "REDIS_URL"
	}
	return os.Getenv(a.RedisURLEnv)
}
func (a Async) WorkerCount() int {
	if a.Workers == 0 {
		return 1
	}
	return a.Workers
}
func (w Workflow) RetryDelay() time.Duration { return time.Duration(w.RetryDelayMS) * time.Millisecond }
func (a HuggingFaceAgent) Timeout() time.Duration {
	return time.Duration(a.TimeoutMS) * time.Millisecond
}
