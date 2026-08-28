package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type Config struct {
	Server   Server               `yaml:"server"`
	Workflow Workflow             `yaml:"workflow"`
	Agents   map[string]EinoAgent `yaml:"agents"`
}
type Server struct {
	Address string `yaml:"address"`
}
type Workflow struct {
	MaxRetries   int `yaml:"max_retries"`
	RetryDelayMS int `yaml:"retry_delay_ms"`
}
type EinoAgent struct {
	Model         string `yaml:"model"`
	APIKeyEnv     string `yaml:"api_key_env"`
	BaseURL       string `yaml:"base_url"`
	Instruction   string `yaml:"instruction"`
	MaxTokens     int    `yaml:"max_tokens"`
	MaxIterations int    `yaml:"max_iterations"`
	TimeoutMS     int    `yaml:"timeout_ms"`
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
	for name, agent := range cfg.Agents {
		if name == "" || agent.Model == "" {
			return Config{}, fmt.Errorf("agents must have a name and model")
		}
		if agent.MaxTokens < 0 || agent.MaxIterations < 0 || agent.TimeoutMS < 0 {
			return Config{}, fmt.Errorf("agent %q values must be non-negative", name)
		}
	}
	return cfg, nil
}
func (w Workflow) RetryDelay() time.Duration { return time.Duration(w.RetryDelayMS) * time.Millisecond }
func (a EinoAgent) Timeout() time.Duration   { return time.Duration(a.TimeoutMS) * time.Millisecond }
