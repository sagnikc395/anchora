package main

import (
	"fmt"
	"os"

	"github.com/goccy/go-yaml"
)

type ProviderConfig struct {
	Name      string `yaml:"name"`
	BaseURL   string `yaml:"base_url"`
	Model     string `yaml:"model"`
	MaxTokens int64  `yaml:"max_tokens"`
}

type WorkflowConfig struct {
	MaxRetries   int `yaml:"max_retries"`
	RetryDelayMS int `yaml:"retry_delay_ms"`
}

type Config struct {
	Provider ProviderConfig `yaml:"provider"`
	Workflow WorkflowConfig `yaml:"workflow"`
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	if cfg.Provider.Name == "" {
		return nil, fmt.Errorf("config: provider.name is required")
	}
	if cfg.Provider.BaseURL == "" {
		return nil, fmt.Errorf("config: provider.base_url is required")
	}
	if cfg.Provider.Model == "" {
		return nil, fmt.Errorf("config: provider.model is required")
	}

	return &cfg, nil
}

// APIKey resolves the correct env variable based on provider name.
func (c *ProviderConfig) APIKey() (string, error) {
	envVars := map[string]string{
		"groq":        "GROQ_API_KEY",
		"huggingface": "HF_API_KEY",
	}

	envKey, ok := envVars[c.Name]
	if !ok {
		return "", fmt.Errorf("unknown provider %q -- add its env var to APIKey()", c.Name)
	}

	val := os.Getenv(envKey)
	if val == "" {
		return "", fmt.Errorf("env var %q is not set", envKey)
	}

	return val, nil
}
