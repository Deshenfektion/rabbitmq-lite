package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "broker.yaml")

	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	return path
}

func TestLoadWithoutPathReturnsDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Server.Address != ":8080" || cfg.Storage.Driver != "sqlite" {
		t.Fatalf("unexpected defaults %+v", cfg)
	}
}

func TestLoadParsesDurationsAndTopology(t *testing.T) {
	path := writeConfig(t, `
server:
  address: "127.0.0.1:9090"
  read_timeout: 3s
storage:
  driver: memory
  dsn: ""
retry:
  max_attempts: 7
  initial_interval: 250ms
  max_interval: 30s
  multiplier: 3.0
topology:
  exchanges:
    - name: erp.events
      kind: topic
      durable: true
  queues:
    - name: invoice-processing
      durable: true
      max_attempts: 5
      visibility_timeout: 45s
      schema: invoice
  bindings:
    - exchange: erp.events
      queue: invoice-processing
      routing_key: invoice.#
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Server.Address != "127.0.0.1:9090" {
		t.Fatalf("unexpected address %s", cfg.Server.Address)
	}

	if cfg.Server.ReadTimeout.Duration() != 3*time.Second {
		t.Fatalf("unexpected read timeout %s", cfg.Server.ReadTimeout)
	}

	if cfg.Server.WriteTimeout.Duration() != 15*time.Second {
		t.Fatalf("expected unspecified fields to keep their defaults, got %s", cfg.Server.WriteTimeout)
	}

	policy := cfg.RetryPolicy()
	if policy.MaxAttempts != 7 || policy.InitialInterval != 250*time.Millisecond || policy.Multiplier != 3 {
		t.Fatalf("unexpected retry policy %+v", policy)
	}

	if len(cfg.Topology.Queues) != 1 || cfg.Topology.Queues[0].VisibilityTimeout.Duration() != 45*time.Second {
		t.Fatalf("unexpected topology %+v", cfg.Topology)
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := writeConfig(t, "server:\n  addres: \":9090\"\n")

	if _, err := Load(path); err == nil {
		t.Fatal("expected a typo in a configuration key to be reported")
	}
}

func TestLoadRejectsInvalidDurations(t *testing.T) {
	path := writeConfig(t, "server:\n  read_timeout: \"ten seconds\"\n")

	if _, err := Load(path); err == nil {
		t.Fatal("expected an invalid duration to be reported")
	}
}

func TestValidateRejectsUnsupportedValues(t *testing.T) {
	cases := map[string]func(*Config){
		"empty address":    func(c *Config) { c.Server.Address = "" },
		"unknown driver":   func(c *Config) { c.Storage.Driver = "postgres" },
		"missing dsn":      func(c *Config) { c.Storage.DSN = "" },
		"zero attempts":    func(c *Config) { c.Engine.MaxAttempts = 0 },
		"bad log level":    func(c *Config) { c.Logging.Level = "chatty" },
		"bad log format":   func(c *Config) { c.Logging.Format = "xml" },
		"broken retry":     func(c *Config) { c.Retry.Multiplier = 0.5 },
		"nameless queue":   func(c *Config) { c.Topology.Queues = []QueueDeclaration{{}} },
		"kindless topic":   func(c *Config) { c.Topology.Exchanges = []ExchangeDeclaration{{Name: "x"}} },
		"negative jitter":  func(c *Config) { c.Retry.JitterFraction = -1 },
		"excessive jitter": func(c *Config) { c.Retry.JitterFraction = 2 },
	}

	for name, mutate := range cases {
		cfg := Default()
		mutate(&cfg)

		if err := cfg.Validate(); err == nil {
			t.Errorf("%s: expected validation to fail", name)
		}
	}
}

func TestEnvironmentOverridesFile(t *testing.T) {
	path := writeConfig(t, "server:\n  address: \":8080\"\n")

	t.Setenv(envAddress, ":7070")
	t.Setenv(envStorageDSN, "/var/lib/broker.db")
	t.Setenv(envLogLevel, "debug")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if cfg.Server.Address != ":7070" {
		t.Fatalf("expected the environment to win, got %s", cfg.Server.Address)
	}

	if cfg.Storage.DSN != "/var/lib/broker.db" || cfg.Logging.Level != "debug" {
		t.Fatalf("unexpected overrides %+v", cfg)
	}
}

func TestShippedConfigurationIsValid(t *testing.T) {
	cfg, err := Load(filepath.Join("..", "..", "config", "broker.yaml"))
	if err != nil {
		t.Fatalf("load shipped configuration: %v", err)
	}

	if len(cfg.Topology.Queues) == 0 || len(cfg.Topology.Bindings) == 0 {
		t.Fatal("expected the shipped configuration to declare a topology")
	}
}
