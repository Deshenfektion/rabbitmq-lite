package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/deshenrao/rabbitmq-lite/internal/logging"
	"github.com/deshenrao/rabbitmq-lite/internal/retry"
)

const (
	envAddress    = "RABBITMQ_LITE_ADDRESS"
	envStorageDSN = "RABBITMQ_LITE_STORAGE_DSN"
	envLogLevel   = "RABBITMQ_LITE_LOG_LEVEL"
	envLogFormat  = "RABBITMQ_LITE_LOG_FORMAT"
)

type Config struct {
	Server   Server   `yaml:"server"`
	Storage  Storage  `yaml:"storage"`
	Engine   Engine   `yaml:"engine"`
	Retry    Retry    `yaml:"retry"`
	Schemas  Schemas  `yaml:"schemas"`
	Logging  Logging  `yaml:"logging"`
	Topology Topology `yaml:"topology"`
}

type Server struct {
	Address         string   `yaml:"address"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
	MaxBodyBytes    int64    `yaml:"max_body_bytes"`
}

type Storage struct {
	Driver       string   `yaml:"driver"`
	DSN          string   `yaml:"dsn"`
	BusyTimeout  Duration `yaml:"busy_timeout"`
	JournalMode  string   `yaml:"journal_mode"`
	Synchronous  string   `yaml:"synchronous"`
	MaxOpenConns int      `yaml:"max_open_conns"`
}

type Engine struct {
	ReclaimInterval   Duration `yaml:"reclaim_interval"`
	VisibilityTimeout Duration `yaml:"visibility_timeout"`
	MaxAttempts       int      `yaml:"max_attempts"`
}

type Retry struct {
	MaxAttempts     int      `yaml:"max_attempts"`
	InitialInterval Duration `yaml:"initial_interval"`
	MaxInterval     Duration `yaml:"max_interval"`
	Multiplier      float64  `yaml:"multiplier"`
	JitterFraction  float64  `yaml:"jitter_fraction"`
}

type Schemas struct {
	Directory string `yaml:"directory"`
}

type Logging struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

type Topology struct {
	Exchanges []ExchangeDeclaration `yaml:"exchanges"`
	Queues    []QueueDeclaration    `yaml:"queues"`
	Bindings  []BindingDeclaration  `yaml:"bindings"`
}

type ExchangeDeclaration struct {
	Name    string `yaml:"name"`
	Kind    string `yaml:"kind"`
	Durable bool   `yaml:"durable"`
}

type QueueDeclaration struct {
	Name              string   `yaml:"name"`
	Durable           bool     `yaml:"durable"`
	MaxAttempts       int      `yaml:"max_attempts"`
	VisibilityTimeout Duration `yaml:"visibility_timeout"`
	Schema            string   `yaml:"schema"`
}

type BindingDeclaration struct {
	Exchange   string `yaml:"exchange"`
	Queue      string `yaml:"queue"`
	RoutingKey string `yaml:"routing_key"`
}

func Default() Config {
	return Config{
		Server: Server{
			Address:         ":8080",
			ReadTimeout:     Duration(10 * time.Second),
			WriteTimeout:    Duration(15 * time.Second),
			IdleTimeout:     Duration(60 * time.Second),
			ShutdownTimeout: Duration(20 * time.Second),
			MaxBodyBytes:    1 << 20,
		},
		Storage: Storage{
			Driver:       "sqlite",
			DSN:          "data/broker.db",
			BusyTimeout:  Duration(5 * time.Second),
			JournalMode:  "WAL",
			Synchronous:  "NORMAL",
			MaxOpenConns: 1,
		},
		Engine: Engine{
			ReclaimInterval:   Duration(5 * time.Second),
			VisibilityTimeout: Duration(30 * time.Second),
			MaxAttempts:       3,
		},
		Retry: Retry{
			MaxAttempts:     3,
			InitialInterval: Duration(500 * time.Millisecond),
			MaxInterval:     Duration(5 * time.Minute),
			Multiplier:      2,
			JitterFraction:  0.2,
		},
		Schemas: Schemas{Directory: "schemas"},
		Logging: Logging{Level: "info", Format: "json"},
	}
}

func Load(path string) (Config, error) {
	cfg := Default()

	if path != "" {
		document, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("config: read %s: %w", path, err)
		}

		decoder := yaml.NewDecoder(strings.NewReader(string(document)))
		decoder.KnownFields(true)

		if err := decoder.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("config: parse %s: %w", path, err)
		}
	}

	cfg.applyEnvironment()

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c *Config) applyEnvironment() {
	if value := os.Getenv(envAddress); value != "" {
		c.Server.Address = value
	}

	if value := os.Getenv(envStorageDSN); value != "" {
		c.Storage.DSN = value
	}

	if value := os.Getenv(envLogLevel); value != "" {
		c.Logging.Level = value
	}

	if value := os.Getenv(envLogFormat); value != "" {
		c.Logging.Format = value
	}
}

func (c Config) Validate() error {
	if c.Server.Address == "" {
		return fmt.Errorf("config: server.address must not be empty")
	}

	switch c.Storage.Driver {
	case "sqlite", "memory":
	default:
		return fmt.Errorf("config: unsupported storage driver %q", c.Storage.Driver)
	}

	if c.Storage.Driver == "sqlite" && c.Storage.DSN == "" {
		return fmt.Errorf("config: storage.dsn is required for the sqlite driver")
	}

	if c.Engine.MaxAttempts < 1 {
		return fmt.Errorf("config: engine.max_attempts must be at least 1")
	}

	if c.Retry.JitterFraction < 0 || c.Retry.JitterFraction > 1 {
		return fmt.Errorf("config: retry.jitter_fraction must be within [0,1], got %.2f", c.Retry.JitterFraction)
	}

	if err := c.RetryPolicy().Validate(); err != nil {
		return err
	}

	switch strings.ToLower(c.Logging.Format) {
	case "json", "text":
	default:
		return fmt.Errorf("config: unsupported logging format %q", c.Logging.Format)
	}

	if _, err := logging.ParseLevel(c.Logging.Level); err != nil {
		return err
	}

	for _, exchange := range c.Topology.Exchanges {
		if exchange.Name == "" || exchange.Kind == "" {
			return fmt.Errorf("config: topology exchange requires a name and a kind")
		}
	}

	for _, queue := range c.Topology.Queues {
		if queue.Name == "" {
			return fmt.Errorf("config: topology queue requires a name")
		}
	}

	return nil
}

func (c Config) RetryPolicy() retry.Policy {
	return retry.Policy{
		MaxAttempts:     c.Retry.MaxAttempts,
		InitialInterval: orDefault(c.Retry.InitialInterval, 500*time.Millisecond),
		MaxInterval:     orDefault(c.Retry.MaxInterval, 5*time.Minute),
		Multiplier:      c.Retry.Multiplier,
		JitterFraction:  c.Retry.JitterFraction,
	}.WithDefaults()
}

func (c Config) MaxBodyBytes() int64 {
	if c.Server.MaxBodyBytes <= 0 {
		return 1 << 20
	}

	return c.Server.MaxBodyBytes
}

func (c Config) String() string {
	return fmt.Sprintf("address=%s driver=%s dsn=%s max_attempts=%s",
		c.Server.Address, c.Storage.Driver, c.Storage.DSN, strconv.Itoa(c.Retry.MaxAttempts))
}
