package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/api"
	"github.com/deshenrao/rabbitmq-lite/internal/config"
	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/logging"
	"github.com/deshenrao/rabbitmq-lite/internal/metrics"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/memory"
	"github.com/deshenrao/rabbitmq-lite/internal/storage/sqlite"
)

type Application struct {
	Config  config.Config
	Logger  *slog.Logger
	Engine  *engine.Engine
	Store   storage.Store
	Schemas *schema.Registry
	Metrics *metrics.Collector

	server *http.Server
}

func Build(ctx context.Context, cfg config.Config, version string) (*Application, error) {
	logger, err := logging.New(os.Stdout, logging.Options{
		Level:   cfg.Logging.Level,
		Format:  cfg.Logging.Format,
		Service: "rabbitmq-lite",
		Version: version,
	})
	if err != nil {
		return nil, err
	}

	store, err := openStore(ctx, cfg.Storage)
	if err != nil {
		return nil, err
	}

	schemas, err := loadSchemas(cfg.Schemas.Directory, logger)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	collector := metrics.NewCollector(metrics.NewRegistry())

	instance, err := engine.New(engine.Options{
		Store:           store,
		Logger:          logger,
		Retry:           cfg.RetryPolicy(),
		Schemas:         schemas,
		Metrics:         collector,
		ReclaimInterval: cfg.Engine.ReclaimInterval.Duration(),
	})
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	if err := instance.Restore(ctx); err != nil {
		_ = store.Close()
		return nil, err
	}

	if err := declareTopology(ctx, instance, cfg); err != nil {
		_ = store.Close()
		return nil, err
	}

	handler := api.New(api.Options{
		Engine:       instance,
		Schemas:      schemas,
		Metrics:      collector.Registry(),
		Logger:       logger,
		MaxBodyBytes: cfg.MaxBodyBytes(),
		Version:      version,
	})

	return &Application{
		Config:  cfg,
		Logger:  logger,
		Engine:  instance,
		Store:   store,
		Schemas: schemas,
		Metrics: collector,
		server: &http.Server{
			Addr:         cfg.Server.Address,
			Handler:      handler,
			ReadTimeout:  cfg.Server.ReadTimeout.Duration(),
			WriteTimeout: cfg.Server.WriteTimeout.Duration(),
			IdleTimeout:  cfg.Server.IdleTimeout.Duration(),
		},
	}, nil
}

func (a *Application) Run(ctx context.Context) error {
	a.Engine.Start(ctx)

	listening := make(chan error, 1)

	go func() {
		a.Logger.Info("broker listening",
			slog.String("address", a.Config.Server.Address),
			slog.String("storage", a.Config.Storage.Driver),
		)

		if err := a.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listening <- err
			return
		}

		listening <- nil
	}()

	select {
	case err := <-listening:
		return err
	case <-ctx.Done():
		return a.Shutdown()
	}
}

func (a *Application) Shutdown() error {
	timeout := a.Config.Server.ShutdownTimeout.Duration()
	if timeout <= 0 {
		timeout = 20 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	a.Logger.Info("shutting down", slog.Duration("grace_period", timeout))

	var failures []error

	if err := a.server.Shutdown(ctx); err != nil {
		failures = append(failures, fmt.Errorf("http server: %w", err))
	}

	if err := a.Engine.Shutdown(ctx); err != nil {
		failures = append(failures, fmt.Errorf("engine: %w", err))
	}

	if err := a.Store.Close(); err != nil {
		failures = append(failures, fmt.Errorf("storage: %w", err))
	}

	return errors.Join(failures...)
}

func openStore(ctx context.Context, cfg config.Storage) (storage.Store, error) {
	switch cfg.Driver {
	case "memory":
		return memory.New(), nil
	case "sqlite":
		if directory := filepath.Dir(cfg.DSN); directory != "." && directory != "" {
			if err := os.MkdirAll(directory, 0o750); err != nil {
				return nil, fmt.Errorf("app: create storage directory: %w", err)
			}
		}

		return sqlite.Open(ctx, sqlite.Options{
			DSN:          cfg.DSN,
			BusyTimeout:  cfg.BusyTimeout.Duration(),
			JournalMode:  cfg.JournalMode,
			Synchronous:  cfg.Synchronous,
			MaxOpenConns: cfg.MaxOpenConns,
		})
	default:
		return nil, fmt.Errorf("app: unsupported storage driver %q", cfg.Driver)
	}
}

func loadSchemas(directory string, logger *slog.Logger) (*schema.Registry, error) {
	registry := schema.NewRegistry()

	if directory == "" {
		return registry, nil
	}

	if _, err := os.Stat(directory); errors.Is(err, os.ErrNotExist) {
		logger.Warn("schema directory not found, validation disabled",
			slog.String("directory", directory))

		return registry, nil
	}

	loaded, err := registry.LoadDirectory(directory)
	if err != nil {
		return nil, err
	}

	for _, definition := range loaded {
		logger.Info("schema registered",
			slog.String("schema", definition.Name),
			slog.Int("version", definition.Version),
		)
	}

	return registry, nil
}
