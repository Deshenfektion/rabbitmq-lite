package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/deshenrao/rabbitmq-lite/internal/app"
	"github.com/deshenrao/rabbitmq-lite/internal/config"
)

var version = "dev"

func main() {
	configPath := flag.String("config", "config/broker.yaml", "path to the broker configuration file")
	showVersion := flag.Bool("version", false, "print the broker version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("rabbitmq-lite", version)
		return
	}

	if err := run(*configPath); err != nil {
		fmt.Fprintln(os.Stderr, "brokerd:", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.Build(ctx, cfg, version)
	if err != nil {
		return err
	}

	return application.Run(ctx)
}
