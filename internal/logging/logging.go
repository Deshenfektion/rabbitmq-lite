package logging

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

const (
	FormatJSON = "json"
	FormatText = "text"
)

type Options struct {
	Level   string
	Format  string
	Service string
	Version string
}

func ParseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "debug":
		return slog.LevelDebug, nil
	case "", "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("logging: unsupported level %q", raw)
	}
}

func New(w io.Writer, opts Options) (*slog.Logger, error) {
	level, err := ParseLevel(opts.Level)
	if err != nil {
		return nil, err
	}

	handlerOptions := &slog.HandlerOptions{Level: level}

	var handler slog.Handler

	switch strings.ToLower(strings.TrimSpace(opts.Format)) {
	case "", FormatJSON:
		handler = slog.NewJSONHandler(w, handlerOptions)
	case FormatText:
		handler = slog.NewTextHandler(w, handlerOptions)
	default:
		return nil, fmt.Errorf("logging: unsupported format %q", opts.Format)
	}

	attributes := make([]slog.Attr, 0, 2)

	if opts.Service != "" {
		attributes = append(attributes, slog.String("service", opts.Service))
	}

	if opts.Version != "" {
		attributes = append(attributes, slog.String("version", opts.Version))
	}

	if len(attributes) > 0 {
		handler = handler.WithAttrs(attributes)
	}

	return slog.New(handler), nil
}
