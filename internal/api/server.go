package api

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
)

const defaultMaxBodyBytes = 1 << 20

type Options struct {
	Engine       *engine.Engine
	Schemas      *schema.Registry
	Logger       *slog.Logger
	MaxBodyBytes int64
	Version      string
	Clock        func() time.Time
}

type Server struct {
	engine       *engine.Engine
	schemas      *schema.Registry
	logger       *slog.Logger
	maxBodyBytes int64
	version      string
	clock        func() time.Time
	handler      http.Handler
}

func New(opts Options) *Server {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}

	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = defaultMaxBodyBytes
	}

	if opts.Clock == nil {
		opts.Clock = func() time.Time { return time.Now().UTC() }
	}

	if opts.Version == "" {
		opts.Version = "dev"
	}

	server := &Server{
		engine:       opts.Engine,
		schemas:      opts.Schemas,
		logger:       opts.Logger,
		maxBodyBytes: opts.MaxBodyBytes,
		version:      opts.Version,
		clock:        opts.Clock,
	}

	server.handler = server.routes()

	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)

	mux.HandleFunc("POST /api/v1/exchanges", s.handleDeclareExchange)
	mux.HandleFunc("GET /api/v1/exchanges", s.handleListExchanges)
	mux.HandleFunc("GET /api/v1/exchanges/{name}", s.handleGetExchange)

	mux.HandleFunc("POST /api/v1/queues", s.handleDeclareQueue)
	mux.HandleFunc("GET /api/v1/queues", s.handleListQueues)
	mux.HandleFunc("GET /api/v1/queues/{name}", s.handleGetQueue)
	mux.HandleFunc("DELETE /api/v1/queues/{name}", s.handleDeleteQueue)
	mux.HandleFunc("POST /api/v1/queues/{name}/purge", s.handlePurgeQueue)

	mux.HandleFunc("POST /api/v1/messages", s.handlePublish)
	mux.HandleFunc("GET /api/v1/messages/{id}", s.handleGetMessage)
	mux.HandleFunc("GET /api/v1/messages/{id}/history", s.handleGetMessageHistory)

	mux.HandleFunc("POST /api/v1/bindings", s.handleBind)
	mux.HandleFunc("GET /api/v1/bindings", s.handleListBindings)
	mux.HandleFunc("DELETE /api/v1/bindings", s.handleUnbind)

	return s.wrap(mux)
}

func (s *Server) wrap(next http.Handler) http.Handler {
	return recoverPanics(s.logger)(
		requestID()(
			logRequests(s.logger)(
				limitBody(s.maxBodyBytes)(next),
			),
		),
	)
}
