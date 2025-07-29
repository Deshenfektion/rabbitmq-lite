package api

import (
	"net/http"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type exchangeRequest struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"`
	Durable bool   `json:"durable"`
}

type queueRequest struct {
	Name              string `json:"name"`
	Durable           bool   `json:"durable"`
	MaxAttempts       int    `json:"max_attempts"`
	VisibilityTimeout string `json:"visibility_timeout"`
	Schema            string `json:"schema"`
}

type bindingRequest struct {
	Exchange   string `json:"exchange"`
	Queue      string `json:"queue"`
	RoutingKey string `json:"routing_key"`
}

type queueView struct {
	Name              string        `json:"name"`
	Durable           bool          `json:"durable"`
	MaxAttempts       int           `json:"max_attempts"`
	VisibilityTimeout string        `json:"visibility_timeout"`
	Schema            string        `json:"schema,omitempty"`
	CreatedAt         time.Time     `json:"created_at"`
	Depth             storage.Depth `json:"depth"`
	DeadLettered      int           `json:"dead_lettered"`
}

func (s *Server) handleDeclareExchange(w http.ResponseWriter, r *http.Request) {
	var request exchangeRequest
	if !decodeBody(w, r, &request) {
		return
	}

	kind, err := broker.ParseExchangeKind(request.Kind)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	exchange, err := s.engine.DeclareExchange(r.Context(), broker.ExchangeSpec{
		Name:    request.Name,
		Kind:    kind,
		Durable: request.Durable,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, exchange)
}

func (s *Server) handleListExchanges(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"exchanges": s.engine.Registry().Exchanges()})
}

func (s *Server) handleGetExchange(w http.ResponseWriter, r *http.Request) {
	exchange, err := s.engine.Registry().Exchange(r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"exchange": exchange,
		"bindings": s.engine.Registry().Bindings(exchange.Name),
	})
}

func (s *Server) handleDeclareQueue(w http.ResponseWriter, r *http.Request) {
	var request queueRequest
	if !decodeBody(w, r, &request) {
		return
	}

	visibility, err := optionalDuration(request.VisibilityTimeout)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	if request.Schema != "" && s.schemas != nil && !s.schemas.Has(request.Schema) {
		writeError(w, http.StatusUnprocessableEntity, "unknown_schema",
			"schema "+request.Schema+" is not registered")

		return
	}

	queue, err := s.engine.DeclareQueue(r.Context(), broker.QueueSpec{
		Name:              request.Name,
		Durable:           request.Durable,
		MaxAttempts:       request.MaxAttempts,
		VisibilityTimeout: visibility,
		Schema:            request.Schema,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, s.describeQueue(r, queue))
}

func (s *Server) handleListQueues(w http.ResponseWriter, r *http.Request) {
	queues := s.engine.Registry().Queues()
	views := make([]queueView, 0, len(queues))

	for _, queue := range queues {
		views = append(views, s.describeQueue(r, queue))
	}

	writeJSON(w, http.StatusOK, map[string]any{"queues": views})
}

func (s *Server) handleGetQueue(w http.ResponseWriter, r *http.Request) {
	queue, err := s.engine.Registry().Queue(r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, s.describeQueue(r, queue))
}

func (s *Server) handleDeleteQueue(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.DeleteQueue(r.Context(), r.PathValue("name")); err != nil {
		s.fail(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handlePurgeQueue(w http.ResponseWriter, r *http.Request) {
	purged, err := s.engine.Purge(r.Context(), r.PathValue("name"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"purged": purged})
}

func (s *Server) handleBind(w http.ResponseWriter, r *http.Request) {
	var request bindingRequest
	if !decodeBody(w, r, &request) {
		return
	}

	binding, err := s.engine.Bind(r.Context(), broker.BindingSpec{
		Exchange:   request.Exchange,
		Queue:      request.Queue,
		RoutingKey: request.RoutingKey,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, binding)
}

func (s *Server) handleListBindings(w http.ResponseWriter, r *http.Request) {
	exchange := r.URL.Query().Get("exchange")

	if exchange != "" {
		writeJSON(w, http.StatusOK, map[string]any{"bindings": s.engine.Registry().Bindings(exchange)})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"bindings": s.engine.Registry().AllBindings()})
}

func (s *Server) handleUnbind(w http.ResponseWriter, r *http.Request) {
	var request bindingRequest
	if !decodeBody(w, r, &request) {
		return
	}

	if err := s.engine.Unbind(r.Context(), broker.BindingSpec{
		Exchange:   request.Exchange,
		Queue:      request.Queue,
		RoutingKey: request.RoutingKey,
	}); err != nil {
		s.fail(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) describeQueue(r *http.Request, queue *broker.Queue) queueView {
	view := queueView{
		Name:              queue.Name,
		Durable:           queue.Durable,
		MaxAttempts:       queue.MaxAttempts,
		VisibilityTimeout: queue.VisibilityTimeout.String(),
		Schema:            queue.Schema,
		CreatedAt:         queue.CreatedAt,
	}

	if depth, err := s.engine.Depth(r.Context(), queue.Name); err == nil {
		view.Depth = depth
	}

	if count, err := s.engine.CountDeadLetters(r.Context(), queue.Name); err == nil {
		view.DeadLettered = count
	}

	return view
}

func optionalDuration(raw string) (time.Duration, error) {
	if raw == "" {
		return 0, nil
	}

	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, &broker.FieldError{Field: "visibility_timeout", Reason: "must be a duration such as 30s"}
	}

	return parsed, nil
}
