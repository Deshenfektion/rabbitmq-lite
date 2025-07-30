package api

import (
	"encoding/json"
	"net/http"

	"github.com/deshenrao/rabbitmq-lite/internal/message"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type publishRequest struct {
	Exchange   string            `json:"exchange"`
	RoutingKey string            `json:"routing_key"`
	Payload    json.RawMessage   `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
	Schema     string            `json:"schema,omitempty"`
}

type messageView struct {
	*message.Message

	History []storage.HistoryEvent `json:"history,omitempty"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	var request publishRequest
	if !decodeBody(w, r, &request) {
		return
	}

	if len(request.Payload) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "payload must not be empty")
		return
	}

	result, err := s.engine.Publish(r.Context(), message.Publication{
		Exchange:   request.Exchange,
		RoutingKey: request.RoutingKey,
		Payload:    request.Payload,
		Headers:    request.Headers,
		Schema:     request.Schema,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request) {
	msg, err := s.engine.Message(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	view := messageView{Message: msg}

	if history, err := s.engine.History(r.Context(), msg.ID); err == nil {
		view.History = history
	}

	writeJSON(w, http.StatusOK, view)
}

func (s *Server) handleGetMessageHistory(w http.ResponseWriter, r *http.Request) {
	history, err := s.engine.History(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"history": history})
}
