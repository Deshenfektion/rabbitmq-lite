package api

import (
	"net/http"
	"time"

	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/message"
)

type consumeRequest struct {
	Consumer string `json:"consumer"`
	Limit    int    `json:"limit"`
	Lease    string `json:"lease"`
}

type consumeResponse struct {
	Queue    string             `json:"queue"`
	Consumer string             `json:"consumer"`
	Lease    string             `json:"lease"`
	Messages []*message.Message `json:"messages"`
}

type nackRequest struct {
	Reason  string `json:"reason"`
	Requeue bool   `json:"requeue"`
}

func (s *Server) handleConsume(w http.ResponseWriter, r *http.Request) {
	request := consumeRequest{Limit: intQuery(r, "limit", 0)}

	if r.ContentLength > 0 && !decodeBody(w, r, &request) {
		return
	}

	lease, err := optionalDuration(request.Lease)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	queue := r.PathValue("name")

	claimed, err := s.engine.Pull(r.Context(), engine.PullRequest{
		Queue:    queue,
		Consumer: request.Consumer,
		Limit:    request.Limit,
		Lease:    lease,
	})
	if err != nil {
		s.fail(w, r, err)
		return
	}

	response := consumeResponse{
		Queue:    queue,
		Consumer: request.Consumer,
		Messages: claimed,
	}

	if lease > 0 {
		response.Lease = lease.String()
	} else if definition, err := s.engine.Registry().Queue(queue); err == nil {
		response.Lease = definition.VisibilityTimeout.String()
	}

	if len(claimed) == 0 {
		writeJSON(w, http.StatusNoContent, nil)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.Ack(r.Context(), r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message_id":      r.PathValue("id"),
		"state":           message.StateAcknowledged,
		"acknowledged_at": s.clock().Format(time.RFC3339Nano),
	})
}

func (s *Server) handleNack(w http.ResponseWriter, r *http.Request) {
	var request nackRequest

	if r.ContentLength > 0 && !decodeBody(w, r, &request) {
		return
	}

	id := r.PathValue("id")

	if err := s.engine.Nack(r.Context(), id, request.Reason, request.Requeue); err != nil {
		s.fail(w, r, err)
		return
	}

	msg, err := s.engine.Message(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"message_id":   id,
		"state":        msg.State,
		"attempts":     msg.Attempts,
		"available_at": msg.AvailableAt,
	})
}
