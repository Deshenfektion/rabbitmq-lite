package api

import (
	"net/http"

	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

func (s *Server) handleListDeadLetters(w http.ResponseWriter, r *http.Request) {
	filter := storage.DeadLetterFilter{
		Queue:  r.URL.Query().Get("queue"),
		Limit:  intQuery(r, "limit", 0),
		Offset: intQuery(r, "offset", 0),
	}

	entries, err := s.engine.DeadLetters(r.Context(), filter)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	total, err := s.engine.CountDeadLetters(r.Context(), filter.Queue)
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"dead_letters": entries,
		"total":        total,
		"limit":        filter.Normalise().Limit,
		"offset":       filter.Normalise().Offset,
	})
}

func (s *Server) handleGetDeadLetter(w http.ResponseWriter, r *http.Request) {
	entry, err := s.engine.DeadLetter(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusOK, entry)
}

func (s *Server) handleRetryDeadLetter(w http.ResponseWriter, r *http.Request) {
	result, err := s.engine.ReplayDeadLetter(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}

	writeJSON(w, http.StatusAccepted, result)
}

func (s *Server) handleDeleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.DiscardDeadLetter(r.Context(), r.PathValue("id")); err != nil {
		s.fail(w, r, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleListSchemas(w http.ResponseWriter, r *http.Request) {
	if s.schemas == nil {
		writeJSON(w, http.StatusOK, map[string]any{"schemas": []any{}})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"schemas": s.schemas.Definitions()})
}

func (s *Server) handleListConsumers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"consumers": s.engine.Consumers()})
}
