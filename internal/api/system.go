package api

import (
	"log/slog"
	"net/http"
)

const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"version": s.version,
		"time":    s.clock(),
	})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	queues := s.engine.Registry().QueueNames()

	for _, name := range queues {
		if _, err := s.engine.Depth(r.Context(), name); err != nil {
			writeError(w, http.StatusServiceUnavailable, "storage_unavailable", err.Error())
			return
		}

		break
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ready",
		"queues": len(queues),
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if s.metrics == nil {
		writeError(w, http.StatusNotFound, "not_found", "metrics are not enabled")
		return
	}

	s.engine.RefreshQueueDepths(r.Context())

	w.Header().Set("Content-Type", prometheusContentType)

	if err := s.metrics.Write(w); err != nil {
		s.logger.Error("failed to write metrics", slog.String("error", err.Error()))
	}
}
