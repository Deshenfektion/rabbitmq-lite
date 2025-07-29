package api

import (
	"net/http"
)

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
