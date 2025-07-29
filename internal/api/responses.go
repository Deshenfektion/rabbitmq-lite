package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/deshenrao/rabbitmq-lite/internal/broker"
	"github.com/deshenrao/rabbitmq-lite/internal/engine"
	"github.com/deshenrao/rabbitmq-lite/internal/schema"
	"github.com/deshenrao/rabbitmq-lite/internal/storage"
)

type errorResponse struct {
	Error      string             `json:"error"`
	Message    string             `json:"message"`
	Violations []schema.Violation `json:"violations,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)

	if body == nil {
		return
	}

	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)

	_ = encoder.Encode(body)
}

func writeError(w http.ResponseWriter, status int, code, detail string) {
	writeJSON(w, status, errorResponse{Error: code, Message: detail})
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code := classify(err)

	if validation, ok := schema.AsValidationError(err); ok {
		writeJSON(w, status, errorResponse{
			Error:      code,
			Message:    validation.Error(),
			Violations: validation.Violations,
		})

		return
	}

	if status >= http.StatusInternalServerError {
		s.logger.Error("request failed",
			slog.String("request_id", RequestIDFrom(r.Context())),
			slog.String("path", r.URL.Path),
			slog.String("error", err.Error()),
		)
	}

	writeError(w, status, code, err.Error())
}

func classify(err error) (int, string) {
	var (
		field      *broker.FieldError
		stateErr   *storage.StateError
		validation *schema.ValidationError
	)

	switch {
	case errors.As(err, &validation):
		return http.StatusUnprocessableEntity, "schema_validation_failed"
	case errors.Is(err, schema.ErrSchemaNotFound):
		return http.StatusUnprocessableEntity, "unknown_schema"
	case errors.As(err, &field):
		return http.StatusBadRequest, "invalid_request"
	case errors.Is(err, broker.ErrUnroutable):
		return http.StatusUnprocessableEntity, "unroutable"
	case errors.Is(err, broker.ErrQueueExists),
		errors.Is(err, broker.ErrExchangeExists),
		errors.Is(err, broker.ErrQueueInUse),
		errors.Is(err, engine.ErrConsumerExists),
		errors.Is(err, storage.ErrConflict):
		return http.StatusConflict, "conflict"
	case errors.As(err, &stateErr):
		return http.StatusConflict, "invalid_state"
	case errors.Is(err, broker.ErrQueueNotFound),
		errors.Is(err, broker.ErrExchangeNotFound),
		errors.Is(err, broker.ErrBindingNotFound),
		errors.Is(err, broker.ErrMessageNotFound),
		errors.Is(err, engine.ErrConsumerNotFound),
		errors.Is(err, storage.ErrNotFound):
		return http.StatusNotFound, "not_found"
	default:
		return http.StatusInternalServerError, "internal_error"
	}
}

func decodeBody(w http.ResponseWriter, r *http.Request, target any) bool {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", fmt.Sprintf("request body could not be decoded: %v", err))
		return false
	}

	return true
}

func intQuery(r *http.Request, name string, fallback int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return fallback
	}

	return parsed
}
