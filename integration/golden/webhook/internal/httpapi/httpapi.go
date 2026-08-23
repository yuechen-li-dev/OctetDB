package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"example.com/octetdb-golden/webhook/internal/service"
	"github.com/go-chi/chi/v5"
)

func New(s *service.Service) http.Handler {
	r := chi.NewRouter()
	r.Post("/webhooks", func(w http.ResponseWriter, r *http.Request) {
		var event service.Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if event.ID == "" {
			http.Error(w, "event id is required", http.StatusBadRequest)
			return
		}
		decision, err := s.Process(r.Context(), event)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(decision)
	})
	return r
}
