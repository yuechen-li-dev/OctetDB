package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"example.com/octetdb-golden/order/internal/service"
	"github.com/go-chi/chi/v5"
)

func New(s *service.Service) http.Handler {
	r := chi.NewRouter()
	r.Post("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		decision, err := s.Create(r.Context(), commandID, chi.URLParam(r, "id"))
		writeDecision(w, decision, err, http.StatusCreated)
	})
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		order, err := s.Get(r.Context(), chi.URLParam(r, "id"))
		if errors.Is(err, service.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, order)
	})
	r.Post("/orders/{id}/transitions", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		var body struct {
			To service.Status `json:"to"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		decision, err := s.Transition(r.Context(), commandID, chi.URLParam(r, "id"), body.To)
		writeDecision(w, decision, err, http.StatusOK)
	})
	return r
}
func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.Header.Get("Idempotency-Key")
	if id == "" {
		http.Error(w, "Idempotency-Key is required", http.StatusBadRequest)
		return "", false
	}
	return id, true
}
func writeDecision(w http.ResponseWriter, d service.Decision, err error, success int) {
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
		return
	}
	status := success
	if !d.Applied {
		status = http.StatusConflict
		if d.Code == "not_found" {
			status = http.StatusNotFound
		}
	}
	writeJSON(w, status, d)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
