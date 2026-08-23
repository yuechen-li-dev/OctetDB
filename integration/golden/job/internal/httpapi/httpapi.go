package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"example.com/octetdb-golden/job/internal/service"
	"github.com/go-chi/chi/v5"
)

func New(s *service.Service) http.Handler {
	r := chi.NewRouter()
	r.Post("/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		d, err := s.Create(r.Context(), commandID, chi.URLParam(r, "id"))
		writeDecision(w, d, err, http.StatusCreated)
	})
	r.Get("/jobs/ready", func(w http.ResponseWriter, r *http.Request) {
		limit := 10
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed <= 0 {
				http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
				return
			}
			limit = parsed
		}
		jobs, err := s.ListReady(r.Context(), limit)
		if err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, jobs)
	})
	r.Get("/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		job, err := s.Get(r.Context(), chi.URLParam(r, "id"))
		if errors.Is(err, service.ErrNotFound) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err != nil {
			http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, job)
	})
	r.Post("/jobs/{id}/claim", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		var body struct {
			Owner string `json:"owner"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		d, err := s.Claim(r.Context(), commandID, chi.URLParam(r, "id"), body.Owner)
		writeDecision(w, d, err, http.StatusOK)
	})
	r.Post("/jobs/{id}/complete", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		var body struct {
			Owner string `json:"owner"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		d, err := s.Complete(r.Context(), commandID, chi.URLParam(r, "id"), body.Owner)
		writeDecision(w, d, err, http.StatusOK)
	})
	r.Post("/jobs/{id}/fail", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		var body struct {
			Owner  string `json:"owner"`
			Reason string `json:"reason"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		d, err := s.Fail(r.Context(), commandID, chi.URLParam(r, "id"), body.Owner, body.Reason)
		writeDecision(w, d, err, http.StatusOK)
	})
	r.Post("/jobs/{id}/requeue", func(w http.ResponseWriter, r *http.Request) {
		commandID, ok := idempotencyKey(w, r)
		if !ok {
			return
		}
		d, err := s.Requeue(r.Context(), commandID, chi.URLParam(r, "id"))
		writeDecision(w, d, err, http.StatusOK)
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
