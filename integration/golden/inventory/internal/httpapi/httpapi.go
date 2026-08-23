package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"example.com/octetdb-golden/inventory/internal/service"
	"github.com/go-chi/chi/v5"
	"github.com/yuechen-li-dev/octetdb"
)

type API struct{ service *service.Service }

func New(s *service.Service) http.Handler {
	a := &API{service: s}
	r := chi.NewRouter()
	r.Post("/items/{id}", a.create)
	r.Get("/items/low-stock", a.lowStock)
	r.Get("/items/{id}", a.get)
	r.Post("/items/{id}/reservations", a.reserve)
	r.Post("/items/{id}/releases", a.release)
	return r
}
func (a *API) lowStock(w http.ResponseWriter, r *http.Request) {
	threshold, err := strconv.Atoi(r.URL.Query().Get("threshold"))
	if err != nil {
		http.Error(w, "threshold is required and must be an integer", http.StatusBadRequest)
		return
	}
	limit := 10
	if raw := r.URL.Query().Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil || limit <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
	}
	items, err := a.service.ListLowStock(r.Context(), threshold, limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, items)
}
func (a *API) create(w http.ResponseWriter, r *http.Request) {
	commandID, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		Stock int `json:"stock"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	decision, err := a.service.Create(r.Context(), commandID, service.Item{ID: chi.URLParam(r, "id"), Stock: body.Stock})
	writeDecision(w, decision, err, http.StatusCreated)
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	item, err := a.service.Get(r.Context(), chi.URLParam(r, "id"))
	if errors.Is(err, service.ErrNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}
func (a *API) reserve(w http.ResponseWriter, r *http.Request) {
	commandID, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		Quantity int `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	decision, err := a.service.Reserve(r.Context(), service.Command{ID: commandID, ItemID: chi.URLParam(r, "id"), Quantity: body.Quantity})
	writeDecision(w, decision, err, http.StatusOK)
}
func (a *API) release(w http.ResponseWriter, r *http.Request) {
	commandID, ok := idempotencyKey(w, r)
	if !ok {
		return
	}
	var body struct {
		Quantity int `json:"quantity"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	decision, err := a.service.Release(r.Context(), service.Command{ID: commandID, ItemID: chi.URLParam(r, "id"), Quantity: body.Quantity})
	writeDecision(w, decision, err, http.StatusOK)
}
func writeDecision[T any](w http.ResponseWriter, decision service.Decision[T], err error, success int) {
	if err != nil {
		writeError(w, err)
		return
	}
	if decision.Applied {
		writeJSON(w, success, decision)
		return
	}
	status := http.StatusConflict
	if decision.Code == "not_found" {
		status = http.StatusNotFound
	}
	if decision.Code == "invalid_item" || decision.Code == "invalid_quantity" {
		status = http.StatusBadRequest
	}
	writeJSON(w, status, decision)
}
func idempotencyKey(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.Header.Get("Idempotency-Key")
	if id == "" {
		http.Error(w, "Idempotency-Key is required", http.StatusBadRequest)
		return "", false
	}
	return id, true
}
func writeError(w http.ResponseWriter, err error) {
	var productErr *octetdb.Error
	if errors.As(err, &productErr) && productErr.Kind == octetdb.ErrorCapacity {
		http.Error(w, "capacity exceeded", http.StatusInsufficientStorage)
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	http.Error(w, "storage unavailable", http.StatusServiceUnavailable)
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
