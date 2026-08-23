package service

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("order not found")

type Status string

const (
	Created   Status = "created"
	Paid      Status = "paid"
	Shipped   Status = "shipped"
	Cancelled Status = "cancelled"
)

type Order struct {
	ID     string `json:"id"`
	Status Status `json:"status"`
}
type Decision struct {
	Applied   bool   `json:"applied"`
	Code      string `json:"code,omitempty"`
	Duplicate bool   `json:"duplicate"`
	Order     Order  `json:"order"`
}
type Store interface {
	Create(context.Context, string, string) (Decision, error)
	Transition(context.Context, string, string, Status) (Decision, error)
	Get(context.Context, string) (Order, error)
}
type Service struct{ store Store }

func New(store Store) *Service { return &Service{store: store} }
func (s *Service) Create(ctx context.Context, commandID, id string) (Decision, error) {
	return s.store.Create(ctx, commandID, id)
}
func (s *Service) Transition(ctx context.Context, commandID, id string, to Status) (Decision, error) {
	return s.store.Transition(ctx, commandID, id, to)
}
func (s *Service) Get(ctx context.Context, id string) (Order, error) { return s.store.Get(ctx, id) }
