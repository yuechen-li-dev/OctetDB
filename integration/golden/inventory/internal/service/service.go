package service

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("item not found")

type Item struct {
	ID    string `json:"id"`
	Stock int    `json:"stock"`
}
type Reservation struct {
	ItemID    string `json:"item_id"`
	Quantity  int    `json:"quantity"`
	Remaining int    `json:"remaining"`
}
type Command struct {
	ID       string
	ItemID   string
	Quantity int
}
type Decision[T any] struct {
	Applied   bool
	Code      string
	Duplicate bool
	Value     T
}

type Store interface {
	Create(context.Context, string, Item) (Decision[Item], error)
	Get(context.Context, string) (Item, error)
	Reserve(context.Context, Command) (Decision[Reservation], error)
	Release(context.Context, Command) (Decision[Item], error)
}

type Service struct{ store Store }

func New(store Store) *Service { return &Service{store: store} }
func (s *Service) Create(ctx context.Context, commandID string, item Item) (Decision[Item], error) {
	return s.store.Create(ctx, commandID, item)
}
func (s *Service) Get(ctx context.Context, id string) (Item, error) { return s.store.Get(ctx, id) }
func (s *Service) Reserve(ctx context.Context, command Command) (Decision[Reservation], error) {
	return s.store.Reserve(ctx, command)
}
func (s *Service) Release(ctx context.Context, command Command) (Decision[Item], error) {
	return s.store.Release(ctx, command)
}
