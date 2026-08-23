package service

import "context"

type Event struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Result string `json:"result"`
}
type Decision struct {
	Applied   bool  `json:"applied"`
	Duplicate bool  `json:"duplicate"`
	Event     Event `json:"event"`
}
type Store interface {
	Process(context.Context, string, Event) (Decision, error)
	Get(context.Context, string) (Event, bool, error)
}
type Service struct{ store Store }

func New(store Store) *Service { return &Service{store: store} }
func (s *Service) Process(ctx context.Context, event Event) (Decision, error) {
	return s.store.Process(ctx, event.ID, event)
}
func (s *Service) Get(ctx context.Context, id string) (Event, bool, error) {
	return s.store.Get(ctx, id)
}
