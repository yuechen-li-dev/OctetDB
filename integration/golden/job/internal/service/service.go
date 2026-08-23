package service

import (
	"context"
	"errors"
)

var ErrNotFound = errors.New("job not found")

type Status string

const (
	Ready     Status = "ready"
	Claimed   Status = "claimed"
	Completed Status = "completed"
	Failed    Status = "failed"
)

type Job struct {
	ID       string `json:"id"`
	Status   Status `json:"status"`
	Owner    string `json:"owner,omitempty"`
	Attempts int    `json:"attempts"`
	Failure  string `json:"failure,omitempty"`
}
type Decision struct {
	Applied   bool   `json:"applied"`
	Code      string `json:"code,omitempty"`
	Duplicate bool   `json:"duplicate"`
	Job       Job    `json:"job"`
}
type Store interface {
	Create(context.Context, string, string) (Decision, error)
	Claim(context.Context, string, string, string) (Decision, error)
	Complete(context.Context, string, string, string) (Decision, error)
	Fail(context.Context, string, string, string, string) (Decision, error)
	Get(context.Context, string) (Job, error)
}
type Service struct{ store Store }

func New(store Store) *Service { return &Service{store: store} }
func (s *Service) Create(ctx context.Context, commandID, id string) (Decision, error) {
	return s.store.Create(ctx, commandID, id)
}
func (s *Service) Claim(ctx context.Context, commandID, id, owner string) (Decision, error) {
	return s.store.Claim(ctx, commandID, id, owner)
}
func (s *Service) Complete(ctx context.Context, commandID, id, owner string) (Decision, error) {
	return s.store.Complete(ctx, commandID, id, owner)
}
func (s *Service) Fail(ctx context.Context, commandID, id, owner, reason string) (Decision, error) {
	return s.store.Fail(ctx, commandID, id, owner, reason)
}
func (s *Service) Get(ctx context.Context, id string) (Job, error) { return s.store.Get(ctx, id) }
