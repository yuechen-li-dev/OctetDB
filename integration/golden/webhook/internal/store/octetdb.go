package store

import (
	"context"
	"net/url"

	"example.com/octetdb-golden/webhook/internal/service"
	"github.com/yuechen-li-dev/octetdb"
)

type DB struct{ db *octetdb.KeyedDB }

func Open(ctx context.Context, path string) (*DB, error) {
	db, err := octetdb.OpenKeyed(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	return &DB{db: db}, nil
}
func (s *DB) Close() error      { return s.db.Close() }
func eventKey(id string) string { return "events/" + url.PathEscape(id) }
func (s *DB) Process(ctx context.Context, commandID string, event service.Event) (service.Decision, error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.KeyedTx) (any, error) {
		var existing service.Event
		if ok, err := tx.Get(eventKey(event.ID), &existing); err != nil {
			return nil, err
		} else if ok {
			return existing, nil
		}
		if event.ID == "" {
			return nil, octetdb.Reject("invalid_event")
		}
		event.Status = "processed"
		if err := tx.Put(eventKey(event.ID), event); err != nil {
			return nil, err
		}
		return event, nil
	})
	if err != nil {
		return service.Decision{}, err
	}
	var result service.Event
	if err := octetdb.DecodeResult(decision, &result); err != nil {
		return service.Decision{}, err
	}
	return service.Decision{Applied: decision.Applied, Duplicate: decision.Duplicate, Event: result}, nil
}
func (s *DB) Get(ctx context.Context, id string) (service.Event, bool, error) {
	var event service.Event
	ok, err := s.db.GetKeyed(ctx, eventKey(id), &event)
	return event, ok, err
}
