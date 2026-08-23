package store

import (
	"context"
	"fmt"

	"example.com/octetdb-golden/inventory/internal/service"
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
func (s *DB) Close() error     { return s.db.Close() }
func itemKey(id string) string { return "items/" + id }

func (s *DB) Create(ctx context.Context, commandID string, item service.Item) (service.Decision[service.Item], error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.KeyedTx) (any, error) {
		var existing service.Item
		if ok, err := tx.Get(itemKey(item.ID), &existing); err != nil {
			return nil, err
		} else if ok {
			return existing, octetdb.RejectWithResult("item_exists", existing)
		}
		if item.ID == "" || item.Stock < 0 {
			return nil, octetdb.Reject("invalid_item")
		}
		return item, tx.Put(itemKey(item.ID), item)
	})
	return decode[service.Item](decision, err)
}
func (s *DB) Get(ctx context.Context, id string) (service.Item, error) {
	var item service.Item
	ok, err := s.db.GetKeyed(ctx, itemKey(id), &item)
	if err != nil {
		return item, err
	}
	if !ok {
		return item, service.ErrNotFound
	}
	return item, nil
}
func (s *DB) Reserve(ctx context.Context, command service.Command) (service.Decision[service.Reservation], error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: command.ID}, func(tx *octetdb.KeyedTx) (any, error) {
		var item service.Item
		ok, err := tx.Get(itemKey(command.ItemID), &item)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, octetdb.Reject("not_found")
		}
		if command.Quantity <= 0 {
			return nil, octetdb.Reject("invalid_quantity")
		}
		if item.Stock < command.Quantity {
			return nil, octetdb.Reject("insufficient_stock")
		}
		item.Stock -= command.Quantity
		if err := tx.Put(itemKey(item.ID), item); err != nil {
			return nil, err
		}
		return service.Reservation{ItemID: item.ID, Quantity: command.Quantity, Remaining: item.Stock}, nil
	})
	return decode[service.Reservation](decision, err)
}
func (s *DB) Release(ctx context.Context, command service.Command) (service.Decision[service.Item], error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: command.ID}, func(tx *octetdb.KeyedTx) (any, error) {
		var item service.Item
		ok, err := tx.Get(itemKey(command.ItemID), &item)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, octetdb.Reject("not_found")
		}
		if command.Quantity <= 0 {
			return nil, octetdb.Reject("invalid_quantity")
		}
		item.Stock += command.Quantity
		return item, tx.Put(itemKey(item.ID), item)
	})
	return decode[service.Item](decision, err)
}
func decode[T any](decision octetdb.KeyedDecision, err error) (service.Decision[T], error) {
	var value T
	if err != nil {
		return service.Decision[T]{}, err
	}
	if len(decision.Result) > 0 {
		if err := octetdb.DecodeResult(decision, &value); err != nil {
			return service.Decision[T]{}, fmt.Errorf("decode decision: %w", err)
		}
	}
	return service.Decision[T]{Applied: decision.Applied, Code: decision.Code, Duplicate: decision.Duplicate, Value: value}, nil
}
