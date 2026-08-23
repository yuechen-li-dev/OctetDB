package store

import (
	"context"
	"fmt"

	"example.com/octetdb-golden/inventory/internal/service"
	"github.com/yuechen-li-dev/octetdb"
)

type DB struct {
	db    *octetdb.Database
	items *octetdb.Dataset
}

func Open(ctx context.Context, path string) (*DB, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "inventory")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	items, err := bucket.Dataset(ctx, "items", octetdb.DatasetOptions{TypeIdentity: "inventory.Item/v1"})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &DB{db: db, items: items}, nil
}
func (s *DB) Close() error { return s.db.Close() }

func (s *DB) Create(ctx context.Context, commandID string, item service.Item) (service.Decision[service.Item], error) {
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var existing service.Item
		if ok, err := tx.Get(s.items, item.ID, &existing); err != nil {
			return nil, err
		} else if ok {
			return existing, octetdb.RejectWithResult("item_exists", existing)
		}
		if item.ID == "" || item.Stock < 0 {
			return nil, octetdb.Reject("invalid_item")
		}
		return item, tx.Put(s.items, item.ID, item)
	})
	return decode[service.Item](decision, err)
}
func (s *DB) Get(ctx context.Context, id string) (service.Item, error) {
	var item service.Item
	ok, err := s.items.Get(ctx, id, &item)
	if err != nil {
		return item, err
	}
	if !ok {
		return item, service.ErrNotFound
	}
	return item, nil
}

// ListLowStock returns at most limit items whose stock is at or below threshold,
// ordered deterministically by item ID.
func (s *DB) ListLowStock(ctx context.Context, threshold, limit int) ([]service.Item, error) {
	if limit <= 0 {
		return []service.Item{}, nil
	}
	items := make([]service.Item, 0, limit)
	err := octetdb.ScanDataset(ctx, s.items, func(_ string, item service.Item) (octetdb.ScanAction, error) {
		if item.Stock > threshold {
			return octetdb.ScanContinue, nil
		}
		items = append(items, item)
		if len(items) == limit {
			return octetdb.ScanStop, nil
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}
func (s *DB) Reserve(ctx context.Context, command service.Command) (service.Decision[service.Reservation], error) {
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: command.ID}, func(tx *octetdb.Tx) (any, error) {
		var item service.Item
		ok, err := tx.Get(s.items, command.ItemID, &item)
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
		if err := tx.Put(s.items, item.ID, item); err != nil {
			return nil, err
		}
		return service.Reservation{ItemID: item.ID, Quantity: command.Quantity, Remaining: item.Stock}, nil
	})
	return decode[service.Reservation](decision, err)
}
func (s *DB) Release(ctx context.Context, command service.Command) (service.Decision[service.Item], error) {
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: command.ID}, func(tx *octetdb.Tx) (any, error) {
		var item service.Item
		ok, err := tx.Get(s.items, command.ItemID, &item)
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
		return item, tx.Put(s.items, item.ID, item)
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
