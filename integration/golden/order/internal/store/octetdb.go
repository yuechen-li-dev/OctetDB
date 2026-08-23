package store

import (
	"context"
	"example.com/octetdb-golden/order/internal/service"
	"github.com/yuechen-li-dev/octetdb"
)

type DB struct {
	db     *octetdb.CatalogDB
	orders *octetdb.Dataset
}

func Open(ctx context.Context, path string) (*DB, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "commerce")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	orders, err := bucket.Dataset(ctx, "orders", octetdb.DatasetOptions{TypeIdentity: "order.Order/v1"})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &DB{db: db, orders: orders}, nil
}
func (s *DB) Close() error { return s.db.Close() }
func (s *DB) Create(ctx context.Context, commandID, id string) (service.Decision, error) {
	decision, err := s.orders.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.DatasetTx) (any, error) {
		var existing service.Order
		if ok, err := tx.Get(id, &existing); err != nil {
			return nil, err
		} else if ok {
			return existing, octetdb.RejectWithResult("order_exists", existing)
		}
		order := service.Order{ID: id, Status: service.Created}
		return order, tx.Put(id, order)
	})
	return decode(decision, err)
}
func (s *DB) Transition(ctx context.Context, commandID, id string, to service.Status) (service.Decision, error) {
	decision, err := s.orders.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.DatasetTx) (any, error) {
		var order service.Order
		ok, err := tx.Get(id, &order)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, octetdb.Reject("not_found")
		}
		if !valid(order.Status, to) {
			return order, octetdb.RejectWithResult("invalid_transition", order)
		}
		order.Status = to
		return order, tx.Put(id, order)
	})
	return decode(decision, err)
}
func (s *DB) Get(ctx context.Context, id string) (service.Order, error) {
	var order service.Order
	ok, err := s.orders.Get(ctx, id, &order)
	if err != nil {
		return order, err
	}
	if !ok {
		return order, service.ErrNotFound
	}
	return order, nil
}
func valid(from, to service.Status) bool {
	switch from {
	case service.Created:
		return to == service.Paid || to == service.Cancelled
	case service.Paid:
		return to == service.Shipped || to == service.Cancelled
	default:
		return false
	}
}
func decode(decision octetdb.KeyedDecision, err error) (service.Decision, error) {
	if err != nil {
		return service.Decision{}, err
	}
	var order service.Order
	if len(decision.Result) > 0 {
		if err := octetdb.DecodeResult(decision, &order); err != nil {
			return service.Decision{}, err
		}
	}
	return service.Decision{Applied: decision.Applied, Code: decision.Code, Duplicate: decision.Duplicate, Order: order}, nil
}
