package store

import (
	"context"
	"example.com/octetdb-golden/order/internal/service"
	"github.com/yuechen-li-dev/octetdb"
	"net/url"
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
func orderKey(id string) string { return "orders/" + url.PathEscape(id) }
func (s *DB) Create(ctx context.Context, commandID, id string) (service.Decision, error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.KeyedTx) (any, error) {
		var existing service.Order
		if ok, err := tx.Get(orderKey(id), &existing); err != nil {
			return nil, err
		} else if ok {
			return existing, octetdb.RejectWithResult("order_exists", existing)
		}
		order := service.Order{ID: id, Status: service.Created}
		return order, tx.Put(orderKey(id), order)
	})
	return decode(decision, err)
}
func (s *DB) Transition(ctx context.Context, commandID, id string, to service.Status) (service.Decision, error) {
	decision, err := s.db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.KeyedTx) (any, error) {
		var order service.Order
		ok, err := tx.Get(orderKey(id), &order)
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
		return order, tx.Put(orderKey(id), order)
	})
	return decode(decision, err)
}
func (s *DB) Get(ctx context.Context, id string) (service.Order, error) {
	var order service.Order
	ok, err := s.db.GetKeyed(ctx, orderKey(id), &order)
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
