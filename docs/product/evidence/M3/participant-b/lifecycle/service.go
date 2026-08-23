// Package lifecycle implements a durable order state machine on OctetDB.
package lifecycle

import (
	"context"
	"fmt"
	"sync"

	"github.com/yuechen-li-dev/octetdb"
)

const commandPrefix = "order/v1/"

// State is the durable lifecycle state of an Order.
type State string

const (
	Created   State = "created"
	Paid      State = "paid"
	Shipped   State = "shipped"
	Cancelled State = "cancelled"
)

// Order is stored under its order ID in the orders dataset.
type Order struct {
	ID    string `json:"id"`
	State State  `json:"state"`
}

// PaymentEmail is a durable outbox message with a stable provider-facing ID.
// Providers should deduplicate message IDs, because a crash after Send and
// before marking the message sent can require an at-least-once retry.
type PaymentEmail struct {
	MessageID string `json:"message_id"`
	OrderID   string `json:"order_id"`
	Sent      bool   `json:"sent"`
}

// EmailSender is an external adapter. Send is never called in a Mutate
// callback: the payment and outbox entry commit before this adapter is used.
type EmailSender interface {
	Send(context.Context, PaymentEmail) error
}

// BeforePersist is a deterministic, local validation hook. An ordinary error
// from it aborts the mutation, so its command ID remains retryable.
type BeforePersist func(action string, order Order) error

// Options configures optional external delivery and local validation.
type Options struct {
	EmailSender   EmailSender
	BeforePersist BeforePersist
}

// Outcome reports the durable command decision and decoded domain result.
// EmailError never rolls back a committed payment; retrying Pay re-attempts the
// durable outbox delivery using its stable MessageID.
type Outcome struct {
	Order      Order
	Decision   octetdb.KeyedDecision
	EmailError error
}

// Service owns the catalog datasets for an order lifecycle application.
type Service struct {
	db            *octetdb.Database
	orders        *octetdb.Dataset
	emails        *octetdb.Dataset
	emailSender   EmailSender
	beforePersist BeforePersist
	deliveryMu    sync.Mutex
}

// Open creates or opens the state-machine catalog.
func Open(ctx context.Context, path string, options Options) (*Service, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "orders")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	orders, err := bucket.Dataset(ctx, "records", octetdb.DatasetOptions{TypeIdentity: "participant-b.lifecycle.Order/v1"})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	emails, err := bucket.Dataset(ctx, "payment-emails", octetdb.DatasetOptions{TypeIdentity: "participant-b.lifecycle.PaymentEmail/v1"})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Service{db: db, orders: orders, emails: emails, emailSender: options.EmailSender, beforePersist: options.BeforePersist}, nil
}

// CommandID makes a database-wide identity from a record key, action, and an
// externally stable request ID. The record key alone is never reused as a
// command identity, since it participates in several commands over its life.
func CommandID(orderID, action, requestID string) string {
	return commandPrefix + orderID + "/" + action + "/" + requestID
}

// Create creates an order in the Created state.
func (s *Service) Create(ctx context.Context, orderID, requestID string) (Outcome, error) {
	return s.transition(ctx, orderID, "create", requestID, func(order Order, found bool) (Order, string, error) {
		if found {
			return order, "order_exists", octetdb.RejectWithResult("order_exists", order)
		}
		return Order{ID: orderID, State: Created}, "", nil
	})
}

// Pay moves a Created order to Paid and commits its email outbox entry. Email
// delivery happens only after the payment command has a durable decision.
func (s *Service) Pay(ctx context.Context, orderID, requestID string) (Outcome, error) {
	outcome, err := s.transition(ctx, orderID, "pay", requestID, func(order Order, found bool) (Order, string, error) {
		if !found {
			return Order{ID: orderID}, "order_not_found", octetdb.RejectWithResult("order_not_found", Order{ID: orderID})
		}
		if order.State != Created {
			return order, rejectionFor("pay", order.State), octetdb.RejectWithResult(rejectionFor("pay", order.State), order)
		}
		order.State = Paid
		return order, "", nil
	})
	if err != nil {
		return outcome, err
	}
	// transition stored the outbox only on an accepted Pay. A duplicate gets the
	// same durable result and can resume delivery after a prior process stopped.
	if outcome.Order.State == Paid && (outcome.Decision.Applied || outcome.Decision.Duplicate && outcome.Decision.Code == "") {
		outcome.EmailError = s.deliverPaymentEmail(ctx, outcome.Order.ID)
	}
	return outcome, nil
}

// Ship moves a Paid order to Shipped.
func (s *Service) Ship(ctx context.Context, orderID, requestID string) (Outcome, error) {
	return s.transition(ctx, orderID, "ship", requestID, func(order Order, found bool) (Order, string, error) {
		if !found {
			return Order{ID: orderID}, "order_not_found", octetdb.RejectWithResult("order_not_found", Order{ID: orderID})
		}
		if order.State != Paid {
			return order, rejectionFor("ship", order.State), octetdb.RejectWithResult(rejectionFor("ship", order.State), order)
		}
		order.State = Shipped
		return order, "", nil
	})
}

// Cancel moves a Created order to Cancelled.
func (s *Service) Cancel(ctx context.Context, orderID, requestID string) (Outcome, error) {
	return s.transition(ctx, orderID, "cancel", requestID, func(order Order, found bool) (Order, string, error) {
		if !found {
			return Order{ID: orderID}, "order_not_found", octetdb.RejectWithResult("order_not_found", Order{ID: orderID})
		}
		if order.State != Created {
			return order, rejectionFor("cancel", order.State), octetdb.RejectWithResult(rejectionFor("cancel", order.State), order)
		}
		order.State = Cancelled
		return order, "", nil
	})
}

// Order returns the durable state by record key.
func (s *Service) Order(ctx context.Context, orderID string) (Order, bool, error) {
	var order Order
	found, err := s.orders.Get(ctx, orderID, &order)
	return order, found, err
}

// Close installs the OctetDB close-time snapshot.
func (s *Service) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Service) transition(ctx context.Context, orderID, action, requestID string, apply func(Order, bool) (Order, string, error)) (Outcome, error) {
	if s == nil || s.db == nil {
		return Outcome{}, fmt.Errorf("lifecycle service is closed")
	}
	commandID := CommandID(orderID, action, requestID)
	decision, err := s.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var current Order
		found, err := tx.Get(s.orders, orderID, &current)
		if err != nil {
			return nil, err
		}
		next, _, err := apply(current, found)
		if err != nil {
			return next, err
		}
		if s.beforePersist != nil {
			if err := s.beforePersist(action, next); err != nil {
				return nil, err
			}
		}
		if err := tx.Put(s.orders, orderID, next); err != nil {
			return nil, err
		}
		if action == "pay" {
			email := PaymentEmail{MessageID: "payment-receipt/v1/" + orderID, OrderID: orderID}
			if err := tx.Put(s.emails, email.MessageID, email); err != nil {
				return nil, err
			}
		}
		return next, nil
	})
	if err != nil {
		return Outcome{}, err
	}
	var order Order
	if err := octetdb.DecodeResult(decision, &order); err != nil {
		return Outcome{}, err
	}
	return Outcome{Order: order, Decision: decision}, nil
}

func (s *Service) deliverPaymentEmail(ctx context.Context, orderID string) error {
	if s.emailSender == nil {
		return nil
	}
	s.deliveryMu.Lock()
	defer s.deliveryMu.Unlock()
	messageID := "payment-receipt/v1/" + orderID
	var email PaymentEmail
	found, err := s.emails.Get(ctx, messageID, &email)
	if err != nil || !found || email.Sent {
		return err
	}
	if err := s.emailSender.Send(ctx, email); err != nil {
		return err
	}
	_, err = s.db.Mutate(ctx, octetdb.KeyedCommand{ID: "email/v1/mark-sent/" + messageID}, func(tx *octetdb.Tx) (any, error) {
		var pending PaymentEmail
		found, err := tx.Get(s.emails, messageID, &pending)
		if err != nil || !found {
			return nil, err
		}
		if pending.Sent {
			return pending, nil
		}
		pending.Sent = true
		return pending, tx.Put(s.emails, messageID, pending)
	})
	return err
}

func rejectionFor(action string, state State) string {
	if action == "pay" && state == Paid {
		return "already_paid"
	}
	if action == "ship" && state == Created {
		return "not_paid"
	}
	return "invalid_" + action + "_from_" + string(state)
}
