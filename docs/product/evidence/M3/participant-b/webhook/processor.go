// Package webhook demonstrates a durable idempotent webhook consumer.
package webhook

import (
	"context"
	"fmt"

	"github.com/yuechen-li-dev/octetdb"
)

const eventCommandPrefix = "webhook/v1/event/"

// Delivery is the durable processing record for one provider event.
// EventID is the record key; it is deliberately distinct from CommandID.
type Delivery struct {
	EventID string `json:"event_id"`
	Status  string `json:"status"`
	Result  string `json:"result"`
}

// Outcome is returned both for a first delivery and for an exact retry.
type Outcome struct {
	Delivery  Delivery
	CommandID string
	Duplicate bool
}

// Mutation performs the local business mutation in the same transaction as
// recording the delivery. It must be deterministic and must not call external
// systems.
type Mutation func(*octetdb.Tx) (string, error)

// Processor owns the durable catalog objects used by one webhook consumer.
type Processor struct {
	db     *octetdb.Database
	events *octetdb.Dataset
}

// Open creates or opens a processor at path.
func Open(ctx context.Context, path string) (*Processor, error) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "webhook")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	events, err := bucket.Dataset(ctx, "events", octetdb.DatasetOptions{
		TypeIdentity: "participant-b.webhook.Delivery/v1",
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Processor{db: db, events: events}, nil
}

// CommandID derives a stable, database-wide command ID from a provider event.
// The event ID itself is the dataset-scoped Delivery record key, while this
// value is the idempotency identity passed to Database.Mutate.
func CommandID(eventID string) string { return eventCommandPrefix + eventID }

// Process applies mutation once and durably stores its result under eventID.
// A retry with the same external event ID replays the recorded Outcome without
// invoking mutation, including after a close/reopen.
func (p *Processor) Process(ctx context.Context, eventID string, mutation Mutation) (Outcome, error) {
	if p == nil || p.db == nil {
		return Outcome{}, fmt.Errorf("webhook processor is closed")
	}
	if eventID == "" {
		return Outcome{}, fmt.Errorf("event ID is required")
	}
	if mutation == nil {
		return Outcome{}, fmt.Errorf("mutation is required")
	}
	commandID := CommandID(eventID)
	decision, err := p.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		result, err := mutation(tx)
		if err != nil {
			return nil, err
		}
		delivery := Delivery{EventID: eventID, Status: "completed", Result: result}
		return delivery, tx.Put(p.events, eventID, delivery)
	})
	if err != nil {
		return Outcome{}, err
	}
	var delivery Delivery
	if err := octetdb.DecodeResult(decision, &delivery); err != nil {
		return Outcome{}, err
	}
	return Outcome{Delivery: delivery, CommandID: commandID, Duplicate: decision.Duplicate}, nil
}

// Delivery returns a durable delivery record by its dataset-scoped event ID.
func (p *Processor) Delivery(ctx context.Context, eventID string) (Delivery, bool, error) {
	var delivery Delivery
	found, err := p.events.Get(ctx, eventID, &delivery)
	return delivery, found, err
}

// Close makes the processor's current state restart-safe.
func (p *Processor) Close() error {
	if p == nil || p.db == nil {
		return nil
	}
	return p.db.Close()
}
