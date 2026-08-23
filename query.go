package octetdb

import (
	"context"
	"encoding/json"
	"errors"
)

// ScanAction tells Dataset.Scan whether to continue or stop successfully.
// Stopping is synchronous: no later record is examined or decoded.
type ScanAction uint8

const (
	// ScanContinue advances to the next record.
	ScanContinue ScanAction = iota
	// ScanStop completes the scan successfully after the current record.
	ScanStop
)

// DatasetRecord is one detached logical KeyedJSON record. JSON never aliases
// OctetDB's internal record storage.
type DatasetRecord struct {
	Key  string
	JSON json.RawMessage
}

// Decode decodes this logical record into destination.
func (record DatasetRecord) Decode(destination any) error {
	if destination == nil {
		return &Error{Kind: ErrorInvalidInput, Op: "dataset_record_decode", err: errors.New("destination is required")}
	}
	if err := json.Unmarshal(record.JSON, destination); err != nil {
		return &Error{Kind: ErrorCorruption, Op: "dataset_record_decode", err: err}
	}
	return nil
}

// Scan visits detached records in record-key ascending order. It is read-only:
// it does not append to the WAL, advance the sequence, or change dedupe state.
// The scan holds the database admission boundary and observes one stable
// committed state. Mutations cannot interleave and must not be called from
// visit. Context cancellation is checked between records.
//
// visit should be deterministic, local, and side-effect-free. It runs
// synchronously and may return ScanStop for First, Any, or Take behavior.
func (dataset *Dataset) Scan(ctx context.Context, visit func(DatasetRecord) (ScanAction, error)) error {
	if visit == nil {
		return &Error{Kind: ErrorInvalidInput, Op: "dataset_scan", err: errors.New("visit callback is required")}
	}
	return dataset.scan(ctx, func(key string, encoded []byte) (ScanAction, error) {
		record := DatasetRecord{Key: key, JSON: append(json.RawMessage(nil), encoded...)}
		return visit(record)
	})
}

// ScanDataset is the typed KeyedJSON scan helper. It decodes each detached
// record into a new T and creates no intermediate result slice. Ordering,
// snapshot, cancellation, read-only, and synchronous-stop semantics are the
// same as Dataset.Scan. A decode failure fails the whole scan; already-observed
// callback values cannot be revoked.
func ScanDataset[T any](ctx context.Context, dataset *Dataset, visit func(key string, value T) (ScanAction, error)) error {
	if visit == nil {
		return &Error{Kind: ErrorInvalidInput, Op: "scan_dataset", err: errors.New("visit callback is required")}
	}
	return dataset.scan(ctx, func(key string, encoded []byte) (ScanAction, error) {
		var value T
		if err := json.Unmarshal(encoded, &value); err != nil {
			return ScanStop, &Error{Kind: ErrorCorruption, Op: "scan_dataset", err: err}
		}
		return visit(key, value)
	})
}

func (dataset *Dataset) scan(ctx context.Context, visit func(key string, encoded []byte) (ScanAction, error)) error {
	if dataset == nil || dataset.db == nil || dataset.db.keyed == nil {
		return &Error{Kind: ErrorClosed, Op: "dataset_scan", err: errors.New("database is closed")}
	}
	if dataset.info.Kind != KeyedJSON {
		return &Error{Kind: ErrorIncompatible, Op: "dataset_scan", err: errors.New("dataset kind does not support scans")}
	}
	if err := dataset.db.keyed.enterKeyed(ctx, "dataset_scan"); err != nil {
		return err
	}
	defer dataset.db.keyed.leaveKeyed()

	// The ordered primary-key cursor is maintained inside the mutation
	// admission boundary, so ScanStop stops upstream work immediately instead
	// of first enumerating the whole backend map.
	for _, key := range dataset.db.queryKeys[dataset.info.ID] {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		action, err := visit(key, dataset.db.keyed.records[backendRecordKey(dataset.info.ID, key)])
		if err != nil {
			return err
		}
		switch action {
		case ScanContinue:
		case ScanStop:
			return nil
		default:
			return &Error{Kind: ErrorInvalidInput, Op: "dataset_scan", err: errors.New("visit returned an invalid ScanAction")}
		}
	}
	return nil
}
