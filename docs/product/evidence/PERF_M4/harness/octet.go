package main

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"sync/atomic"

	"github.com/yuechen-li-dev/octetdb"
)

type record struct {
	ID        int    `json:"id"`
	Balance   int64  `json:"balance,omitempty"`
	Available int64  `json:"available,omitempty"`
	Reserved  int64  `json:"reserved,omitempty"`
	Status    string `json:"status,omitempty"`
	Attempts  int    `json:"attempts,omitempty"`
	Result    string `json:"result,omitempty"`
}

type octetBackend struct {
	cfg      config
	db       *octetdb.Database
	dataset  *octetdb.Dataset
	examined atomic.Uint64
}

func newOctetBackend(cfg config) (*octetBackend, error) { return &octetBackend{cfg: cfg}, nil }

func (b *octetBackend) Setup(ctx context.Context) error {
	db, err := octetdb.OpenCatalog(ctx, b.cfg.DataPath, octetdb.DefaultKeyedOptions())
	if err != nil {
		return err
	}
	b.db = db
	bucket, err := db.Bucket(ctx, "perfm4")
	if err != nil {
		return err
	}
	dataset, err := bucket.Dataset(ctx, b.cfg.Workload, octetdb.DatasetOptions{TypeIdentity: "perfm4.Record/v1"})
	if err != nil {
		return err
	}
	b.dataset = dataset
	if b.cfg.SkipSeed {
		return nil
	}
	if b.cfg.Workload == "w4" {
		return nil
	}
	for i := 0; i < b.cfg.Population; i++ {
		value := seedRecord(b.cfg.Workload, i)
		if b.cfg.Workload == "w5" {
			value.Status = queryStatus(b.cfg.Selectivity, i)
		}
		_, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: fmt.Sprintf("seed-%d", i)}, func(tx *octetdb.Tx) (any, error) {
			return value, tx.Put(b.dataset, key(i), value)
		})
		if err != nil {
			return fmt.Errorf("seed %d: %w", i, err)
		}
	}
	return nil
}

func seedRecord(workload string, id int) record {
	switch workload {
	case "w1":
		return record{ID: id, Balance: 100_000}
	case "w2":
		return record{ID: id, Available: 10_000, Reserved: 100}
	case "w3", "w5":
		statuses := []string{"ready", "claimed", "completed", "failed"}
		return record{ID: id, Status: statuses[id%len(statuses)], Attempts: id % 3}
	case "w6":
		return record{ID: id, Available: 10_000, Reserved: 100, Status: "active"}
	default:
		return record{ID: id}
	}
}

func (b *octetBackend) Operation(ctx context.Context, operation int) error {
	switch b.cfg.Workload {
	case "w1":
		return b.transfer(ctx, operation)
	case "w2":
		return b.inventory(ctx, operation)
	case "w3":
		return b.jobs(ctx, operation)
	case "w4":
		return b.webhook(ctx, operation)
	case "w5":
		return b.query(ctx, operation)
	case "w6":
		return b.mixed(ctx, operation)
	default:
		return fmt.Errorf("unknown workload %s", b.cfg.Workload)
	}
}

func (b *octetBackend) transfer(ctx context.Context, operation int) error {
	source, destination := choosePair(b.cfg, operation)
	_, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID(operation)}, func(tx *octetdb.Tx) (any, error) {
		var from, to record
		ok, err := tx.Get(b.dataset, key(source), &from)
		if err != nil || !ok {
			return nil, fmt.Errorf("source: %w", err)
		}
		ok, err = tx.Get(b.dataset, key(destination), &to)
		if err != nil || !ok {
			return nil, fmt.Errorf("destination: %w", err)
		}
		if from.Balance < 1 {
			return from, octetdb.RejectWithResult("insufficient", from)
		}
		from.Balance--
		to.Balance++
		if err := tx.Put(b.dataset, key(source), from); err != nil {
			return nil, err
		}
		if err := tx.Put(b.dataset, key(destination), to); err != nil {
			return nil, err
		}
		return from, nil
	})
	return err
}

func (b *octetBackend) inventory(ctx context.Context, operation int) error {
	id := positive(operation) % b.cfg.Population
	_, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID(operation)}, func(tx *octetdb.Tx) (any, error) {
		var item record
		ok, err := tx.Get(b.dataset, key(id), &item)
		if err != nil || !ok {
			return nil, fmt.Errorf("item: %w", err)
		}
		switch positive(operation) % 3 {
		case 0:
			if item.Available < 1 {
				return item, octetdb.RejectWithResult("insufficient", item)
			}
			item.Available--
			item.Reserved++
		case 1:
			if item.Reserved < 1 {
				return item, octetdb.RejectWithResult("none_reserved", item)
			}
			item.Reserved--
			item.Available++
		case 2:
			item.Available++
		}
		return item, tx.Put(b.dataset, key(id), item)
	})
	return err
}

func (b *octetBackend) jobs(ctx context.Context, operation int) error {
	if positive(operation)%5 == 4 {
		return b.takeReady(ctx, 10)
	}
	id := positive(operation) % b.cfg.Population
	_, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID(operation)}, func(tx *octetdb.Tx) (any, error) {
		var job record
		ok, err := tx.Get(b.dataset, key(id), &job)
		if err != nil || !ok {
			return nil, fmt.Errorf("job: %w", err)
		}
		switch positive(operation) % 4 {
		case 0:
			if job.Status != "ready" && job.Status != "failed" {
				return job, octetdb.RejectWithResult("not_ready", job)
			}
			job.Status = "claimed"
			job.Attempts++
		case 1:
			if job.Status != "claimed" {
				return job, octetdb.RejectWithResult("not_claimed", job)
			}
			job.Status = "completed"
		case 2:
			if job.Status != "claimed" {
				return job, octetdb.RejectWithResult("not_claimed", job)
			}
			job.Status = "failed"
		case 3:
			job.Status = "ready"
		}
		return job, tx.Put(b.dataset, key(id), job)
	})
	return err
}

func (b *octetBackend) webhook(ctx context.Context, operation int) error {
	id := positive(operation) % b.cfg.Population
	decision, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: fmt.Sprintf("event-%d", id)}, func(tx *octetdb.Tx) (any, error) {
		var event record
		ok, err := tx.Get(b.dataset, key(id), &event)
		if err != nil {
			return nil, err
		}
		if ok {
			return event, octetdb.RejectWithResult("already_processed", event)
		}
		event = record{ID: id, Status: "processed", Result: fmt.Sprintf("result-%d", id)}
		return event, tx.Put(b.dataset, key(id), event)
	})
	if err != nil {
		return err
	}
	var durable record
	if err := octetdb.DecodeResult(decision, &durable); err != nil {
		return err
	}
	if durable.ID != id || durable.Result != fmt.Sprintf("result-%d", id) {
		return fmt.Errorf("webhook durable result mismatch")
	}
	return nil
}

func (b *octetBackend) query(ctx context.Context, operation int) error {
	switch queryVariant(b.cfg.QueryOp, operation) {
	case 0:
		var item record
		_, err := b.dataset.Get(ctx, key(positive(operation)%b.cfg.Population), &item)
		return err
	case 1:
		return b.scan(ctx, false, false)
	case 2:
		return b.scan(ctx, true, false)
	case 3:
		return b.scan(ctx, false, true)
	default:
		return b.dataset.Scan(ctx, func(_ octetdb.DatasetRecord) (octetdb.ScanAction, error) {
			b.examined.Add(1)
			return octetdb.ScanContinue, nil
		})
	}
}

func (b *octetBackend) scan(ctx context.Context, take, project bool) error {
	matched := 0
	return octetdb.ScanDataset(ctx, b.dataset, func(_ string, item record) (octetdb.ScanAction, error) {
		b.examined.Add(1)
		if item.Status != "ready" {
			return octetdb.ScanContinue, nil
		}
		matched++
		if project {
			_ = item.ID * 2
		}
		if take && matched == 10 {
			return octetdb.ScanStop, nil
		}
		return octetdb.ScanContinue, nil
	})
}

func (b *octetBackend) takeReady(ctx context.Context, limit int) error {
	matched := 0
	return octetdb.ScanDataset(ctx, b.dataset, func(_ string, item record) (octetdb.ScanAction, error) {
		b.examined.Add(1)
		if item.Status == "ready" {
			matched++
		}
		if matched == limit {
			return octetdb.ScanStop, nil
		}
		return octetdb.ScanContinue, nil
	})
}

func (b *octetBackend) mixed(ctx context.Context, operation int) error {
	selector := positive(operation) % 100
	writeLimit := 20
	if b.cfg.Mix == "50r40w10q" {
		writeLimit = 40
	}
	readLimit := 90 - writeLimit
	if selector < readLimit {
		var item record
		_, err := b.dataset.Get(ctx, key(positive(operation)%b.cfg.Population), &item)
		return err
	}
	if selector < 90 {
		return b.inventory(ctx, operation)
	}
	matched := 0
	return octetdb.ScanDataset(ctx, b.dataset, func(_ string, item record) (octetdb.ScanAction, error) {
		b.examined.Add(1)
		if item.Available < 10_002 {
			matched++
		}
		if matched == 10 {
			return octetdb.ScanStop, nil
		}
		return octetdb.ScanContinue, nil
	})
}

func (b *octetBackend) Verify(ctx context.Context) (map[string]bool, error) {
	checks := map[string]bool{"records_valid": true, "domain_invariant": true, "idempotency": true}
	count := 0
	var balanceTotal int64
	err := octetdb.ScanDataset(ctx, b.dataset, func(_ string, item record) (octetdb.ScanAction, error) {
		count++
		if item.Balance < 0 || item.Available < 0 || item.Reserved < 0 {
			checks["domain_invariant"] = false
		}
		balanceTotal += item.Balance
		if b.cfg.Workload == "w4" && (item.Status != "processed" || item.Result == "") {
			checks["records_valid"] = false
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		return nil, err
	}
	if b.cfg.Workload != "w4" && count != b.cfg.Population {
		checks["records_valid"] = false
	}
	if b.cfg.Workload == "w1" && balanceTotal != int64(b.cfg.Population)*100_000 {
		checks["domain_invariant"] = false
	}
	first, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: "verify-idempotency"}, func(*octetdb.Tx) (any, error) { return "ok", nil })
	if err != nil {
		return nil, err
	}
	second, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: "verify-idempotency"}, func(*octetdb.Tx) (any, error) { return nil, fmt.Errorf("duplicate callback ran") })
	if err != nil || !second.Duplicate || (first.Duplicate && string(first.Result) != string(second.Result)) {
		checks["idempotency"] = false
	}
	return checks, nil
}

func (b *octetBackend) StorageBytes() (int64, error) {
	var total int64
	err := filepath.WalkDir(b.cfg.DataPath, func(_ string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}
func (b *octetBackend) WALBytes() (int64, error) {
	var total int64
	err := filepath.WalkDir(b.cfg.DataPath, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) == ".wal" || entry.Name() == "wal" {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			total += info.Size()
		}
		return nil
	})
	return total, err
}
func (b *octetBackend) RecordsExamined() uint64 { return b.examined.Load() }
func (b *octetBackend) ResetMetrics()           { b.examined.Store(0) }
func (b *octetBackend) Metadata() map[string]string {
	return map[string]string{"specialization_level": "S0"}
}
func (b *octetBackend) Close() error {
	if b.db != nil {
		return b.db.Close()
	}
	return nil
}

func key(id int) string { return fmt.Sprintf("%012d", id) }
func commandID(operation int) string {
	if operation < 0 {
		return fmt.Sprintf("warmup-%d", -operation)
	}
	return fmt.Sprintf("measure-%d", operation)
}
func positive(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func queryVariant(name string, operation int) int {
	if name == "mixed" {
		return positive(operation) % 5
	}
	return map[string]int{"point": 0, "filter": 1, "take": 2, "map": 3, "count": 4}[name]
}

func queryStatus(selectivity string, id int) string {
	switch selectivity {
	case "early":
		if id < 10 {
			return "ready"
		}
	case "1":
		if id%100 == 0 {
			return "ready"
		}
	case "10":
		if id%10 == 0 {
			return "ready"
		}
	case "25":
		if id%4 == 0 {
			return "ready"
		}
	case "50":
		if id%2 == 0 {
			return "ready"
		}
	case "100":
		return "ready"
	}
	return "other"
}
