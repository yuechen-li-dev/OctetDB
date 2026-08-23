package octetdb

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type testItem struct {
	Stock int `json:"stock"`
}

type testOrder struct {
	Status string `json:"status"`
}

func TestKeyedAtomicMutationDedupeAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenKeyed(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}

	created, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "create-item"}, func(tx *KeyedTx) (any, error) {
		if err := tx.Put("items/widget", testItem{Stock: 10}); err != nil {
			return nil, err
		}
		return testItem{Stock: 10}, nil
	})
	if err != nil || !created.Applied {
		t.Fatalf("created=%+v err=%v", created, err)
	}

	reserve := func(tx *KeyedTx) (any, error) {
		var item testItem
		ok, err := tx.Get("items/widget", &item)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, Reject("not_found")
		}
		if item.Stock < 4 {
			return nil, RejectWithResult("insufficient_stock", item)
		}
		item.Stock -= 4
		if err := tx.Put("items/widget", item); err != nil {
			return nil, err
		}
		return item, nil
	}
	first, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "reserve-1"}, reserve)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "reserve-1"}, func(*KeyedTx) (any, error) {
		t.Fatal("duplicate executed mutation callback")
		return nil, nil
	})
	if err != nil || !retry.Duplicate || retry.Sequence != first.Sequence {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}

	rejected, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "reserve-2"}, func(tx *KeyedTx) (any, error) {
		var item testItem
		_, _ = tx.Get("items/widget", &item)
		if err := tx.Put("items/widget", testItem{Stock: -100}); err != nil {
			return nil, err
		}
		return nil, Reject("insufficient_stock")
	})
	if err != nil || rejected.Applied || rejected.Code != "insufficient_stock" {
		t.Fatalf("rejected=%+v err=%v", rejected, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenKeyed(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var item testItem
	if ok, err := db.GetKeyed(ctx, "items/widget", &item); err != nil || !ok || item.Stock != 6 {
		t.Fatalf("item=%+v ok=%v err=%v", item, ok, err)
	}
	afterRestart, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "reserve-1"}, reserve)
	if err != nil || !afterRestart.Duplicate || afterRestart.Sequence != first.Sequence {
		t.Fatalf("afterRestart=%+v err=%v", afterRestart, err)
	}
	rejectedRetry, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "reserve-2"}, reserve)
	if err != nil || !rejectedRetry.Duplicate || rejectedRetry.Applied || rejectedRetry.Code != "insufficient_stock" {
		t.Fatalf("rejectedRetry=%+v err=%v", rejectedRetry, err)
	}
}

func TestKeyedValidatedStateTransitionIsAtomic(t *testing.T) {
	ctx := context.Background()
	db, err := OpenKeyed(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "create"}, func(tx *KeyedTx) (any, error) {
		return nil, tx.Put("orders/1", testOrder{Status: "created"})
	})
	if err != nil {
		t.Fatal(err)
	}
	pay := func(id string) KeyedDecision {
		decision, submitErr := db.SubmitKeyed(ctx, KeyedCommand{ID: id}, func(tx *KeyedTx) (any, error) {
			var order testOrder
			if _, getErr := tx.Get("orders/1", &order); getErr != nil {
				return nil, getErr
			}
			if order.Status != "created" {
				return nil, Reject("invalid_transition")
			}
			order.Status = "paid"
			return order, tx.Put("orders/1", order)
		})
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		return decision
	}
	decisions := make(chan KeyedDecision, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"pay-1", "pay-2"} {
		wg.Add(1)
		go func() { defer wg.Done(); decisions <- pay(id) }()
	}
	wg.Wait()
	close(decisions)
	applied, rejected := 0, 0
	for decision := range decisions {
		if decision.Applied {
			applied++
		}
		if !decision.Applied && decision.Code == "invalid_transition" {
			rejected++
		}
	}
	if applied != 1 || rejected != 1 {
		t.Fatalf("applied=%d rejected=%d", applied, rejected)
	}
}

func TestKeyedAtomicMultiRecord(t *testing.T) {
	ctx := context.Background()
	db, err := OpenKeyed(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "multi"}, func(tx *KeyedTx) (any, error) {
		if err := tx.Put("jobs/1", map[string]string{"status": "ready"}); err != nil {
			return nil, err
		}
		if err := tx.Put("jobs/2", map[string]string{"status": "ready"}); err != nil {
			return nil, err
		}
		return map[string]int{"created": 2}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"jobs/1", "jobs/2"} {
		var job map[string]string
		if ok, err := db.GetKeyed(ctx, key, &job); err != nil || !ok || job["status"] != "ready" {
			t.Fatalf("key=%s job=%+v ok=%v err=%v", key, job, ok, err)
		}
	}
}

func TestKeyedBoundsAndTypedErrors(t *testing.T) {
	ctx := context.Background()
	db, err := OpenKeyed(ctx, filepath.Join(t.TempDir(), "db"), KeyedOptions{MaxRecords: 1, DedupeHorizon: 2, MaxValueBytes: 16, MaxTransactionBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "one"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("one", "value") })
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "two"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("two", "value") })
	var productErr *Error
	if !errors.As(err, &productErr) || productErr.Kind != ErrorCapacity {
		t.Fatalf("err=%v", err)
	}
	_, err = db.GetKeyed(ctx, string(make([]byte, keyedMaxKeyBytes+1)), &testItem{})
	if !errors.As(err, &productErr) || productErr.Kind != ErrorCapacity {
		t.Fatalf("long-key err=%v", err)
	}
}

func TestKeyedDurabilityFailurePoisonsFurtherWrites(t *testing.T) {
	ctx := context.Background()
	db, err := OpenKeyed(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "first"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("one", 1) })
	var productErr *Error
	if !errors.As(err, &productErr) || productErr.Kind != ErrorStorage {
		t.Fatalf("first err=%v", err)
	}
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "second"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("two", 2) })
	if !errors.As(err, &productErr) || productErr.Kind != ErrorPoisoned {
		t.Fatalf("second err=%v", err)
	}
	if err := db.Close(); err == nil {
		t.Fatal("close unexpectedly hid the closed WAL")
	}
}

func TestKeyedTransactionCannotEscapePanickingCallback(t *testing.T) {
	ctx := context.Background()
	db, err := OpenKeyed(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var escaped *KeyedTx
	func() {
		defer func() { _ = recover() }()
		_, _ = db.SubmitKeyed(ctx, KeyedCommand{ID: "panic"}, func(tx *KeyedTx) (any, error) { escaped = tx; panic("boom") })
	}()
	err = escaped.Put("late", 1)
	var productErr *Error
	if !errors.As(err, &productErr) || productErr.Kind != ErrorInvalidInput {
		t.Fatalf("escaped err=%v", err)
	}
	if _, err := db.SubmitKeyed(ctx, KeyedCommand{ID: "after"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("after", 1) }); err != nil {
		t.Fatal(err)
	}
}

func TestKeyedRecoveryTruncatesIncompleteTailAndRejectsCorruption(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenKeyed(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "one"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("one", 1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := db.wal.close(); err != nil {
		t.Fatal(err)
	}
	db.closed.Store(true)
	walPath := filepath.Join(path, "wal")
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{10, 0}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()
	db, err = OpenKeyed(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.wal.close(); err != nil {
		t.Fatal(err)
	}
	db.closed.Store(true)

	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) < 9 {
		t.Fatal("WAL unexpectedly short")
	}
	length := int(binary.LittleEndian.Uint32(data[:4]))
	data[4+length] ^= 0xff
	if err := os.WriteFile(walPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = OpenKeyed(ctx, path, DefaultKeyedOptions())
	var productErr *Error
	if !errors.As(err, &productErr) || productErr.Kind != ErrorCorruption {
		t.Fatalf("err=%v", err)
	}
}

func TestKeyedOpenReportsSmallerConfiguredBoundAsCapacity(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenKeyed(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.SubmitKeyed(ctx, KeyedCommand{ID: "large"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("value", "a value larger than eight bytes") })
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = OpenKeyed(ctx, path, KeyedOptions{MaxValueBytes: 8, MaxTransactionBytes: 8})
	var productErr *Error
	if !errors.As(err, &productErr) || productErr.Kind != ErrorCapacity {
		t.Fatalf("err=%v", err)
	}
}
