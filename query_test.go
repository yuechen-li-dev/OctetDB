package octetdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

type queryTestRecord struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Stock  int    `json:"stock"`
}

func openQueryTestDataset(t testing.TB, path, bucketName, datasetName string, options KeyedOptions) (*CatalogDB, *Dataset) {
	t.Helper()
	db, err := OpenCatalog(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := db.Bucket(context.Background(), bucketName)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	dataset, err := bucket.Dataset(context.Background(), datasetName, DatasetOptions{TypeIdentity: "query.test/v1"})
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, dataset
}

func putQueryRecords(t testing.TB, dataset *Dataset, commandID string, records map[string]any) {
	t.Helper()
	decision, err := dataset.Mutate(context.Background(), KeyedCommand{ID: commandID}, func(tx *DatasetTx) (any, error) {
		for key, value := range records {
			if err := tx.Put(key, value); err != nil {
				return nil, err
			}
		}
		return len(records), nil
	})
	if err != nil || !decision.Applied {
		t.Fatalf("put decision=%+v err=%v", decision, err)
	}
}

func TestDatasetScanDeterministicFilterTakeAndReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	db, jobs := openQueryTestDataset(t, path, "workers", "jobs", DefaultKeyedOptions())
	defer db.Close()
	putQueryRecords(t, jobs, "seed", map[string]any{
		"job-30": queryTestRecord{ID: "job-30", Status: "ready"},
		"job-10": queryTestRecord{ID: "job-10", Status: "claimed"},
		"job-20": queryTestRecord{ID: "job-20", Status: "ready"},
		"job-40": queryTestRecord{ID: "job-40", Status: "ready"},
	})

	walBefore, err := os.Stat(filepath.Join(path, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	sequenceBefore := db.keyed.sequence
	dedupeBefore := append([]string(nil), db.keyed.dedupeIDs...)
	examined := 0
	var ready []string
	err = ScanDataset(context.Background(), jobs, func(key string, value queryTestRecord) (ScanAction, error) {
		examined++
		if value.Status != "ready" {
			return ScanContinue, nil
		}
		ready = append(ready, key)
		if len(ready) == 2 {
			return ScanStop, nil
		}
		return ScanContinue, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(ready, []string{"job-20", "job-30"}) {
		t.Fatalf("ready=%v", ready)
	}
	if examined != 3 {
		t.Fatalf("examined=%d want=3; Take did not short-circuit", examined)
	}
	walAfter, err := os.Stat(filepath.Join(path, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	if walAfter.Size() != walBefore.Size() || db.keyed.sequence != sequenceBefore || !reflect.DeepEqual(db.keyed.dedupeIDs, dedupeBefore) {
		t.Fatalf("query mutated durable state: wal %d->%d sequence %d->%d dedupe %v->%v", walBefore.Size(), walAfter.Size(), sequenceBefore, db.keyed.sequence, dedupeBefore, db.keyed.dedupeIDs)
	}
}

func TestDatasetScanRawDetachedOrderRestartAndSeparateDataset(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, jobs := openQueryTestDataset(t, path, "workers", "jobs", DefaultKeyedOptions())
	workers, err := db.Bucket(ctx, "workers")
	if err != nil {
		t.Fatal(err)
	}
	archived, err := workers.Dataset(ctx, "archived", DatasetOptions{TypeIdentity: "query.test/v1"})
	if err != nil {
		t.Fatal(err)
	}
	putQueryRecords(t, jobs, "jobs", map[string]any{"same": queryTestRecord{ID: "live", Status: "ready"}, "a": queryTestRecord{ID: "a"}})
	putQueryRecords(t, archived, "archived", map[string]any{"same": queryTestRecord{ID: "old", Status: "completed"}})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, jobs = openQueryTestDataset(t, path, "workers", "jobs", DefaultKeyedOptions())
	defer db.Close()
	var keys []string
	var first DatasetRecord
	if err := jobs.Scan(ctx, func(record DatasetRecord) (ScanAction, error) {
		keys = append(keys, record.Key)
		if first.JSON == nil {
			first = record
		}
		return ScanContinue, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "same"}) {
		t.Fatalf("keys=%v", keys)
	}
	first.JSON[0] = 'x'
	var live queryTestRecord
	if ok, err := jobs.Get(ctx, "a", &live); err != nil || !ok || live.ID != "a" {
		t.Fatalf("detached JSON changed storage: value=%+v ok=%v err=%v", live, ok, err)
	}
	workers, err = db.Bucket(ctx, "workers")
	if err != nil {
		t.Fatal(err)
	}
	archived, err = workers.Dataset(ctx, "archived", DatasetOptions{TypeIdentity: "query.test/v1"})
	if err != nil {
		t.Fatal(err)
	}
	var old queryTestRecord
	if ok, err := archived.Get(ctx, "same", &old); err != nil || !ok || old.ID != "old" {
		t.Fatalf("separate dataset value=%+v ok=%v err=%v", old, ok, err)
	}
}

func TestDatasetScanCancellationClosedDecodeAndCallbackFailure(t *testing.T) {
	db, dataset := openQueryTestDataset(t, filepath.Join(t.TempDir(), "db"), "inventory", "items", DefaultKeyedOptions())
	putQueryRecords(t, dataset, "seed", map[string]any{"bad": 42, "good": queryTestRecord{ID: "good", Stock: 3}})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if err := ScanDataset(ctx, dataset, func(string, queryTestRecord) (ScanAction, error) {
		called = true
		return ScanContinue, nil
	}); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("cancel err=%v called=%v", err, called)
	}

	var octErr *Error
	err := ScanDataset(context.Background(), dataset, func(string, queryTestRecord) (ScanAction, error) { return ScanContinue, nil })
	if !errors.As(err, &octErr) || octErr.Kind != ErrorCorruption {
		t.Fatalf("decode err=%v", err)
	}
	sentinel := errors.New("predicate failed")
	err = dataset.Scan(context.Background(), func(record DatasetRecord) (ScanAction, error) { return ScanStop, sentinel })
	if !errors.Is(err, sentinel) {
		t.Fatalf("callback err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	err = dataset.Scan(context.Background(), func(DatasetRecord) (ScanAction, error) { return ScanContinue, nil })
	if !errors.As(err, &octErr) || octErr.Kind != ErrorClosed {
		t.Fatalf("closed err=%v", err)
	}
}

func TestDatasetScanHundredThousandCancellationAndWriteBlocking(t *testing.T) {
	if testing.Short() {
		t.Skip("bounded 100k long-query contract")
	}
	options := DefaultKeyedOptions()
	options.MaxRecords = 100_010
	options.MaxTransactionBytes = 32 << 20
	db, dataset := openQueryTestDataset(t, filepath.Join(t.TempDir(), "db"), "scale", "records", options)
	defer db.Close()
	records := make(map[string]any, 100_000)
	for index := 0; index < 100_000; index++ {
		key := fmt.Sprintf("%06d", index)
		records[key] = queryTestRecord{ID: key, Stock: index}
	}
	putQueryRecords(t, dataset, "seed-100k", records)

	ctx, cancel := context.WithCancel(context.Background())
	examined := 0
	err := ScanDataset(ctx, dataset, func(_ string, _ queryTestRecord) (ScanAction, error) {
		examined++
		if examined == 100 {
			cancel()
		}
		return ScanContinue, nil
	})
	if !errors.Is(err, context.Canceled) || examined != 100 {
		t.Fatalf("cancel err=%v examined=%d", err, examined)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	scanDone := make(chan error, 1)
	go func() {
		scanDone <- dataset.Scan(context.Background(), func(DatasetRecord) (ScanAction, error) {
			close(entered)
			<-release
			return ScanStop, nil
		})
	}()
	<-entered
	mutationDone := make(chan error, 1)
	go func() {
		_, err := dataset.Mutate(context.Background(), KeyedCommand{ID: "blocked-write"}, func(tx *DatasetTx) (any, error) {
			return nil, tx.Put("later", queryTestRecord{ID: "later"})
		})
		mutationDone <- err
	}()
	select {
	case err := <-mutationDone:
		t.Fatalf("mutation did not block behind stable query snapshot: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-scanDone; err != nil {
		t.Fatal(err)
	}
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
}

func TestDatasetScanConcurrentReadersRemainDeterministic(t *testing.T) {
	db, dataset := openQueryTestDataset(t, filepath.Join(t.TempDir(), "db"), "workers", "jobs", DefaultKeyedOptions())
	defer db.Close()
	putQueryRecords(t, dataset, "seed", map[string]any{"b": queryTestRecord{ID: "b"}, "a": queryTestRecord{ID: "a"}})
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var keys []string
			err := dataset.Scan(context.Background(), func(record DatasetRecord) (ScanAction, error) {
				keys = append(keys, record.Key)
				return ScanContinue, nil
			})
			if err == nil && !reflect.DeepEqual(keys, []string{"a", "b"}) {
				err = fmt.Errorf("keys=%v", keys)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDatasetScanPrimaryKeyCursorTracksInsertUpdateAndDelete(t *testing.T) {
	db, dataset := openQueryTestDataset(t, filepath.Join(t.TempDir(), "db"), "cursor", "records", DefaultKeyedOptions())
	defer db.Close()
	putQueryRecords(t, dataset, "seed", map[string]any{
		"b": queryTestRecord{ID: "b"},
		"d": queryTestRecord{ID: "d"},
	})
	decision, err := dataset.Mutate(context.Background(), KeyedCommand{ID: "edit"}, func(tx *DatasetTx) (any, error) {
		if err := tx.Put("a", queryTestRecord{ID: "a"}); err != nil {
			return nil, err
		}
		if err := tx.Put("b", queryTestRecord{ID: "b-updated"}); err != nil {
			return nil, err
		}
		return nil, tx.Delete("d")
	})
	if err != nil || !decision.Applied {
		t.Fatalf("edit decision=%+v err=%v", decision, err)
	}
	var keys []string
	if err := dataset.Scan(context.Background(), func(record DatasetRecord) (ScanAction, error) {
		keys = append(keys, record.Key)
		return ScanContinue, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(keys, []string{"a", "b"}) {
		t.Fatalf("cursor keys=%v", keys)
	}
}
