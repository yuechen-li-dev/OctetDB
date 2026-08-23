package octetdb

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

type catalogTestRecord struct {
	Value int `json:"value"`
}

func TestCatalogTreeIdentityEnumerationAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	databaseID := db.DatabaseID()
	if databaseID == "" {
		t.Fatal("database identity is empty")
	}
	workers, err := db.Bucket(ctx, "workers")
	if err != nil {
		t.Fatal(err)
	}
	commerce, err := db.Bucket(ctx, "commerce")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := workers.Dataset(ctx, "jobs", DatasetOptions{TypeIdentity: "example.Job/v1"})
	if err != nil {
		t.Fatal(err)
	}
	orders, err := commerce.Dataset(ctx, "orders", DatasetOptions{TypeIdentity: "example.Order/v1"})
	if err != nil {
		t.Fatal(err)
	}
	jobsAgain, err := workers.Dataset(ctx, "jobs", DatasetOptions{TypeIdentity: "example.Job/v1"})
	if err != nil {
		t.Fatal(err)
	}
	if jobsAgain.Info().ID != jobs.Info().ID {
		t.Fatalf("duplicate open changed identity: %d != %d", jobsAgain.Info().ID, jobs.Info().ID)
	}
	buckets, err := db.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(buckets, []BucketInfo{{Name: "commerce"}, {Name: "workers"}}) {
		t.Fatalf("buckets=%+v", buckets)
	}
	datasets, err := workers.ListDatasets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(datasets) != 1 || datasets[0].Name != "jobs" || datasets[0].Kind != KeyedJSON {
		t.Fatalf("datasets=%+v", datasets)
	}

	decision, err := db.Mutate(ctx, KeyedCommand{ID: "same-key"}, func(tx *CatalogTx) (any, error) {
		if err := tx.Put(jobs, "123", catalogTestRecord{Value: 1}); err != nil {
			return nil, err
		}
		if err := tx.Put(orders, "123", catalogTestRecord{Value: 2}); err != nil {
			return nil, err
		}
		return "stored", nil
	})
	if err != nil || !decision.Applied {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	crashCatalogForTest(t, db)

	db, err = OpenCatalog(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if db.DatabaseID() != databaseID {
		t.Fatalf("database identity changed: %q != %q", db.DatabaseID(), databaseID)
	}
	workers, err = db.Bucket(ctx, "workers")
	if err != nil {
		t.Fatal(err)
	}
	commerce, err = db.Bucket(ctx, "commerce")
	if err != nil {
		t.Fatal(err)
	}
	jobs, err = workers.Dataset(ctx, "jobs", DatasetOptions{TypeIdentity: "example.Job/v1"})
	if err != nil {
		t.Fatal(err)
	}
	orders, err = commerce.Dataset(ctx, "orders", DatasetOptions{TypeIdentity: "example.Order/v1"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		dataset *Dataset
		want    int
	}{{jobs, 1}, {orders, 2}} {
		var got catalogTestRecord
		ok, err := test.dataset.Get(ctx, "123", &got)
		if err != nil || !ok || got.Value != test.want {
			t.Fatalf("record=%+v ok=%v err=%v want=%d", got, ok, err, test.want)
		}
	}
}

func TestCatalogCrossDatasetAtomicityAndGlobalDuplicateIdentity(t *testing.T) {
	ctx := context.Background()
	db, err := OpenCatalog(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	inventoryBucket, _ := db.Bucket(ctx, "inventory")
	ordersBucket, _ := db.Bucket(ctx, "commerce")
	items, _ := inventoryBucket.Dataset(ctx, "items", DefaultDatasetOptions())
	orders, _ := ordersBucket.Dataset(ctx, "orders", DefaultDatasetOptions())
	_, err = items.Mutate(ctx, KeyedCommand{ID: "seed"}, func(tx *DatasetTx) (any, error) {
		return nil, tx.Put("widget", catalogTestRecord{Value: 10})
	})
	if err != nil {
		t.Fatal(err)
	}

	decision, err := db.Mutate(ctx, KeyedCommand{ID: "reserve-1"}, func(tx *CatalogTx) (any, error) {
		var item catalogTestRecord
		ok, err := tx.Get(items, "widget", &item)
		if err != nil || !ok {
			return nil, err
		}
		item.Value -= 3
		if err := tx.Put(items, "widget", item); err != nil {
			return nil, err
		}
		if err := tx.Put(orders, "order-1", catalogTestRecord{Value: 3}); err != nil {
			return nil, err
		}
		return item, nil
	})
	if err != nil || !decision.Applied {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	called := false
	duplicate, err := orders.Mutate(ctx, KeyedCommand{ID: "reserve-1"}, func(tx *DatasetTx) (any, error) {
		called = true
		return nil, tx.Put("should-not-exist", 1)
	})
	if err != nil || !duplicate.Duplicate || called {
		t.Fatalf("duplicate=%+v called=%v err=%v", duplicate, called, err)
	}

	rejected, err := db.Mutate(ctx, KeyedCommand{ID: "reserve-rejected"}, func(tx *CatalogTx) (any, error) {
		if err := tx.Put(items, "widget", catalogTestRecord{Value: 0}); err != nil {
			return nil, err
		}
		if err := tx.Put(orders, "order-rejected", catalogTestRecord{Value: 7}); err != nil {
			return nil, err
		}
		return nil, Reject("business_rule")
	})
	if err != nil || rejected.Applied || rejected.Code != "business_rule" {
		t.Fatalf("rejected=%+v err=%v", rejected, err)
	}
	var item catalogTestRecord
	if ok, err := items.Get(ctx, "widget", &item); err != nil || !ok || item.Value != 7 {
		t.Fatalf("item=%+v ok=%v err=%v", item, ok, err)
	}
	var absent catalogTestRecord
	if ok, err := orders.Get(ctx, "order-rejected", &absent); err != nil || ok {
		t.Fatalf("rejected write visible: ok=%v err=%v", ok, err)
	}
}

func TestCatalogDatasetCompatibilityAndCapacity(t *testing.T) {
	ctx := context.Background()
	db, err := OpenCatalog(ctx, filepath.Join(t.TempDir(), "db"), KeyedOptions{MaxRecords: 10, MaxValueBytes: 64, MaxTransactionBytes: 128})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, _ := db.Bucket(ctx, "inventory")
	dataset, err := bucket.Dataset(ctx, "items", DatasetOptions{TypeIdentity: "Item/v1", MaxRecords: 1, MaxValueBytes: 32})
	if err != nil {
		t.Fatal(err)
	}
	_, err = bucket.Dataset(ctx, "items", DatasetOptions{TypeIdentity: "Item/v2", MaxRecords: 1, MaxValueBytes: 32})
	assertErrorKind(t, err, ErrorIncompatible)
	_, err = bucket.Dataset(ctx, "items", DatasetOptions{Kind: DatasetKind("octagon_record"), TypeIdentity: "Item/v1", MaxRecords: 1, MaxValueBytes: 32})
	assertErrorKind(t, err, ErrorIncompatible)
	_, err = dataset.Mutate(ctx, KeyedCommand{ID: "one"}, func(tx *DatasetTx) (any, error) { return nil, tx.Put("one", "value") })
	if err != nil {
		t.Fatal(err)
	}
	_, err = dataset.Mutate(ctx, KeyedCommand{ID: "two"}, func(tx *DatasetTx) (any, error) { return nil, tx.Put("two", "value") })
	assertErrorKind(t, err, ErrorCapacity)
	_, err = dataset.Mutate(ctx, KeyedCommand{ID: "large"}, func(tx *DatasetTx) (any, error) { return nil, tx.Put("large", string(make([]byte, 40))) })
	assertErrorKind(t, err, ErrorCapacity)
}

func TestCatalogMaximumRecordKeySurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	bucket, _ := db.Bucket(ctx, "inventory")
	dataset, _ := bucket.Dataset(ctx, "items", DefaultDatasetOptions())
	key := string(make([]byte, keyedMaxKeyBytes))
	if _, err := dataset.Mutate(ctx, KeyedCommand{ID: "maximum-key"}, func(tx *DatasetTx) (any, error) {
		return nil, tx.Put(key, catalogTestRecord{Value: 1})
	}); err != nil {
		t.Fatal(err)
	}
	crashCatalogForTest(t, db)
	db, err = OpenCatalog(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, _ = db.Bucket(ctx, "inventory")
	dataset, _ = bucket.Dataset(ctx, "items", DefaultDatasetOptions())
	var record catalogTestRecord
	if ok, err := dataset.Get(ctx, key, &record); err != nil || !ok || record.Value != 1 {
		t.Fatalf("record=%+v ok=%v err=%v", record, ok, err)
	}
}

func TestCatalogTransactionCannotEscapeCallback(t *testing.T) {
	ctx := context.Background()
	db, err := OpenCatalog(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	bucket, _ := db.Bucket(ctx, "inventory")
	dataset, _ := bucket.Dataset(ctx, "items", DefaultDatasetOptions())
	var escaped *DatasetTx
	if _, err := dataset.Mutate(ctx, KeyedCommand{ID: "capture"}, func(tx *DatasetTx) (any, error) {
		escaped = tx
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	err = escaped.Put("late", 1)
	assertErrorKind(t, err, ErrorInvalidInput)
}

func TestCatalogCorruptionAndUncatalogedDataFailClosed(t *testing.T) {
	ctx := context.Background()
	t.Run("catalog checksum", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db")
		db, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Bucket(ctx, "inventory"); err != nil {
			t.Fatal(err)
		}
		crashCatalogForTest(t, db)
		catalogPath := filepath.Join(path, "catalog")
		data, err := os.ReadFile(catalogPath)
		if err != nil {
			t.Fatal(err)
		}
		data[len(data)-2] ^= 0xff
		if err := os.WriteFile(catalogPath, data, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = OpenCatalog(ctx, path, DefaultKeyedOptions())
		assertErrorKind(t, err, ErrorCorruption)
	})

	t.Run("legacy keyed data", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db")
		legacy, err := OpenKeyed(ctx, path, DefaultKeyedOptions())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := legacy.SubmitKeyed(ctx, KeyedCommand{ID: "legacy"}, func(tx *KeyedTx) (any, error) { return nil, tx.Put("jobs/123", 1) }); err != nil {
			t.Fatal(err)
		}
		if err := legacy.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = OpenCatalog(ctx, path, DefaultKeyedOptions())
		assertErrorKind(t, err, ErrorIncompatible)
	})

	t.Run("catalog marker rejects global API", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db")
		db, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = OpenKeyed(ctx, path, DefaultKeyedOptions())
		assertErrorKind(t, err, ErrorIncompatible)
	})
}

func TestCatalogRejectsPathLikeNesting(t *testing.T) {
	ctx := context.Background()
	db, err := OpenCatalog(ctx, filepath.Join(t.TempDir(), "db"), DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Bucket(ctx, "inventory/archive")
	assertErrorKind(t, err, ErrorInvalidInput)
	bucket, err := db.Bucket(ctx, "inventory")
	if err != nil {
		t.Fatal(err)
	}
	_, err = bucket.Dataset(ctx, "items/archive", DefaultDatasetOptions())
	assertErrorKind(t, err, ErrorInvalidInput)
}

func crashCatalogForTest(t *testing.T, db *CatalogDB) {
	t.Helper()
	if err := db.keyed.wal.close(); err != nil {
		t.Fatal(err)
	}
	db.keyed.closed.Store(true)
}

func assertErrorKind(t *testing.T, err error, kind ErrorKind) {
	t.Helper()
	var productErr *Error
	if !errors.As(err, &productErr) || productErr.Kind != kind {
		t.Fatalf("err=%v, want kind %s", err, kind)
	}
}
