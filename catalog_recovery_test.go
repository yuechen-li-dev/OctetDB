package octetdb

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCatalogCanonicalCorruptionMatrix(t *testing.T) {
	ctx := context.Background()

	t.Run("snapshot corruption", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db")
		db, dataset := openQueryTestDataset(t, path, "inventory", "items", DefaultKeyedOptions())
		putQueryRecords(t, dataset, "seed", map[string]any{"widget": catalogTestRecord{Value: 1}})
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		corruptLastByte(t, filepath.Join(path, "snapshot"))
		_, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
		assertErrorKind(t, err, ErrorCorruption)
	})

	t.Run("WAL corruption", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db")
		db, dataset := openQueryTestDataset(t, path, "inventory", "items", DefaultKeyedOptions())
		putQueryRecords(t, dataset, "seed", map[string]any{"widget": catalogTestRecord{Value: 1}})
		crashCatalogForTest(t, db)
		corruptLastByte(t, filepath.Join(path, "wal"))
		_, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
		assertErrorKind(t, err, ErrorCorruption)
	})

	t.Run("incomplete final WAL append", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "db")
		db, dataset := openQueryTestDataset(t, path, "inventory", "items", DefaultKeyedOptions())
		putQueryRecords(t, dataset, "seed", map[string]any{"widget": catalogTestRecord{Value: 7}})
		crashCatalogForTest(t, db)
		wal, err := os.OpenFile(filepath.Join(path, "wal"), os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wal.Write([]byte{10, 0}); err != nil {
			_ = wal.Close()
			t.Fatal(err)
		}
		if err := wal.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = OpenCatalog(ctx, path, DefaultKeyedOptions())
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		bucket, _ := db.Bucket(ctx, "inventory")
		dataset, _ = bucket.Dataset(ctx, "items", DatasetOptions{TypeIdentity: "query.test/v1"})
		var got catalogTestRecord
		if ok, err := dataset.Get(ctx, "widget", &got); err != nil || !ok || got.Value != 7 {
			t.Fatalf("record=%+v ok=%v err=%v", got, ok, err)
		}
	})
}

func TestCatalogCloseSnapshotPreservesTopologyAtomicStateDedupeAndQuery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := OpenCatalog(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	commerce, _ := db.Bucket(ctx, "commerce")
	inventory, _ := db.Bucket(ctx, "inventory")
	orders, _ := commerce.Dataset(ctx, "orders", DatasetOptions{TypeIdentity: "example.Order/v1"})
	items, _ := inventory.Dataset(ctx, "items", DatasetOptions{TypeIdentity: "example.Item/v1"})
	decision, err := db.Mutate(ctx, KeyedCommand{ID: "place-order-1"}, func(tx *Tx) (any, error) {
		if err := tx.Put(orders, "order-1", catalogTestRecord{Value: 2}); err != nil {
			return nil, err
		}
		if err := tx.Put(items, "widget", catalogTestRecord{Value: 3}); err != nil {
			return nil, err
		}
		return "placed", nil
	})
	if err != nil || !decision.Applied {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenCatalog(ctx, path, DefaultKeyedOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	topology, err := db.Catalog(ctx)
	if err != nil || len(topology.Buckets) != 2 || len(topology.Datasets) != 2 {
		t.Fatalf("topology=%+v err=%v", topology, err)
	}
	commerce, _ = db.Bucket(ctx, "commerce")
	inventory, _ = db.Bucket(ctx, "inventory")
	orders, _ = commerce.Dataset(ctx, "orders", DatasetOptions{TypeIdentity: "example.Order/v1"})
	items, _ = inventory.Dataset(ctx, "items", DatasetOptions{TypeIdentity: "example.Item/v1"})
	called := false
	retry, err := db.Mutate(ctx, KeyedCommand{ID: "place-order-1"}, func(*Tx) (any, error) {
		called = true
		return nil, nil
	})
	if err != nil || !retry.Duplicate || called {
		t.Fatalf("retry=%+v called=%v err=%v", retry, called, err)
	}
	var order, item catalogTestRecord
	if ok, err := orders.Get(ctx, "order-1", &order); err != nil || !ok || order.Value != 2 {
		t.Fatalf("order=%+v ok=%v err=%v", order, ok, err)
	}
	if ok, err := items.Get(ctx, "widget", &item); err != nil || !ok || item.Value != 3 {
		t.Fatalf("item=%+v ok=%v err=%v", item, ok, err)
	}
	var keys []string
	if err := ScanDataset(ctx, items, func(key string, _ catalogTestRecord) (ScanAction, error) {
		keys = append(keys, key)
		return ScanContinue, nil
	}); err != nil || !reflect.DeepEqual(keys, []string{"widget"}) {
		t.Fatalf("query keys=%v err=%v", keys, err)
	}
}

func corruptLastByte(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatalf("cannot corrupt empty file %s", path)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}
