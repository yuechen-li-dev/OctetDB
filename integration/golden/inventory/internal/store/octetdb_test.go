package store

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"

	"example.com/octetdb-golden/inventory/internal/service"
)

func TestInventoryLifecycleRestartAndRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := db.Create(ctx, "create", service.Item{ID: "widget", Stock: 10}); err != nil || !d.Applied {
		t.Fatalf("d=%+v err=%v", d, err)
	}
	first, err := db.Reserve(ctx, service.Command{ID: "reserve", ItemID: "widget", Quantity: 4})
	if err != nil || !first.Applied {
		t.Fatalf("d=%+v err=%v", first, err)
	}
	retry, err := db.Reserve(ctx, service.Command{ID: "reserve", ItemID: "widget", Quantity: 4})
	if err != nil || !retry.Duplicate || retry.Value.Remaining != 6 {
		t.Fatalf("d=%+v err=%v", retry, err)
	}
	if rejected, err := db.Reserve(ctx, service.Command{ID: "too-many", ItemID: "widget", Quantity: 7}); err != nil || rejected.Applied || rejected.Code != "insufficient_stock" {
		t.Fatalf("d=%+v err=%v", rejected, err)
	}
	if _, err := db.Release(ctx, service.Command{ID: "release", ItemID: "widget", Quantity: 4}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	item, err := db.Get(ctx, "widget")
	if err != nil || item.Stock != 10 {
		t.Fatalf("item=%+v err=%v", item, err)
	}
}

func TestListLowStockDeterministicTakeAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []service.Item{{ID: "cable", Stock: 2}, {ID: "adapter", Stock: 5}, {ID: "battery", Stock: 20}} {
		if decision, err := db.Create(ctx, "create-"+item.ID, item); err != nil || !decision.Applied {
			t.Fatalf("create %s decision=%+v err=%v", item.ID, decision, err)
		}
	}
	items, err := db.ListLowStock(ctx, 5, 1)
	if err != nil || !reflect.DeepEqual(itemIDs(items), []string{"adapter"}) {
		t.Fatalf("take low stock=%v err=%v", itemIDs(items), err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	items, err = db.ListLowStock(ctx, 5, 10)
	if err != nil || !reflect.DeepEqual(itemIDs(items), []string{"adapter", "cable"}) {
		t.Fatalf("restart low stock=%v err=%v", itemIDs(items), err)
	}
}

func itemIDs(items []service.Item) []string {
	ids := make([]string, len(items))
	for index, item := range items {
		ids[index] = item.ID
	}
	return ids
}
