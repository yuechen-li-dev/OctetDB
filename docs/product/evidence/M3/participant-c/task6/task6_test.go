package task6

import (
	"context"
	"fmt"
	"testing"
)

func TestRunMixedWorkloadIsDeterministic(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	seedCatalog(t, ctx, store)

	result, err := store.RunMixedWorkload(ctx, WorkloadRequest{
		LowStockAt:        5,
		PointOrderID:      "order-07",
		MutationCommandID: "restock-main",
		RestockSKU:        "restock-target",
		RestockBy:         4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.LowStock) != 20 {
		t.Fatalf("low stock count = %d, want 20", len(result.LowStock))
	}
	for i, item := range result.LowStock {
		want := fmt.Sprintf("item-%02d", i)
		if item.SKU != want {
			t.Fatalf("low stock[%d] = %s, want %s", i, item.SKU, want)
		}
	}
	if len(result.PaidOrders) != 20 {
		t.Fatalf("paid order count = %d, want 20", len(result.PaidOrders))
	}
	for i, order := range result.PaidOrders {
		want := fmt.Sprintf("order-%02d", i)
		if order.ID != want {
			t.Fatalf("paidOrders[%d] = %s, want %s", i, order.ID, want)
		}
	}
	if !result.PointOrderFound || result.PointOrder.ID != "order-07" || result.PointOrder.Status != paidStatus {
		t.Fatalf("bad point read: found=%v order=%+v", result.PointOrderFound, result.PointOrder)
	}
	if !result.Mutation.Applied || !result.Mutation.Found || result.Mutation.Item.Stock != 14 {
		t.Fatalf("bad mutation result: %+v", result.Mutation)
	}
	item, found, err := store.GetItem(ctx, "restock-target")
	if err != nil {
		t.Fatal(err)
	}
	if !found || item.Stock != 14 {
		t.Fatalf("mutation did not persist: found=%v item=%+v", found, item)
	}
}

func TestRunMixedWorkloadSurvivesRestartAndDatasetScopedKeys(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutItem(ctx, "seed-same-item", Item{SKU: "same-key", Stock: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutOrder(ctx, "seed-same-order", Order{ID: "same-key", Status: paidStatus, Total: 900}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.RunMixedWorkload(ctx, WorkloadRequest{
		LowStockAt:        5,
		PointOrderID:      "same-key",
		MutationCommandID: "restock-same",
		RestockSKU:        "same-key",
		RestockBy:         5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.LowStock) != 1 || result.LowStock[0].SKU != "same-key" {
		t.Fatalf("unexpected low-stock scan after restart: %+v", result.LowStock)
	}
	if len(result.PaidOrders) != 1 || result.PaidOrders[0].ID != "same-key" {
		t.Fatalf("unexpected paid-order scan after restart: %+v", result.PaidOrders)
	}
	if !result.PointOrderFound || result.PointOrder.ID != "same-key" || result.PointOrder.Total != 900 {
		t.Fatalf("unexpected point read: found=%v order=%+v", result.PointOrderFound, result.PointOrder)
	}
	if !result.Mutation.Applied || result.Mutation.Item.SKU != "same-key" || result.Mutation.Item.Stock != 7 {
		t.Fatalf("unexpected mutation result: %+v", result.Mutation)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	item, found, err := store.GetItem(ctx, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	if !found || item.Stock != 7 {
		t.Fatalf("same key item lost after second restart: found=%v item=%+v", found, item)
	}
	order, found, err := store.GetOrder(ctx, "same-key")
	if err != nil {
		t.Fatal(err)
	}
	if !found || order.Status != paidStatus || order.Total != 900 {
		t.Fatalf("same key order lost after second restart: found=%v order=%+v", found, order)
	}
}

func seedCatalog(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	for i := 24; i >= 0; i-- {
		sku := fmt.Sprintf("item-%02d", i)
		if _, err := store.PutItem(ctx, "seed-"+sku, Item{SKU: sku, Stock: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		sku := fmt.Sprintf("healthy-%02d", i)
		if _, err := store.PutItem(ctx, "seed-"+sku, Item{SKU: sku, Stock: 30}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := store.PutItem(ctx, "seed-restock", Item{SKU: "restock-target", Stock: 10}); err != nil {
		t.Fatal(err)
	}
	for i := 24; i >= 0; i-- {
		id := fmt.Sprintf("order-%02d", i)
		if _, err := store.PutOrder(ctx, "seed-"+id, Order{ID: id, Status: paidStatus, Total: 100 + i}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("pending-%02d", i)
		if _, err := store.PutOrder(ctx, "seed-"+id, Order{ID: id, Status: "Pending", Total: 10}); err != nil {
			t.Fatal(err)
		}
	}
}
