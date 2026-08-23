package task3

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestPlaceOrderRetryAndFailureAreAtomic(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if _, err := store.PutItem(ctx, "seed-widget", Item{SKU: "widget", Name: "Widget", Stock: 5}); err != nil {
		t.Fatal(err)
	}

	result, decision, err := store.PlaceOrder(ctx, "place-1", PlaceOrderRequest{OrderID: "order-1", SKU: "widget", Quantity: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Applied || result.Duplicate || result.Item.Stock != 2 || result.Order.Quantity != 3 {
		t.Fatalf("unexpected first placement: decision=%+v result=%+v", decision, result)
	}

	retry, retryDecision, err := store.PlaceOrder(ctx, "place-1", PlaceOrderRequest{OrderID: "order-1", SKU: "widget", Quantity: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !retryDecision.Duplicate || !retry.Applied || retry.Item.Stock != 2 || retry.Order.Quantity != 3 {
		t.Fatalf("unexpected retry: decision=%+v result=%+v", retryDecision, retry)
	}
	item, found, err := store.GetItem(ctx, "widget")
	if err != nil {
		t.Fatal(err)
	}
	if !found || item.Stock != 2 {
		t.Fatalf("retry changed inventory: found=%v item=%+v", found, item)
	}

	rejected, rejectedDecision, err := store.PlaceOrder(ctx, "place-too-many", PlaceOrderRequest{OrderID: "order-too-many", SKU: "widget", Quantity: 4})
	if err != nil {
		t.Fatal(err)
	}
	if rejectedDecision.Applied || rejected.Code != "insufficient_stock" {
		t.Fatalf("expected insufficient stock rejection: decision=%+v result=%+v", rejectedDecision, rejected)
	}
	item, found, err = store.GetItem(ctx, "widget")
	if err != nil {
		t.Fatal(err)
	}
	if !found || item.Stock != 2 {
		t.Fatalf("rejection changed inventory: found=%v item=%+v", found, item)
	}
	if order, found, err := store.GetOrder(ctx, "order-too-many"); err != nil || found {
		t.Fatalf("rejection created order: found=%v order=%+v err=%v", found, order, err)
	}
}

func TestPlaceOrderSurvivesRestartAndRetry(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutItem(ctx, "seed-gizmo", Item{SKU: "gizmo", Stock: 4}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.PlaceOrder(ctx, "place-gizmo", PlaceOrderRequest{OrderID: "order-gizmo", SKU: "gizmo", Quantity: 2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = Open(ctx, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	retry, decision, err := store.PlaceOrder(ctx, "place-gizmo", PlaceOrderRequest{OrderID: "order-gizmo", SKU: "gizmo", Quantity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Duplicate || retry.Item.Stock != 2 {
		t.Fatalf("expected restart retry to return retained decision: decision=%+v result=%+v", decision, retry)
	}
	item, found, err := store.GetItem(ctx, "gizmo")
	if err != nil {
		t.Fatal(err)
	}
	if !found || item.Stock != 2 {
		t.Fatalf("restart retry mutated stock: found=%v item=%+v", found, item)
	}
	order, found, err := store.GetOrder(ctx, "order-gizmo")
	if err != nil {
		t.Fatal(err)
	}
	if !found || order.Quantity != 2 {
		t.Fatalf("order did not survive restart: found=%v order=%+v", found, order)
	}
}

func TestConcurrentConflictingPlaceOrdersAreBoundedAndAtomic(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if _, err := store.PutItem(ctx, "seed-shared", Item{SKU: "shared", Stock: 5}); err != nil {
		t.Fatal(err)
	}

	const attempts = 12
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make(chan PlaceOrderResult, attempts)
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			result, _, err := store.PlaceOrder(ctx, fmt.Sprintf("conflict-%02d", i), PlaceOrderRequest{
				OrderID:  fmt.Sprintf("order-%02d", i),
				SKU:      "shared",
				Quantity: 1,
			})
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	var applied, rejected int
	for result := range results {
		if result.Applied {
			applied++
		} else if result.Code == "insufficient_stock" {
			rejected++
		} else {
			t.Fatalf("unexpected result: %+v", result)
		}
	}
	if applied != 5 || rejected != attempts-5 {
		t.Fatalf("unexpected applied/rejected counts: applied=%d rejected=%d", applied, rejected)
	}
	item, found, err := store.GetItem(ctx, "shared")
	if err != nil {
		t.Fatal(err)
	}
	if !found || item.Stock != 0 {
		t.Fatalf("final stock mismatch: found=%v item=%+v", found, item)
	}
	var orderCount int
	for i := 0; i < attempts; i++ {
		if _, found, err := store.GetOrder(ctx, fmt.Sprintf("order-%02d", i)); err != nil {
			t.Fatal(err)
		} else if found {
			orderCount++
		}
	}
	if orderCount != 5 {
		t.Fatalf("created order count = %d, want 5", orderCount)
	}
}
