package hidden_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"participant-c/task3"
	"participant-c/task6"
)

func TestParticipantCCrossDatasetConflictRetryAndRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := task3.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if decision, err := store.PutItem(ctx, "seed", task3.Item{SKU: "widget", Stock: 5}); err != nil || !decision.Applied {
		t.Fatalf("seed=%+v err=%v", decision, err)
	}

	type outcome struct {
		result task3.PlaceOrderResult
		err    error
	}
	outcomes := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			result, _, err := store.PlaceOrder(ctx, "place-"+id, task3.PlaceOrderRequest{OrderID: id, SKU: "widget", Quantity: 4})
			outcomes <- outcome{result, err}
		}(id)
	}
	wg.Wait()
	close(outcomes)
	applied, rejected := 0, 0
	var acceptedID string
	for value := range outcomes {
		if value.err != nil {
			t.Fatal(value.err)
		}
		if value.result.Applied {
			applied++
			acceptedID = value.result.Order.ID
		} else if value.result.Code == "insufficient_stock" {
			rejected++
		}
	}
	if applied != 1 || rejected != 1 {
		t.Fatalf("applied=%d rejected=%d", applied, rejected)
	}
	item, found, err := store.GetItem(ctx, "widget")
	if err != nil || !found || item.Stock != 1 {
		t.Fatalf("item=%+v found=%v err=%v", item, found, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = task3.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	retry, decision, err := store.PlaceOrder(ctx, "place-"+acceptedID, task3.PlaceOrderRequest{OrderID: acceptedID, SKU: "widget", Quantity: 4})
	if err != nil || !decision.Duplicate || !retry.Duplicate {
		t.Fatalf("retry=%+v decision=%+v err=%v", retry, decision, err)
	}
	item, _, err = store.GetItem(ctx, "widget")
	if err != nil || item.Stock != 1 {
		t.Fatalf("retry changed item=%+v err=%v", item, err)
	}
	otherID := "a"
	if acceptedID == "a" {
		otherID = "b"
	}
	if _, exists, err := store.GetOrder(ctx, otherID); err != nil || exists {
		t.Fatalf("rejected order partially persisted: id=%s exists=%v err=%v", otherID, exists, err)
	}
}

func TestParticipantCMixedScansPointReadIdentityAndDedupe(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	store, err := task6.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 25)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%02d", i)
	}
	reversed := append([]string(nil), keys...)
	sort.Sort(sort.Reverse(sort.StringSlice(reversed)))
	for i, key := range reversed {
		if _, err := store.PutItem(ctx, "put-item-"+key, task6.Item{SKU: key, Stock: i % 3}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PutOrder(ctx, "put-order-"+key, task6.Order{ID: key, Status: "Paid", Total: i}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := store.RunMixedWorkload(ctx, task6.WorkloadRequest{
		LowStockAt: 2, PointOrderID: "key-07", MutationCommandID: "restock-once", RestockSKU: "key-24", RestockBy: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.LowStock) != 20 || len(result.PaidOrders) != 20 {
		t.Fatalf("limits: low=%d paid=%d", len(result.LowStock), len(result.PaidOrders))
	}
	lowIDs, paidIDs := make([]string, 20), make([]string, 20)
	for i := range result.LowStock {
		lowIDs[i] = result.LowStock[i].SKU
	}
	for i := range result.PaidOrders {
		paidIDs[i] = result.PaidOrders[i].ID
	}
	if !reflect.DeepEqual(lowIDs, keys[:20]) || !reflect.DeepEqual(paidIDs, keys[:20]) {
		t.Fatalf("scan order low=%v paid=%v", lowIDs, paidIDs)
	}
	if !result.PointOrderFound || result.PointOrder.ID != "key-07" {
		t.Fatalf("point=%+v found=%v", result.PointOrder, result.PointOrderFound)
	}
	item, itemFound, err := store.GetItem(ctx, "key-07")
	if err != nil || !itemFound {
		t.Fatalf("item found=%v err=%v", itemFound, err)
	}
	order, orderFound, err := store.GetOrder(ctx, "key-07")
	if err != nil || !orderFound || item.SKU != order.ID {
		t.Fatalf("dataset identity item=%+v order=%+v err=%v", item, order, err)
	}
	mutatedStock := result.Mutation.Item.Stock
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = task6.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	result, err = store.RunMixedWorkload(ctx, task6.WorkloadRequest{
		LowStockAt: 2, PointOrderID: "key-07", MutationCommandID: "restock-once", RestockSKU: "key-24", RestockBy: 3,
	})
	if err != nil || !result.Mutation.Duplicate || result.Mutation.Item.Stock != mutatedStock {
		t.Fatalf("restart duplicate mutation=%+v err=%v", result.Mutation, err)
	}
}
