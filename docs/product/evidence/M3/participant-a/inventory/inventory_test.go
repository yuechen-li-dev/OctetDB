package inventory_test

import (
	"context"
	"testing"

	"participant-a/inventory"
)

func TestInventoryReservationRetryReleaseAndRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()

	store, err := inventory.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.CreateItem(ctx, "create-widget", "widget", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !created.Applied || created.Item.Available != 10 || created.Item.Reserved != 0 {
		t.Fatalf("unexpected create result: %+v", created)
	}

	reserved, err := store.Reserve(ctx, "reserve-order-1", "widget", 4)
	if err != nil {
		t.Fatal(err)
	}
	if !reserved.Applied || reserved.Duplicate || reserved.Item.Available != 6 || reserved.Item.Reserved != 4 {
		t.Fatalf("unexpected first reservation: %+v", reserved)
	}

	// The same command retry must return the original decision without applying
	// the four-unit reservation a second time.
	retry, err := store.Reserve(ctx, "reserve-order-1", "widget", 4)
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Applied || !retry.Duplicate || retry.Sequence != reserved.Sequence || retry.Item != reserved.Item {
		t.Fatalf("retry was not the exact original decision: first=%+v retry=%+v", reserved, retry)
	}

	item, found, err := store.GetItem(ctx, "widget")
	if err != nil || !found {
		t.Fatalf("read after retry: found=%v err=%v", found, err)
	}
	if item.Available != 6 || item.Reserved != 4 {
		t.Fatalf("retry reapplied reservation: %+v", item)
	}

	over, err := store.Reserve(ctx, "reserve-order-2", "widget", 7)
	if err != nil {
		t.Fatal(err)
	}
	if over.Applied || over.Code != "insufficient_stock" || over.Item != item {
		t.Fatalf("over-reservation was not rejected with unchanged state: %+v", over)
	}

	released, err := store.Release(ctx, "release-order-1", "widget", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !released.Applied || released.Item.Available != 8 || released.Item.Reserved != 2 {
		t.Fatalf("unexpected release result: %+v", released)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = inventory.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	item, found, err = store.GetItem(ctx, "widget")
	if err != nil || !found {
		t.Fatalf("read after restart: found=%v err=%v", found, err)
	}
	if item.Available != 8 || item.Reserved != 2 {
		t.Fatalf("restart lost inventory state: %+v", item)
	}

	// Dedupe state also survives restart.
	restartRetry, err := store.Reserve(ctx, "reserve-order-1", "widget", 4)
	if err != nil {
		t.Fatal(err)
	}
	if !restartRetry.Duplicate || restartRetry.Sequence != reserved.Sequence || restartRetry.Item != reserved.Item {
		t.Fatalf("restart retry did not return the original decision: %+v", restartRetry)
	}
	item, _, err = store.GetItem(ctx, "widget")
	if err != nil {
		t.Fatal(err)
	}
	if item.Available != 8 || item.Reserved != 2 {
		t.Fatalf("restart retry reapplied old reservation: %+v", item)
	}
}

func TestInventoryNeverPersistsNegativeQuantities(t *testing.T) {
	ctx := context.Background()
	store, err := inventory.Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	negative, err := store.CreateItem(ctx, "create-negative", "bad", -1)
	if err != nil {
		t.Fatal(err)
	}
	if negative.Applied || negative.Code != "negative_stock" {
		t.Fatalf("negative initial stock was not rejected: %+v", negative)
	}
	if _, found, err := store.GetItem(ctx, "bad"); err != nil || found {
		t.Fatalf("rejected item persisted: found=%v err=%v", found, err)
	}

	if _, err := store.CreateItem(ctx, "create-small", "small", 2); err != nil {
		t.Fatal(err)
	}
	over, err := store.Reserve(ctx, "reserve-too-many", "small", 3)
	if err != nil {
		t.Fatal(err)
	}
	if over.Applied || over.Code != "insufficient_stock" || over.Item.Available != 2 || over.Item.Reserved != 0 {
		t.Fatalf("over-reservation changed quantities: %+v", over)
	}
	doubleRelease, err := store.Release(ctx, "release-too-many", "small", 1)
	if err != nil {
		t.Fatal(err)
	}
	if doubleRelease.Applied || doubleRelease.Code != "insufficient_reserved" || doubleRelease.Item.Available != 2 || doubleRelease.Item.Reserved != 0 {
		t.Fatalf("over-release changed quantities: %+v", doubleRelease)
	}
}
