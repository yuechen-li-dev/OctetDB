package store

import (
	"context"
	"example.com/octetdb-golden/order/internal/service"
	"path/filepath"
	"testing"
)

func TestOrderTransitionsRestartAndRetry(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if d, err := db.Create(ctx, "create", "o1"); err != nil || !d.Applied {
		t.Fatalf("d=%+v err=%v", d, err)
	}
	paid, err := db.Transition(ctx, "pay", "o1", service.Paid)
	if err != nil || !paid.Applied {
		t.Fatalf("d=%+v err=%v", paid, err)
	}
	retry, err := db.Transition(ctx, "pay", "o1", service.Paid)
	if err != nil || !retry.Duplicate {
		t.Fatalf("d=%+v err=%v", retry, err)
	}
	shipped, err := db.Transition(ctx, "ship", "o1", service.Shipped)
	if err != nil || !shipped.Applied {
		t.Fatalf("d=%+v err=%v", shipped, err)
	}
	invalid, err := db.Transition(ctx, "pay-again", "o1", service.Paid)
	if err != nil || invalid.Applied || invalid.Code != "invalid_transition" {
		t.Fatalf("d=%+v err=%v", invalid, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	order, err := db.Get(ctx, "o1")
	if err != nil || order.Status != service.Shipped {
		t.Fatalf("order=%+v err=%v", order, err)
	}
}
