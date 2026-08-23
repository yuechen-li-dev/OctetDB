package store

import (
	"context"
	"example.com/octetdb-golden/webhook/internal/service"
	"path/filepath"
	"testing"
)

func TestWebhookDuplicateAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "db")
	db, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	event := service.Event{ID: "evt/external-1", Result: "invoice-created"}
	first, err := db.Process(ctx, event.ID, event)
	if err != nil || !first.Applied {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	retry, err := db.Process(ctx, event.ID, event)
	if err != nil || !retry.Duplicate || retry.Event.Result != "invoice-created" {
		t.Fatalf("retry=%+v err=%v", retry, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	got, ok, err := db.Get(ctx, event.ID)
	if err != nil || !ok || got.Status != "processed" || got.Result != "invoice-created" {
		t.Fatalf("got=%+v ok=%v err=%v", got, ok, err)
	}
}
