package webhook_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/yuechen-li-dev/octetdb"
	"github.com/yuechen-li-dev/octetdb-m3-participant-b/webhook"
)

func TestProcessDuplicateAndRestartDoNotRerunMutation(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	processor, err := webhook.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	first, err := processor.Process(ctx, "provider-event-42", func(_ *octetdb.Tx) (string, error) {
		calls.Add(1)
		return "accepted", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || first.Delivery.Status != "completed" || first.Delivery.Result != "accepted" {
		t.Fatalf("first outcome = %+v", first)
	}
	if first.CommandID != webhook.CommandID("provider-event-42") {
		t.Fatalf("command ID = %q", first.CommandID)
	}

	retry, err := processor.Process(ctx, "provider-event-42", func(_ *octetdb.Tx) (string, error) {
		calls.Add(1)
		return "must not run", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Duplicate || retry.Delivery != first.Delivery || calls.Load() != 1 {
		t.Fatalf("retry = %+v, calls = %d", retry, calls.Load())
	}
	if err := processor.Close(); err != nil {
		t.Fatal(err)
	}

	processor, err = webhook.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = processor.Close() })
	afterRestart, err := processor.Process(ctx, "provider-event-42", func(_ *octetdb.Tx) (string, error) {
		calls.Add(1)
		return "must not run", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !afterRestart.Duplicate || afterRestart.Delivery != first.Delivery || calls.Load() != 1 {
		t.Fatalf("restart retry = %+v, calls = %d", afterRestart, calls.Load())
	}
	delivery, found, err := processor.Delivery(ctx, "provider-event-42")
	if err != nil || !found || delivery != first.Delivery {
		t.Fatalf("stored delivery = %+v, found=%v, err=%v", delivery, found, err)
	}
}
