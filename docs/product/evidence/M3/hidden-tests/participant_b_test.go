package hidden_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yuechen-li-dev/octetdb"
	"github.com/yuechen-li-dev/octetdb-m3-participant-b/lifecycle"
	"github.com/yuechen-li-dev/octetdb-m3-participant-b/webhook"
)

func TestParticipantBWebhookCallbackCount(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	processor, err := webhook.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	mutation := func(_ *octetdb.Tx) (string, error) {
		calls.Add(1)
		return "processed", nil
	}
	first, err := processor.Process(ctx, "provider-42", mutation)
	if err != nil || first.Duplicate {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	if err := processor.Close(); err != nil {
		t.Fatal(err)
	}
	processor, err = webhook.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer processor.Close()
	retry, err := processor.Process(ctx, "provider-42", mutation)
	if err != nil || !retry.Duplicate || retry.Delivery != first.Delivery || calls.Load() != 1 {
		t.Fatalf("retry=%+v first=%+v calls=%d err=%v", retry, first, calls.Load(), err)
	}
}

type recordingSender struct {
	mu  sync.Mutex
	ids []string
}

func (s *recordingSender) Send(_ context.Context, email lifecycle.PaymentEmail) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ids = append(s.ids, email.MessageID)
	return nil
}

func TestParticipantBDurableRejectionAndAbortRetry(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	var failPay atomic.Bool
	failPay.Store(true)
	sender := &recordingSender{}
	service, err := lifecycle.Open(ctx, path, lifecycle.Options{
		EmailSender: sender,
		BeforePersist: func(action string, _ lifecycle.Order) error {
			if action == "pay" && failPay.Swap(false) {
				return errors.New("injected operational failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result, err := service.Create(ctx, "order-1", "create-request"); err != nil || !result.Decision.Applied {
		t.Fatalf("create=%+v err=%v", result, err)
	}
	rejected, err := service.Ship(ctx, "order-1", "premature-ship")
	if err != nil || rejected.Decision.Applied || rejected.Decision.Code != "not_paid" {
		t.Fatalf("rejection=%+v err=%v", rejected, err)
	}
	if _, err := service.Pay(ctx, "order-1", "pay-request"); err == nil {
		t.Fatal("injected callback error unexpectedly accepted")
	}
	paid, err := service.Pay(ctx, "order-1", "pay-request")
	if err != nil || !paid.Decision.Applied || paid.Decision.Duplicate || paid.Order.State != lifecycle.Paid {
		t.Fatalf("retry after abort=%+v err=%v", paid, err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	service, err = lifecycle.Open(ctx, path, lifecycle.Options{EmailSender: sender})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	retry, err := service.Ship(ctx, "order-1", "premature-ship")
	if err != nil || !retry.Decision.Duplicate || retry.Decision.Code != "not_paid" || retry.Order.State != lifecycle.Created {
		t.Fatalf("rejected retry=%+v err=%v", retry, err)
	}
	sender.mu.Lock()
	defer sender.mu.Unlock()
	if len(sender.ids) != 1 {
		t.Fatalf("email sends=%v, want exactly one", sender.ids)
	}
}
