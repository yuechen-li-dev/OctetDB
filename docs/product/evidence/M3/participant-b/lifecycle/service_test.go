package lifecycle_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yuechen-li-dev/octetdb-m3-participant-b/lifecycle"
)

type recordingSender struct {
	mu    sync.Mutex
	mails []lifecycle.PaymentEmail
}

func (s *recordingSender) Send(_ context.Context, email lifecycle.PaymentEmail) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mails = append(s.mails, email)
	return nil
}

func (s *recordingSender) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.mails)
}

func TestDurableRejectionsEmailAndRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir()
	sender := &recordingSender{}
	service, err := lifecycle.Open(ctx, path, lifecycle.Options{EmailSender: sender})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Create(ctx, "order-42", "create-1"); err != nil {
		t.Fatal(err)
	}

	shipRejected, err := service.Ship(ctx, "order-42", "ship-early-1")
	if err != nil {
		t.Fatal(err)
	}
	if shipRejected.Decision.Applied || shipRejected.Decision.Code != "not_paid" || shipRejected.Decision.Duplicate {
		t.Fatalf("ship-before-pay decision = %+v", shipRejected.Decision)
	}
	shipRetry, err := service.Ship(ctx, "order-42", "ship-early-1")
	if err != nil {
		t.Fatal(err)
	}
	if !shipRetry.Decision.Duplicate || shipRetry.Decision.Code != "not_paid" || shipRetry.Order != shipRejected.Order {
		t.Fatalf("durable rejected retry = %+v", shipRetry)
	}

	paid, err := service.Pay(ctx, "order-42", "payment-1")
	if err != nil {
		t.Fatal(err)
	}
	if !paid.Decision.Applied || paid.Order.State != lifecycle.Paid || paid.EmailError != nil || sender.count() != 1 {
		t.Fatalf("paid = %+v, emails=%d", paid, sender.count())
	}

	doublePay, err := service.Pay(ctx, "order-42", "payment-2")
	if err != nil {
		t.Fatal(err)
	}
	if doublePay.Decision.Applied || doublePay.Decision.Code != "already_paid" || sender.count() != 1 {
		t.Fatalf("double pay = %+v, emails=%d", doublePay, sender.count())
	}
	doublePayRetry, err := service.Pay(ctx, "order-42", "payment-2")
	if err != nil {
		t.Fatal(err)
	}
	if !doublePayRetry.Decision.Duplicate || doublePayRetry.Decision.Code != "already_paid" || sender.count() != 1 {
		t.Fatalf("double pay retry = %+v, emails=%d", doublePayRetry, sender.count())
	}

	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	service, err = lifecycle.Open(ctx, path, lifecycle.Options{EmailSender: sender})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	afterRestart, err := service.Pay(ctx, "order-42", "payment-1")
	if err != nil {
		t.Fatal(err)
	}
	if !afterRestart.Decision.Duplicate || afterRestart.Decision.Code != "" || sender.count() != 1 {
		t.Fatalf("restart payment retry = %+v, emails=%d", afterRestart, sender.count())
	}
	order, found, err := service.Order(ctx, "order-42")
	if err != nil || !found || order.State != lifecycle.Paid {
		t.Fatalf("persisted order = %+v, found=%v, err=%v", order, found, err)
	}
}

func TestUnexpectedCallbackErrorDoesNotConsumePaymentIdentity(t *testing.T) {
	ctx := context.Background()
	var fail atomic.Bool
	fail.Store(true)
	sender := &recordingSender{}
	service, err := lifecycle.Open(ctx, t.TempDir(), lifecycle.Options{
		EmailSender: sender,
		BeforePersist: func(action string, _ lifecycle.Order) error {
			if action == "pay" && fail.Load() {
				return errors.New("temporary validation failure")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	if _, err := service.Create(ctx, "order-failure", "create-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Pay(ctx, "order-failure", "payment-1"); err == nil {
		t.Fatal("Pay succeeded despite unexpected callback error")
	}
	fail.Store(false)
	retry, err := service.Pay(ctx, "order-failure", "payment-1")
	if err != nil {
		t.Fatal(err)
	}
	if retry.Decision.Duplicate || !retry.Decision.Applied || retry.Order.State != lifecycle.Paid || sender.count() != 1 {
		t.Fatalf("retry after callback error = %+v, emails=%d", retry, sender.count())
	}
}

func TestConcurrentConflictingPaymentsHaveOneWinner(t *testing.T) {
	ctx := context.Background()
	sender := &recordingSender{}
	service, err := lifecycle.Open(ctx, t.TempDir(), lifecycle.Options{EmailSender: sender})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	if _, err := service.Create(ctx, "order-race", "create-1"); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	type result struct {
		outcome lifecycle.Outcome
		err     error
	}
	results := make(chan result, 2)
	for _, requestID := range []string{"payment-a", "payment-b"} {
		go func(requestID string) {
			<-start
			outcome, err := service.Pay(ctx, "order-race", requestID)
			results <- result{outcome: outcome, err: err}
		}(requestID)
	}
	close(start)
	var applied, rejected int
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		outcome := result.outcome
		if outcome.Decision.Applied {
			applied++
		} else if outcome.Decision.Code == "already_paid" {
			rejected++
		} else {
			t.Fatalf("unexpected concurrent outcome: %+v", outcome)
		}
	}
	if applied != 1 || rejected != 1 || sender.count() != 1 {
		t.Fatalf("applied=%d rejected=%d emails=%d", applied, rejected, sender.count())
	}
}
