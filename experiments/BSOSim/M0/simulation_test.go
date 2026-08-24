package bsosim

import (
	"context"
	"testing"
)

func TestEveryProtocolMessageIsReplaySafe(t *testing.T) {
	ctx := context.Background()
	metrics := Metrics{}
	root := t.TempDir()
	sender, err := openBSO(ctx, root, bsoID(0), 1_000, &metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.close()
	receiver, err := openBSO(ctx, root, bsoID(1), 1_000, &metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.close()

	attempt := Attempt{ID: "transfer:replay", From: sender.id, To: receiver.id, Amount: 100}
	offer, send, err := sender.reserve(ctx, attempt, 0)
	if err != nil || !send {
		t.Fatalf("reserve send=%v err=%v", send, err)
	}
	accept, err := receiver.handle(ctx, offer)
	if err != nil || accept == nil || accept.Kind != MessageAccept {
		t.Fatalf("accept=%+v err=%v", accept, err)
	}
	duplicateAccept, err := receiver.handle(ctx, offer)
	if err != nil || duplicateAccept == nil || duplicateAccept.Kind != MessageAccept {
		t.Fatalf("duplicate offer response=%+v err=%v", duplicateAccept, err)
	}
	commit, err := sender.handle(ctx, *accept)
	if err != nil || commit == nil || commit.Kind != MessageCommit {
		t.Fatalf("commit=%+v err=%v", commit, err)
	}
	duplicateCommit, err := sender.handle(ctx, *accept)
	if err != nil || duplicateCommit == nil || duplicateCommit.Kind != MessageCommit {
		t.Fatalf("duplicate accept response=%+v err=%v", duplicateCommit, err)
	}
	ack, err := receiver.handle(ctx, *commit)
	if err != nil || ack == nil || ack.Kind != MessageAck {
		t.Fatalf("ack=%+v err=%v", ack, err)
	}
	duplicateAck, err := receiver.handle(ctx, *commit)
	if err != nil || duplicateAck == nil || duplicateAck.Kind != MessageAck {
		t.Fatalf("duplicate commit response=%+v err=%v", duplicateAck, err)
	}
	if response, err := sender.handle(ctx, *ack); err != nil || response != nil {
		t.Fatalf("ack response=%+v err=%v", response, err)
	}
	if response, err := sender.handle(ctx, *ack); err != nil || response != nil {
		t.Fatalf("duplicate ack response=%+v err=%v", response, err)
	}
	reconcile := newEnvelope(attempt.ID, sender.id, receiver.id, attempt.Amount, MessageReconcile, StateAcknowledged)
	if response, err := receiver.handle(ctx, reconcile); err != nil || response == nil || response.Kind != MessageAck {
		t.Fatalf("reconcile response=%+v err=%v", response, err)
	}
	if response, err := receiver.handle(ctx, reconcile); err != nil || response == nil || response.Kind != MessageAck {
		t.Fatalf("duplicate reconcile response=%+v err=%v", response, err)
	}

	senderState, _ := sender.load(ctx)
	receiverState, _ := receiver.load(ctx)
	if senderState.Balance != 900 || senderState.Reserved != 0 || receiverState.Balance != 1_100 {
		t.Fatalf("sender=%+v receiver=%+v", senderState, receiverState)
	}
	if len(senderState.Audit) != 1 || len(receiverState.Audit) != 1 {
		t.Fatalf("duplicate value application: sender audit=%v receiver audit=%v", senderState.Audit, receiverState.Audit)
	}
	if metrics.DuplicatesSuppressed < 5 {
		t.Fatalf("duplicates suppressed=%d", metrics.DuplicatesSuppressed)
	}
}

func TestAuthenticationFailuresDoNotChangeValue(t *testing.T) {
	ctx := context.Background()
	metrics := Metrics{}
	receiver, err := openBSO(ctx, t.TempDir(), bsoID(1), 1_000, &metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.close()
	before, _ := receiver.load(ctx)

	cases := []Envelope{
		newEnvelope("unknown", bsoID(0), receiver.id, 100, MessageCommit, StateCommitted),
		newEnvelope("wrong-receiver", bsoID(0), bsoID(2), 100, MessageOffer, StateReserved),
		newEnvelope("wrong-sender", receiver.id, receiver.id, 100, MessageOffer, StateReserved),
		newEnvelope("tampered", bsoID(0), receiver.id, 100, MessageOffer, StateReserved),
	}
	cases[3].Amount++
	for _, envelope := range cases {
		if _, err := receiver.handle(ctx, envelope); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := receiver.load(ctx)
	if before.Balance != after.Balance || before.Reserved != after.Reserved || len(after.Audit) != 0 {
		t.Fatalf("value changed after invalid envelopes: before=%+v after=%+v", before, after)
	}
	if metrics.AuthenticationFailures != 3 {
		t.Fatalf("authentication failures=%d, want 3 (unknown transfer is authentic)", metrics.AuthenticationFailures)
	}
}

func TestFaultsResponseLossAndCrashesConvergeDeterministically(t *testing.T) {
	config := DefaultConfig()
	config.BSOs, config.Transfers, config.InitialBalance = 10, 60, 20_000
	config.Faults = FaultProfiles["mean"]
	config.CrashSchedule = []CrashPoint{CrashAfterReserve, CrashAfterAccept, CrashAfterSenderCommit, CrashAfterReceiverCommit, CrashBeforeAck}
	first, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Conservation || first.Metrics.Unresolved != 0 || first.Metrics.DoubleDebits != 0 || first.Metrics.DoubleCredits != 0 {
		t.Fatalf("incorrect result: %+v", first)
	}
	if first.Metrics.Crashes != len(config.CrashSchedule) {
		t.Fatalf("crashes=%d", first.Metrics.Crashes)
	}
	if first.Metrics.Retries == 0 || first.Metrics.DuplicatesSuppressed == 0 {
		t.Fatalf("fault lanes not exercised: %+v", first.Metrics)
	}
	if first.CorrectnessDigest != second.CorrectnessDigest {
		t.Fatalf("digest differs: %s vs %s", first.CorrectnessDigest, second.CorrectnessDigest)
	}
}

func TestReservationExpiryReleasesGhostLockedMoney(t *testing.T) {
	config := DefaultConfig()
	config.BSOs, config.Transfers, config.InitialBalance = 4, 20, 10_000
	config.Faults = FaultProfile{Name: "black-hole", DropRate: 1}
	config.ReservationExpiry, config.MaxRounds = 2, 6
	result, err := Run(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Conservation || result.FinalTotal != result.InitialTotal || result.Metrics.Rejected != config.Transfers || result.Metrics.Unresolved != 0 {
		t.Fatalf("reservation expiry failed: %+v", result)
	}
}

func TestAcceptedReservationExpiryReconcilesWithoutCentralEditing(t *testing.T) {
	ctx := context.Background()
	metrics := Metrics{}
	root := t.TempDir()
	sender, err := openBSO(ctx, root, bsoID(0), 1_000, &metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.close()
	receiver, err := openBSO(ctx, root, bsoID(1), 1_000, &metrics)
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.close()
	attempt := Attempt{ID: "transfer:expire-accepted", From: sender.id, To: receiver.id, Amount: 100}
	offer, _, err := sender.reserve(ctx, attempt, 0)
	if err != nil {
		t.Fatal(err)
	}
	accept, err := receiver.handle(ctx, offer)
	if err != nil || accept == nil {
		t.Fatalf("accept=%+v err=%v", accept, err)
	}
	// Lose Accept. The sender knows only its durable reservation; expiry sends a
	// fact, and the receiver independently expires its uncommitted acceptance.
	s := &simulation{ctx: ctx, config: Config{ReservationExpiry: 2}, bsos: map[string]*durableBSO{sender.id: sender, receiver.id: receiver}, orderedIDs: []string{sender.id, receiver.id}, metrics: Metrics{}}
	s.transport = newTransport(1, FaultProfiles["none"], &s.metrics)
	if err := s.reconcile(2); err != nil {
		t.Fatal(err)
	}
	if err := s.transport.drain(s.deliver); err != nil {
		t.Fatal(err)
	}
	senderState, _ := sender.load(ctx)
	receiverState, _ := receiver.load(ctx)
	if senderState.Balance != 1_000 || senderState.Reserved != 0 || senderState.Outgoing[attempt.ID].State != StateExpired || receiverState.Incoming[attempt.ID].State != StateExpired {
		t.Fatalf("sender=%+v receiver=%+v", senderState, receiverState)
	}
}

func TestTransportInjectsDropDuplicateDelayAndReorder(t *testing.T) {
	metrics := Metrics{}
	transport := newTransport(42, FaultProfile{Name: "test", DropRate: 0.20, DuplicateRate: 0.50, MaxDelay: 4, ReorderWindow: 8}, &metrics)
	for i := 0; i < 100; i++ {
		transport.send(newEnvelope("transfer:test", bsoID(0), bsoID(1), Money(i+1), MessageOffer, StateReserved))
	}
	delivered := 0
	if err := transport.drain(func(Envelope) error { delivered++; return nil }); err != nil {
		t.Fatal(err)
	}
	if metrics.MessagesDropped == 0 || metrics.DuplicatesInjected == 0 || metrics.DelayedOrReordered == 0 || delivered <= 100-metrics.MessagesDropped {
		t.Fatalf("fault injection not exercised: metrics=%+v delivered=%d", metrics, delivered)
	}
}

func TestHotWorkloadsAndGlobalControl(t *testing.T) {
	for _, workload := range []string{"hot-merchant", "hot-payer"} {
		t.Run(workload, func(t *testing.T) {
			config := DefaultConfig()
			config.BSOs, config.Transfers, config.InitialBalance = 10, 80, 1_000
			config.Workload, config.Faults = workload, FaultProfiles["none"]
			comparison, err := RunComparison(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			if !comparison.BSO.Conservation || !comparison.Global.Conservation {
				t.Fatalf("comparison=%+v", comparison)
			}
			if comparison.BSO.Metrics.DoubleDebits != 0 || comparison.BSO.Metrics.DoubleCredits != 0 {
				t.Fatalf("double apply: %+v", comparison.BSO.Metrics)
			}
			if workload == "hot-payer" && comparison.BSO.Metrics.Rejected == 0 {
				t.Fatalf("hot payer did not exercise insufficient funds")
			}
			if comparison.Global.Metrics.GlobalSerializationOps != config.Transfers {
				t.Fatalf("global ops=%d", comparison.Global.Metrics.GlobalSerializationOps)
			}
		})
	}
}
