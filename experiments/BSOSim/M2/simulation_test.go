package bsosim

import (
	"context"
	"encoding/json"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestOctagonDTOsRoundTripAndRejectVersion(t *testing.T) {
	states := map[MessageKind]TransferState{MessageOffer: StateReserved, MessageAccept: StateAccepted, MessageCommit: StateCommitted, MessageAcknowledge: StateCommitted, MessageReconcile: StateExpired}
	for _, kind := range []MessageKind{MessageOffer, MessageAccept, MessageCommit, MessageAcknowledge, MessageReconcile} {
		e := newEnvelope("transfer:1", "bso:1", "bso:2", 42, kind, states[kind])
		b, err := EncodeEnvelope(e)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeEnvelope(b)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(e, got) {
			t.Fatalf("%s envelope mismatch\n%+v\n%+v", kind, e, got)
		}
		b2, _ := EncodeEnvelope(got)
		if string(b) != string(b2) {
			t.Fatalf("%s Octagon bytes are not deterministic", kind)
		}
		baseline, _ := json.Marshal(e)
		t.Logf("%s JSON=%d Octagon=%d", schemaFor(kind), len(baseline), len(b))
	}
	a := TransactionAgent{ProtocolVersion: 1, TransferID: "transfer:1", SenderBSO: "bso:1", ReceiverBSO: "bso:2", Amount: 42, Phase: PhaseAwaitAccept, RetryCount: 2, NextLogicalDeadline: 9, PlacementGeneration: 3, LastMessageKind: MessageOffer}
	c, err := EncodeCheckpoint(a)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := DecodeCheckpoint(c)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, restored) {
		t.Fatalf("checkpoint mismatch\n%+v\n%+v", a, restored)
	}
	baseline, _ := json.Marshal(a)
	t.Logf("TransactionAgentCheckpointV1 JSON=%d Octagon=%d", len(baseline), len(c))
	bad := []byte("ProtocolEnvelopeV1 { ProtocolVersion: 2 Schema: \"TransferOfferV1\" MessageID: \"m\" TransferID: \"t\" From: \"a\" To: \"b\" Kind: MessageKind.Offer Amount: 1 State: TransferState.Reserved Auth: \"x\" }")
	if _, err = DecodeEnvelope(bad); err == nil {
		t.Fatal("wrong version accepted")
	}
}

func TestAgenticSimulationConvergesWithFaults(t *testing.T) {
	c := DefaultConfig()
	c.BSOs, c.Transfers, c.Workers = 10, 80, 4
	c.InitialBalance = 100_000
	r, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Correct || !r.Conservation {
		t.Fatalf("incorrect result: %+v", r)
	}
	if r.Metrics.HotPathCoordinatorMessages != 0 {
		t.Fatal("coordinator entered protocol hot path")
	}
	if r.Metrics.ReconcileEntriesExamined > r.Metrics.Attempted*4 {
		t.Fatalf("non-local reconciliation: %+v", r.Metrics)
	}
}

func TestWorkerLossMigratesOnlyOwnedAgents(t *testing.T) {
	c := DefaultConfig()
	c.BSOs, c.Transfers, c.Workers = 100, 100, 4
	c.Faults = FaultProfiles["worker-loss"]
	c.KillWorker, c.KillRound = 1, 1
	r, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Correct {
		t.Fatalf("worker loss failed: %+v", r)
	}
	if r.Metrics.AgentsMigrated == 0 || r.Metrics.AgentsMigrated >= c.Transfers {
		t.Fatalf("migration was not partition-local: %+v", r.Metrics)
	}
	if r.Metrics.UnrelatedAgentsPaused != 0 {
		t.Fatal("unrelated agents paused")
	}
}

func TestTargetedRestartTouchesAffectedSet(t *testing.T) {
	c := DefaultConfig()
	c.BSOs, c.Transfers, c.Workers = 10_000, 1, 2
	c.Faults = FaultProfiles["none"]
	c.Workload = "affected-set"
	c.RestartBSO, c.RestartRound = bsoID(37), 1 // transfer endpoints are deterministic; restart may be inactive and must not scan.
	r, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Correct {
		t.Fatalf("restart lane failed: %+v", r)
	}
	if r.Metrics.OpenBSODatabases > 2 {
		t.Fatalf("lazy activation failed: %d open", r.Metrics.OpenBSODatabases)
	}
	if r.Metrics.RecoveryBSOsTouched != 2 {
		t.Fatalf("broad recovery: %+v", r.Metrics)
	}
	if r.Metrics.RecoveryAgentsTouched != 1 || r.Metrics.OpenBSODatabases != 2 {
		t.Fatalf("affected set was not exactly one agent/two BSOs: %+v", r.Metrics)
	}
}

func TestDeterministicSemanticDigest(t *testing.T) {
	c := DefaultConfig()
	c.BSOs, c.Transfers, c.Workers = 10, 30, 2
	c.Faults = FaultProfiles["none"]
	a, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if a.CorrectnessDigest != b.CorrectnessDigest {
		t.Fatalf("digest changed: %s %s", a.CorrectnessDigest, b.CorrectnessDigest)
	}
}

func TestOneGoroutinePerWorkerAndRealOverlap(t *testing.T) {
	for _, workers := range []int{1, 8} {
		c := DefaultConfig()
		c.BSOs, c.Transfers, c.Workers = 20, 160, workers
		c.Faults = FaultProfiles["none"]
		r, err := Run(context.Background(), c)
		if err != nil {
			t.Fatal(err)
		}
		if r.Metrics.WorkerGoroutinesStarted != workers || r.Metrics.WorkerGoroutinesStopped != workers {
			t.Fatalf("worker lifecycle is not bounded by configured workers: %+v", r.Metrics)
		}
		if delta := r.Metrics.GoroutinesPeak - r.Metrics.GoroutinesIdle; delta < workers-1 || delta > workers+1 {
			t.Fatalf("unexpected worker goroutine delta %d for %d workers", delta, workers)
		}
		if workers == 1 && r.Metrics.WorkerActivePeak != 1 {
			t.Fatalf("one worker overlap=%d", r.Metrics.WorkerActivePeak)
		}
		if workers == 8 && r.Metrics.WorkerActivePeak < 2 {
			t.Fatalf("workers never overlapped: %+v", r.Metrics)
		}
	}
}

func TestConcurrentSameBSOAdmissionIsSafe(t *testing.T) {
	for _, workload := range []string{"hot-merchant", "hot-payer"} {
		c := DefaultConfig()
		c.BSOs, c.Transfers, c.Workers = 16, 240, 8
		c.InitialBalance = 500
		c.Faults = FaultProfiles["fun"]
		c.Workload = workload
		r, err := Run(context.Background(), c)
		if err != nil {
			t.Fatalf("%s: %v", workload, err)
		}
		if !r.Correct || !r.Conservation || r.Metrics.DoubleDebits != 0 || r.Metrics.DoubleCredits != 0 {
			t.Fatalf("%s same-BSO admission violated financial truth: %+v", workload, r)
		}
		if r.Metrics.WorkerActivePeak < 2 {
			t.Fatalf("%s did not exercise concurrent workers", workload)
		}
	}
}

func TestMigrationAfterOutboundOperationConverges(t *testing.T) {
	c := DefaultConfig()
	c.BSOs, c.Transfers, c.Workers = 40, 160, 4
	c.Faults = FaultProfiles["fun"]
	c.KillWorker, c.KillRound = 1, 3 // offers/replies are queued or awaiting at this seam.
	r, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !r.Correct || r.Metrics.AgentsMigrated == 0 || r.Metrics.UnrelatedAgentsPaused != 0 {
		t.Fatalf("migration seam failed: %+v", r)
	}
}

func TestCancellationStopsWorkers(t *testing.T) {
	before := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	c := DefaultConfig()
	c.BSOs, c.Transfers, c.Workers = 100, 2000, 8
	done := make(chan error, 1)
	go func() { _, err := Run(ctx, c); done <- err }()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() < before+c.BSOs+c.Workers && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if runtime.NumGoroutine() < before+c.BSOs+c.Workers {
		t.Fatal("workers did not start before cancellation deadline")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled run unexpectedly succeeded")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled simulation leaked or failed to stop")
	}
	for i := 0; i < 100 && runtime.NumGoroutine() > before+2; i++ {
		runtime.Gosched()
	}
	if after := runtime.NumGoroutine(); after > before+2 {
		t.Fatalf("goroutines leaked: before=%d after=%d", before, after)
	}
}

func TestSemanticDigestNormalizedAcrossWorkerCounts(t *testing.T) {
	c := DefaultConfig()
	c.BSOs, c.Transfers = 30, 120
	c.InitialBalance = 1_000_000
	c.Faults = FaultProfiles["none"]
	c.Workers = 1
	one, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	c.Workers = 8
	eight, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if one.CorrectnessDigest != eight.CorrectnessDigest {
		t.Fatalf("logical terminal state changed across scheduling: %s != %s", one.CorrectnessDigest, eight.CorrectnessDigest)
	}
}
