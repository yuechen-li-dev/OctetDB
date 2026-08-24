package bsosim

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
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
