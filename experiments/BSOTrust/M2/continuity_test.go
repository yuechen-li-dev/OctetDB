package bsotrustm2

import (
	"context"
	"testing"
)

func TestContinuitySuite(t *testing.T) {
	r, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !r.Correct || !r.Conservation || !r.RuntimeWithinBudget {
		t.Fatalf("incorrect result: %+v", r)
	}
	if r.Comparison.M1PostRemovalSettled != 0 || r.Comparison.M2PostRemovalSettled != 600 || r.FailoverSuccess != 1 {
		t.Fatalf("monopoly failover mismatch: %+v", r.Comparison)
	}
	if r.PrimaryUsageShare != 1 || r.ContinuityCoverage != 1 || r.CaptiveRelationships != 0 {
		t.Fatalf("concentration/captivity mismatch: %+v", r)
	}
	if r.DirectContinuityCalls != 0 || r.StrictFinancialMutations != 0 || r.UnrelatedRelationshipsTouched != 0 {
		t.Fatalf("locality/dry-run mismatch: %+v", r)
	}
}

func TestStaleProofRevalidatesAndPeriodicMaintenanceRepairs(t *testing.T) {
	r, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(r.RotRows) != 2 || r.RotRows[0].Result != "fail; stale proof revalidated and rejected" || r.StaleRevalidations == 0 {
		t.Fatalf("stale proof authorized failover: %+v", r.RotRows)
	}
	if r.PeriodicDetectedRot != 1 || r.PeriodicRemediated != 1 {
		t.Fatalf("periodic maintenance failed: %+v", r)
	}
}

func TestPolicyVersionDuplicateThresholdAndReplicaInvariants(t *testing.T) {
	r, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.PolicyInvalidations != 1 || r.VersionInvalidations != 1 {
		t.Fatalf("proof invalidation failed: %+v", r)
	}
	if r.DuplicateProofAlternates != 1 || r.ReplicaAlternates != 0 || r.ThresholdSettled != 1 {
		t.Fatalf("independent authority count failed: %+v", r)
	}
	if r.BridgeContinuityHealthy != 1 || r.BridgeHotPathCalls != 0 {
		t.Fatalf("bridge became hot-path mandatory: %+v", r)
	}
}

func TestCheckIsCanonicalDryRun(t *testing.T) {
	e := &continuityEngine{providers: providers()}
	rel := relationship("dry-run", true, false)
	result := e.check(&rel, rel.Continuity[0], "general:a", "identity:b", 5)
	if !result.Viable || result.FinancialMutation || result.ProviderCalls != 1 || result.DurableRecords != 1 || result.CanonicalBytes == 0 {
		t.Fatalf("bad dry run: %+v", result)
	}
	if len(e.providerRetention) != 1 {
		t.Fatalf("provider retention was not bounded and dedicated: %+v", e.providerRetention)
	}
	first := result.Proof.ProofID
	result = e.check(&rel, rel.Continuity[0], "general:a", "identity:b", 5)
	if result.Proof.ProofID != first || countDistinctProofAlternates(rel, Identity) != 1 {
		t.Fatal("canonical ID/idempotency failed")
	}
}

func TestCheckEnforcesTransactionClassAndAmountScope(t *testing.T) {
	e := &continuityEngine{providers: providers()}
	rel := relationship("scope", true, false)
	cp := rel.Continuity[0]
	cp.TransactionClass = "unsupported"
	if got := e.check(&rel, cp, "general:a", "identity:b", 5); got.Viable || got.Reason != PolicyNoLongerCompatible {
		t.Fatalf("unsupported transaction class passed: %+v", got)
	}
	cp.TransactionClass = "subscription"
	cp.MaximumAmountBand = 100001
	if got := e.check(&rel, cp, "general:a", "identity:b", 5); got.Viable || got.Reason != PolicyNoLongerCompatible {
		t.Fatalf("unsupported amount band passed: %+v", got)
	}
}
