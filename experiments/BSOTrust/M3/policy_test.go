package bsotrustm3

import (
	"context"
	"testing"
)

func TestM3RequiredWorkloads(t *testing.T) {
	r, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !r.Correct || !r.Conservation || !r.RuntimeWithinBudget {
		t.Fatalf("M3 failed: %+v", r)
	}
	if r.ConcurrentSucceeded != 10 || r.ConcurrentConsumed != 100 || r.ConcurrentDoubleConsumption != 0 {
		t.Fatalf("concurrent authority escaped bound: %+v", r)
	}
	if r.TOCTOURejected != 1 || r.StaleDecisionRejected != 1 || r.NestedActionRejected != 1 {
		t.Fatalf("state ordering failure: %+v", r)
	}
}

func TestConfusedDeputyCompositionAndContainment(t *testing.T) {
	r, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.ConfusedDeputyRejected != 4 || r.AuthorityAmplifications != 0 {
		t.Fatalf("delegated scope escaped: %+v", r)
	}
	if r.CompositionRejected != 1 || r.CompositionFinancialMutations != 0 {
		t.Fatalf("deny-wins composition failed: %+v", r)
	}
	if r.UnrelatedStateTouched != 0 || r.BlastRadius.UnrelatedBSOsTouched != 0 || r.BlastRadius.UnrelatedBalancesTouched != 0 {
		t.Fatalf("policy bug escaped local relationship: %+v", r.BlastRadius)
	}
}

func TestVersionDisableRecoveryAndMigration(t *testing.T) {
	r, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.BuggyTransfers != 12 || r.FutureAdmissionsAfterDisable != 0 || r.InFlightCompletedAfterDisable != 1 {
		t.Fatalf("disable semantics failed: %+v", r)
	}
	if r.CompensatingTransfers != 1 || r.IncompleteRecoveries == 0 || r.HistoricalBindingsRetained < 13 {
		t.Fatalf("history/recovery semantics failed: %+v", r)
	}
	if r.WideningDetected != 1 || r.UnapprovedWideningActivated != 0 || r.WidenedFutureSettled != 1 {
		t.Fatalf("widening controls failed: %+v", r)
	}
	if r.MigrationDecisionStable != 1 || r.MigrationFinancialEffects != 1 {
		t.Fatalf("migration replay failed: %+v", r)
	}
}

func TestPurePolicyReplayAndTypedEscrowValidation(t *testing.T) {
	p := policy("policy:test", 1, "bso:a", "auth:test", "service:x", "bso:b", Subscription, 10, 100)
	f := PolicyFactsV1{FactID: "facts:1", FactVersion: 1, TransferID: "transfer:1", Subject: "bso:a", Actor: "service:x", Counterparty: "bso:b", TransactionClass: Subscription, Amount: 10, LogicalTime: 1, TrustRoles: []string{"authorization"}}
	a, b := EvaluatePolicy(p, f), EvaluatePolicy(p, f)
	if a.DecisionID != b.DecisionID || a.FactsDigest != b.FactsDigest || a.Allowed != b.Allowed || a.Reason != b.Reason {
		t.Fatalf("policy replay changed: %+v %+v", a, b)
	}
	malformed := p
	malformed.EscrowRequiredAbove = 5
	malformed.EscrowCondition = NoEscrowCondition
	malformed = SealPolicy(malformed)
	if ValidatePolicy(malformed) == nil {
		t.Fatal("malformed escrow condition accepted")
	}
}

func TestDuplicateTransferIdentityCannotBeRebound(t *testing.T) {
	ctx := context.Background()
	payer, err := OpenAuthority(ctx, t.TempDir(), "bso:payer", 100)
	if err != nil {
		t.Fatal(err)
	}
	defer payer.Close()
	payee, err := OpenAuthority(ctx, t.TempDir(), "bso:payee", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer payee.Close()
	p, _, err := install(ctx, payer, policy("policy:dedupe", 1, payer.id, "auth:dedupe", payer.id, payee.id, Subscription, 10, 100), false)
	if err != nil {
		t.Fatal(err)
	}
	r := request("dedupe:1", p, 5, 1)
	if _, _, reason, err := Settle(ctx, payer, payee, p, r); err != nil || reason != ReasonAllowed {
		t.Fatalf("initial settlement failed: %v %s", err, reason)
	}
	before, _ := payer.Load(ctx)
	r.Amount = 6
	if _, _, reason, err := Settle(ctx, payer, payee, p, r); err != nil || reason != StateConflict {
		t.Fatalf("altered duplicate was not rejected: %v %s", err, reason)
	}
	after, _ := payer.Load(ctx)
	if after.Balance != before.Balance || len(after.Audit) != len(before.Audit) {
		t.Fatal("altered duplicate caused a financial effect")
	}
}
