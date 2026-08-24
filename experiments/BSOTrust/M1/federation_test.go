package bsotrustm1

import (
	"context"
	"reflect"
	"testing"
)

func TestFederationSuite(t *testing.T) {
	result, err := Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.Correct || !result.Conservation || !result.RuntimeWithinBudget {
		t.Fatalf("incorrect result: %+v", result)
	}
	if result.DirectProviderCalls != 0 || result.UnrelatedTransactionsAffected != 0 || result.PolicyLocalityTouches != 0 {
		t.Fatalf("locality/direct invariant failed: %+v", result)
	}
	if result.CapacityFallbacks == 0 {
		t.Fatal("provider saturation did not exercise fallback")
	}
	if len(result.Outages) != 5 || result.Outages[0].PostRemovalSuccess == 0 || result.Outages[0].Fallbacks == 0 {
		t.Fatalf("outage fallback failed: %+v", result.Outages)
	}
	monopoly := result.Outages[len(result.Outages)-1]
	if monopoly.Resilience >= 0.2 {
		t.Fatalf("de facto mandatory criterion was not exercised: %+v", monopoly)
	}
	if len(result.Compatibility) != 2 || result.Compatibility[0].Compatible != 0 || result.Compatibility[1].Compatible != 200 {
		t.Fatalf("island/bridge result mismatch: %+v", result.Compatibility)
	}
}

func TestRecurringCacheIsExactAndPortable(t *testing.T) {
	rows, portable := runRecurring()
	if !portable || len(rows) != 5 {
		t.Fatalf("recurrence not portable: %+v", rows)
	}
	if rows[0].FreshProviderCalls != 3 || rows[0].CachedAttestations != 0 {
		t.Fatalf("first payment did not establish evidence: %+v", rows[0])
	}
	if rows[1].FreshProviderCalls != 1 || rows[1].CachedAttestations != 2 {
		t.Fatalf("evidence was not amortized: %+v", rows[1])
	}
	if rows[3].ProviderChanges != 1 || !rows[3].Settled {
		t.Fatalf("provider replacement interrupted relationship: %+v", rows[3])
	}
}

func TestSelectionIsDeterministicAndHardPolicyWins(t *testing.T) {
	ps := profiles()
	policies := populationPolicies("preferred")
	props := proposals("determinism", 100, []Role{Identity, Risk, Authorization})
	_, first, _ := runPopulation("first", ps, policies, props)
	_, second, _ := runPopulation("second", ps, policies, props)
	if !reflect.DeepEqual(first, second) {
		t.Fatal("provider selection was not deterministic")
	}
	sender, receiver := policies["bso:000"], policies["bso:011"]
	sender.Revoked["identity:a"] = true
	e := newEngine(ps)
	e.resetRound()
	r := e.resolve(ProposalV1{TransferID: "revoked", Sender: sender.BSOID, Receiver: receiver.BSOID, Amount: 20, LogicalRound: 1, Roles: []Role{Identity}}, sender, receiver)
	if !r.Admitted || len(r.Selections) != 1 || r.Selections[0].ProviderID == "identity:a" {
		t.Fatalf("preference overrode revocation: %+v", r)
	}
}

func TestBundledProviderCannotBypassSeparation(t *testing.T) {
	e := newEngine(profiles())
	sender := policy("a", 0, true, true)
	receiver := policy("b", 0, true, true)
	e.resetRound()
	r := e.resolve(ProposalV1{TransferID: "high", Sender: "a", Receiver: "b", Amount: 1000, LogicalRound: 1, Roles: []Role{Identity, Risk, Authorization}}, sender, receiver)
	if !r.Admitted {
		t.Fatalf("separated transaction rejected: %+v", r)
	}
	seen := map[string]bool{}
	for _, s := range r.Selections {
		if seen[s.ProviderID] {
			t.Fatalf("provider reused across separated roles: %+v", r)
		}
		seen[s.ProviderID] = true
	}
}

func TestRecoveredProviderIsUsedOnlyByNewTransactions(t *testing.T) {
	e := newEngine(profiles())
	sender, receiver := policy("a", 0, false, false), policy("b", 0, false, false)
	p := e.providers["identity:a"]
	p.Available = false
	e.providers[p.ProviderID] = p
	e.resetRound()
	first := e.resolve(ProposalV1{TransferID: "during-outage", Sender: "a", Receiver: "b", Amount: 20, LogicalRound: 1, Roles: []Role{Identity}}, sender, receiver)
	if !first.Admitted || first.Selections[0].ProviderID == "identity:a" {
		t.Fatalf("outage did not fallback: %+v", first)
	}
	p.Available = true
	e.providers[p.ProviderID] = p
	e.resetRound()
	second := e.resolve(ProposalV1{TransferID: "after-recovery", Sender: "a", Receiver: "b", Amount: 20, LogicalRound: 2, Roles: []Role{Identity}}, sender, receiver)
	if !second.Admitted || second.Selections[0].ProviderID != "identity:a" {
		t.Fatalf("recovered provider was not selected: %+v", second)
	}
	if first.Selections[0].ProviderID == second.Selections[0].ProviderID {
		t.Fatal("historical selection changed instead of remaining fallback provenance")
	}
}
