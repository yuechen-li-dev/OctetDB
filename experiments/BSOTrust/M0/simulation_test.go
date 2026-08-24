package bsosim

import (
	"context"
	"reflect"
	"testing"
)

func resolutionByID(t *testing.T, result Result, transferID string) TrustResolutionV1 {
	t.Helper()
	for _, resolution := range result.TrustResolutions {
		if resolution.TransferID == transferID {
			return resolution
		}
	}
	t.Fatalf("resolution %s missing", transferID)
	return TrustResolutionV1{}
}

func TestTrustOctagonDTOsAreTypedDeterministicAndCanonical(t *testing.T) {
	identity := IdentityAttestationV1{ProviderID: "trust:identity:acme", SubjectBSO: "bso:1", IdentityLevel: IdentityVerified, IssuedAt: 1, ValidUntil: 9, PolicyVersion: "1", AttestationID: "att:1"}
	unsigned := EncodeIdentityAttestation(identity)
	identity.Auth = canonicalAuth(identity.ProviderID, unsigned)
	first := EncodeIdentityAttestation(identity)
	second := EncodeIdentityAttestation(identity)
	if string(first) != string(second) || string(first[:21]) != "IdentityAttestationV1" {
		t.Fatal("identity attestation is not deterministic typed Octagon")
	}
	if identity.Auth == canonicalAuth(identity.ProviderID, first) {
		t.Fatal("auth must bind the canonical unsigned DTO, not recursively sign itself")
	}

	risk := EncodeRiskAttestation(RiskAttestationV1{ProviderID: "trust:risk:a", TransferID: "t", Decision: RiskApprove, PolicyVersion: "1", IssuedAt: 1, ValidUntil: 2, AttestationID: "a"})
	authorization := EncodeAuthorizationAttestation(AuthorizationAttestationV1{ProviderID: "trust:authorization:patron", SubjectBSO: "bso:1", TransactionClass: ClassSubscription, MaxAmount: 10, IssuedAt: 1, ValidUntil: 20, PolicyVersion: "1", AttestationID: "a", ApplicationReference: "subscription:x"})
	if len(risk) == 0 || len(authorization) == 0 {
		t.Fatal("typed attestation encoder returned empty bytes")
	}
	if len(EncodeEscrowAttestation(EscrowAttestationV1{ProviderID: "trust:escrow:x", TransferID: "t", HoldAccepted: true, ReleasePolicyID: "release:1", PolicyVersion: "1", AttestationID: "e"})) == 0 || len(EncodeDisputeAttestation(DisputeAttestationV1{ProviderID: "trust:dispute:x", OriginalTransferID: "t", Decision: DisputeNoAction, PolicyVersion: "1", AttestationID: "d"})) == 0 {
		t.Fatal("optional typed attestation encoder returned empty bytes")
	}
	policy := defaultPolicies()[bsoID(0)]
	if len(EncodeTrustPolicy(policy)) == 0 || len(EncodeTrustRule(policy.Rules[0])) == 0 {
		t.Fatal("policy DTO encoding missing")
	}
	if len(EncodeProviderCapabilities(TrustProviderCapabilitiesV1{ProviderID: "p", Roles: []TrustRole{TrustIdentity}, PolicyVersion: 1})) == 0 {
		t.Fatal("capability DTO encoding missing")
	}

	agent := TransactionAgent{ProtocolVersion: 1, TransferID: "t", SenderBSO: "a", ReceiverBSO: "b", Amount: 10, Phase: PhaseTrustCollect, TransactionClass: ClassSubscription, ApplicationReference: "subscription:x", TrustResolutionID: "r", RequiredRoles: []TrustRole{TrustIdentity, TrustRisk}, TrustThresholds: []int{1, 2}, TrustCandidates: []string{"identity|p", "risk|a", "risk|b"}, SelectedProviders: []string{"identity|p"}, CollectedAttestationIDs: []string{"att:1"}, TrustProviderIndex: 1, SenderPolicyVersion: 2, ReceiverPolicyVersion: 3, TrustProvidersConsulted: 1, FreshProviderCalls: 1}
	checkpoint, err := EncodeCheckpoint(agent)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := DecodeCheckpoint(checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(agent, restored) {
		t.Fatalf("trust checkpoint mismatch\n%+v\n%+v", agent, restored)
	}
}

func TestFederatedTrustSuiteSettlesOnlyAdmittedTransfers(t *testing.T) {
	c := DefaultConfig()
	c.DataDir = t.TempDir()
	result, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Correct || !result.Conservation {
		t.Fatalf("suite incorrect: %+v", result)
	}
	if result.Metrics.Successful != 8 || result.Metrics.Rejected != 1 || result.Metrics.SettlementBeforeTrust != 0 {
		t.Fatalf("unexpected outcomes: %+v", result.Metrics)
	}
	if len(result.TrustResolutions) != 9 || result.Metrics.TrustAdmitted != 8 || result.Metrics.TrustRejected != 1 {
		t.Fatalf("trust resolutions incomplete: %+v", result.Metrics)
	}

	direct := resolutionByID(t, result, "trust:direct")
	if !direct.Admitted || len(direct.RequiredRoles) != 0 || direct.ProvidersConsulted != 0 {
		t.Fatalf("direct trust required an intermediary: %+v", direct)
	}
	purchase := resolutionByID(t, result, "trust:purchase")
	if !purchase.Admitted || !reflect.DeepEqual(purchase.RequiredRoles, []TrustRole{TrustIdentity, TrustRisk}) {
		t.Fatalf("purchase trust mismatch: %+v", purchase)
	}
	fallback := resolutionByID(t, result, "trust:fallback")
	if !containsString(fallback.SelectedProviders, "trust:identity:backup") || result.Metrics.FallbackProviderUses < 1 {
		t.Fatalf("fallback not used: %+v", fallback)
	}
	high := resolutionByID(t, result, "trust:high-value")
	if !containsString(high.SelectedProviders, "trust:risk:a") || containsString(high.SelectedProviders, "trust:risk:b") || !containsString(high.SelectedProviders, "trust:risk:c") {
		t.Fatalf("2-of-3 decision counted incorrectly: %+v", high)
	}
	incompatible := resolutionByID(t, result, "trust:incompatible")
	if incompatible.Admitted || incompatible.FailureReason == "" || result.Metrics.PolicyIntersectionFailures != 1 {
		t.Fatalf("incompatible policy admitted: %+v", incompatible)
	}

	metrics := &metricStore{}
	policies := defaultPolicies()
	for _, id := range []string{bsoID(6), bsoID(7)} {
		bso, openErr := openBSO(context.Background(), c.DataDir+"/bso", id, c.InitialBalance, policies[id], metrics)
		if openErr != nil {
			t.Fatal(openErr)
		}
		state, loadErr := bso.load(context.Background())
		_ = bso.close()
		if loadErr != nil {
			t.Fatal(loadErr)
		}
		if _, ok := state.Outgoing["trust:incompatible"]; ok {
			t.Fatal("incompatible transfer created outgoing financial state")
		}
		if _, ok := state.Incoming["trust:incompatible"]; ok {
			t.Fatal("incompatible transfer created incoming financial state")
		}
	}
}

func TestRecurringAuthorizationIsStableAndReused(t *testing.T) {
	result, err := Run(context.Background(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	fresh, reused, calls := 0, 0, 0
	authorizationIDs := map[string]bool{}
	for _, n := range []string{"1", "2", "3"} {
		resolution := resolutionByID(t, result, "trust:subscription:"+n)
		if !resolution.Admitted {
			t.Fatalf("subscription %s rejected", n)
		}
		fresh += resolution.FreshProviderCalls
		reused += resolution.ReusedAttestations
		calls += resolution.ProvidersConsulted
		for i, provider := range resolution.SelectedProviders {
			if provider == "trust:authorization:patron" {
				authorizationIDs[resolution.AttestationIDs[i]] = true
			}
		}
	}
	if calls != 9 || fresh != 5 || reused != 4 {
		t.Fatalf("recurring reuse mismatch: calls=%d fresh=%d reused=%d", calls, fresh, reused)
	}
	if len(authorizationIDs) != 1 {
		t.Fatalf("authorization was not semantically stable: %v", authorizationIDs)
	}
}

func TestThresholdDisagreementAndChallengeAreNotConsensus(t *testing.T) {
	registry := newTrustRegistry(&metricStore{})
	request := TrustRequestV1{Role: TrustRisk, TransferID: "threshold-pass", SenderBSO: "a", ReceiverBSO: "b", Amount: 1000, TransactionClass: ClassInstitutionSettlement, LogicalTime: 1}
	approvals := 0
	for _, id := range []string{"trust:risk:a", "trust:risk:b", "trust:risk:c"} {
		attestation, _, err := registry.issue(id, request)
		if err != nil {
			t.Fatal(err)
		}
		if attestation.approves() {
			approvals++
		}
	}
	if approvals != 2 {
		t.Fatalf("expected exact 2 approvals, got %d", approvals)
	}
	request.TransferID = "threshold-fail"
	approvals = 0
	for _, id := range []string{"trust:risk:a", "trust:risk:b", "trust:risk:c"} {
		attestation, _, err := registry.issue(id, request)
		if err != nil {
			t.Fatal(err)
		}
		if attestation.approves() {
			approvals++
		}
	}
	if approvals != 1 {
		t.Fatalf("threshold failure should have one approval, got %d", approvals)
	}
	request.TransferID = "challenge"
	attestation, _, err := registry.issue("trust:risk:a", request)
	if err != nil {
		t.Fatal(err)
	}
	if attestation.approves() {
		t.Fatal("challenge was treated as approval")
	}
}

func TestExpiryRevocationPolicyVersionAndCapabilityIsolation(t *testing.T) {
	metrics := &metricStore{}
	registry := newTrustRegistry(metrics)
	request := TrustRequestV1{Role: TrustIdentity, TransferID: "t", SubjectBSO: bsoID(0), SenderBSO: bsoID(0), ReceiverBSO: bsoID(1), Amount: 20, TransactionClass: ClassPurchase, LogicalTime: 1}
	old, _, err := registry.issue("trust:identity:acme", request)
	if err != nil {
		t.Fatal(err)
	}
	request.LogicalTime = old.ValidUntil + 1
	refreshed, reused, err := registry.issue("trust:identity:acme", request)
	if err != nil {
		t.Fatal(err)
	}
	if reused || old.ValidUntil >= request.LogicalTime || refreshed.IssuedAt != request.LogicalTime {
		t.Fatal("expired evidence was reused")
	}
	if old.ID == refreshed.ID { t.Fatal("refreshed evidence reused the expired attestation identity") }
	if metrics.snapshot().ExpiredAttestationsRejected != 1 {
		t.Fatal("expiry rejection not measured")
	}

	policies := defaultPolicies()
	sender, receiver := policies[bsoID(0)], policies[bsoID(1)]
	attempt := Attempt{ID: "purchase", From: sender.BSOID, To: receiver.BSOID, Amount: 20, Class: ClassPurchase}
	requirements, failure := resolvePolicyIntersection(sender, receiver, attempt, 1, registry)
	if failure != "" || len(requirements) != 2 {
		t.Fatalf("baseline intersection failed: %s", failure)
	}
	captured := TrustResolutionV1{TransferID: attempt.ID, SenderPolicyVersion: sender.Version, ReceiverPolicyVersion: receiver.Version, Admitted: true}
	sender.Version = 2
	sender.RevokedProviders = append(sender.RevokedProviders, "trust:identity:acme", "trust:identity:backup")
	_, failure = resolvePolicyIntersection(sender, receiver, attempt, 2, registry)
	if failure == "" {
		t.Fatal("revoked providers remained acceptable for a future resolution")
	}
	if captured.SenderPolicyVersion != 1 || !captured.Admitted {
		t.Fatal("policy change retroactively altered captured resolution")
	}

	request.Role = TrustRisk
	if _, _, err = registry.issue("trust:identity:acme", request); err == nil {
		t.Fatal("provider attested an undeclared role")
	}
	providerType := reflect.TypeOf(deterministicProvider{})
	for i := 0; i < providerType.NumField(); i++ {
		name := providerType.Field(i).Name
		if name == "db" || name == "state" || name == "scheduler" {
			t.Fatalf("provider acquired forbidden authority field %s", name)
		}
	}
}

func TestWorkerMigrationPreservesPendingTrustEvidence(t *testing.T) {
	c := DefaultConfig()
	c.KillWorker, c.KillRound = 1, 2
	result, err := Run(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Correct || result.Metrics.AgentsMigrated == 0 || result.Metrics.DuplicateAttestationsSuppressed != 0 || result.Metrics.DoubleDebits != 0 || result.Metrics.DoubleCredits != 0 {
		t.Fatalf("trust-pending migration failed: %+v", result.Metrics)
	}
}

func TestBSOLocallyPersistsFuturePolicyChanges(t *testing.T) {
	ctx := context.Background()
	policy := defaultPolicies()[bsoID(0)]
	root := t.TempDir() + "/bso"
	bso, err := openBSO(ctx, root, policy.BSOID, 100, policy, &metricStore{})
	if err != nil {
		t.Fatal(err)
	}
	updated := policy
	updated.Version = 2
	updated.RevokedProviders = []string{"trust:identity:acme"}
	if err = bso.replaceTrustPolicy(ctx, updated); err != nil {
		t.Fatal(err)
	}
	if err = bso.close(); err != nil {
		t.Fatal(err)
	}
	bso, err = openBSO(ctx, root, policy.BSOID, 100, policy, &metricStore{})
	if err != nil {
		t.Fatal(err)
	}
	persisted, err := bso.trustPolicy(ctx)
	_ = bso.close()
	if err != nil {
		t.Fatal(err)
	}
	if persisted.Version != 2 || !containsString(persisted.RevokedProviders, "trust:identity:acme") {
		t.Fatalf("local policy did not persist: %+v", persisted)
	}
}

func TestProviderRetentionIsRoleScoped(t *testing.T) {
	result, err := Run(context.Background(), DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	for _, retained := range result.ProviderRetention {
		switch retained.Role {
		case TrustIdentity, TrustAuthorization:
			if retained.SubjectBSO == "" || retained.TransferID != "" {
				t.Fatalf("subject attestation over-retained transfer data: %+v", retained)
			}
		case TrustRisk:
			if retained.TransferID == "" || retained.SubjectBSO != "" {
				t.Fatalf("risk attestation over-retained subject data: %+v", retained)
			}
		default:
			t.Fatalf("unexpected retained role: %+v", retained)
		}
	}
}
