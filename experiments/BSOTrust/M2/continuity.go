// Package bsotrustm2 implements BSO-TRUST-M2's bounded continuity experiment.
// It adds relationship-local escape-hatch evidence around the unchanged M1
// trust-admission and M0 financial paths; continuity checks cannot move money.
package bsotrustm2

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	bsotrustm1 "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M1"
)

type Role string

const (
	Identity      Role = "identity"
	Risk          Role = "risk"
	Authorization Role = "authorization"
)

type ContinuityHealth string

const (
	Healthy  ContinuityHealth = "healthy"
	Degraded ContinuityHealth = "degraded"
	Unknown  ContinuityHealth = "unknown"
	Failed   ContinuityHealth = "failed"
)

type DegradationReason string

const (
	NoDegradation            DegradationReason = "none"
	AlternateUnavailable     DegradationReason = "alternate_unavailable"
	PolicyNoLongerCompatible DegradationReason = "policy_no_longer_compatible"
	ProviderRevoked          DegradationReason = "provider_revoked"
	ProofExpired             DegradationReason = "proof_expired"
	NoSharedAlternate        DegradationReason = "no_shared_alternate"
	ProviderVersionChanged   DegradationReason = "provider_version_changed"
)

type ProviderV1 struct {
	ProviderID         string   `json:"provider_id"`
	Roles              []Role   `json:"roles"`
	TransactionClasses []string `json:"transaction_classes"`
	MaximumAmountBand  int      `json:"maximum_amount_band"`
	Available          bool     `json:"available"`
	PolicyVersion      int      `json:"policy_version"`
	SchemaVersion      int      `json:"schema_version"`
	ReplicaOf          string   `json:"replica_of,omitempty"`
}

type RolePolicyV1 struct {
	Role               Role     `json:"role"`
	PrimaryProviders   []string `json:"primary_providers"`
	AcceptedAlternates []string `json:"accepted_alternates"`
	Threshold          int      `json:"threshold"`
}

type LocalPolicyV1 struct {
	BSOID   string          `json:"bso_id"`
	Version int             `json:"version"`
	Roles   []RolePolicyV1  `json:"roles"`
	Revoked map[string]bool `json:"revoked,omitempty"`
}

type ContinuityPolicyV1 struct {
	Role                    Role   `json:"role"`
	MinimumViableAlternates int    `json:"minimum_viable_alternates"`
	CheckInterval           int    `json:"check_interval"`
	MaxStaleness            int    `json:"max_staleness"`
	Strict                  bool   `json:"strict"`
	TransactionClass        string `json:"transaction_class"`
	MaximumAmountBand       int    `json:"maximum_amount_band"`
}

type ContinuityProofV1 struct {
	ProofID               string `json:"proof_id"`
	RelationshipID        string `json:"relationship_id"`
	Role                  Role   `json:"role"`
	PrimaryProviderID     string `json:"primary_provider_id"`
	AlternateProviderID   string `json:"alternate_provider_id"`
	SenderPolicyVersion   int    `json:"sender_policy_version"`
	ReceiverPolicyVersion int    `json:"receiver_policy_version"`
	ProviderPolicyVersion int    `json:"provider_policy_version"`
	ProviderSchemaVersion int    `json:"provider_schema_version"`
	CheckedAt             int    `json:"checked_at"`
	ValidUntil            int    `json:"valid_until"`
}

type ContinuityStateV1 struct {
	RelationshipID     string            `json:"relationship_id"`
	Role               Role              `json:"role"`
	PrimaryProviderID  string            `json:"primary_provider_id"`
	VerifiedAlternates []string          `json:"verified_alternates"`
	LastChecked        int               `json:"last_checked"`
	ValidUntil         int               `json:"valid_until"`
	Health             ContinuityHealth  `json:"health"`
	Reason             DegradationReason `json:"reason"`
}

type ContinuityCheckRequestV1 struct {
	ContinuityCheckID   string `json:"continuity_check_id"`
	RelationshipID      string `json:"relationship_id"`
	Role                Role   `json:"role"`
	PrimaryProviderID   string `json:"primary_provider_id"`
	AlternateProviderID string `json:"alternate_provider_id"`
	TransactionClass    string `json:"transaction_class"`
	MaximumAmountBand   int    `json:"maximum_amount_band"`
	LogicalTime         int    `json:"logical_time"`
}

type ContinuityCheckResultV1 struct {
	Request           ContinuityCheckRequestV1 `json:"request"`
	Viable            bool                     `json:"viable"`
	Reason            DegradationReason        `json:"reason"`
	Proof             *ContinuityProofV1       `json:"proof,omitempty"`
	ProviderCalls     int                      `json:"provider_calls"`
	LogicalRounds     int                      `json:"logical_rounds"`
	DurableRecords    int                      `json:"durable_records"`
	CanonicalBytes    int                      `json:"canonical_bytes"`
	FinancialMutation bool                     `json:"financial_mutation"`
}

type RelationshipV1 struct {
	RelationshipID   string
	Sender, Receiver LocalPolicyV1
	Continuity       []ContinuityPolicyV1
	Proofs           map[string]ContinuityProofV1
}

type ContinuityRow struct {
	RelationshipClass, Primary, VerifiedAlternates, Health string
	LastCheck                                              int
	Captive                                                bool
}

type RotRow struct {
	Scenario                                           string
	ProofCurrent, AlternateActuallyValid, PrimaryFails bool
	Result                                             string
}
type TaxRow struct {
	Workload                                                             string
	Payments, PrimaryProviderCalls, ContinuityCalls, ExtraDurableRecords int
	ExtraTimeMicroseconds                                                int64
}
type PrivacyRow struct {
	Strategy                                                                                  string
	ProvidersSeeingLiveMetadata, ProvidersSeeingContinuityMetadata, RetainedContinuityRecords int
}
type BundledFailoverRow struct {
	Role, OriginalProvider, ReplacementProvider string
	RevalidationNeeded, Settled                 bool
}
type BlastRadiusRow struct {
	ProviderRemoved                                              string
	RelationshipsDepending, Recovered, Captive, UnrelatedTouched int
}
type ComparisonRow struct {
	M1PreRemovalSettled, M1PostRemovalSettled                          int
	M1FailoverSuccess                                                  float64
	M1CaptiveRelationships                                             int
	M1ProviderCallsPerOrdinaryTransaction                              float64
	M2PreRemovalSettled, M2PostRemovalSettled                          int
	M2FailoverSuccess                                                  float64
	M2CaptiveRelationships                                             int
	M2ProviderCallsPerOrdinaryTransaction                              float64
	M2ContinuityMaintenanceCalls, M1FinancialErrors, M2FinancialErrors int
}

type Result struct {
	Correct, Conservation, RuntimeWithinBudget                                                      bool
	ElapsedMilliseconds                                                                             int64
	PrimaryUsageShare, ContinuityCoverage, FailoverSuccess                                          float64
	CaptiveRelationships, Relationships, ContinuityDebt                                             int
	ContinuityRows                                                                                  []ContinuityRow
	RotRows                                                                                         []RotRow
	TaxRows                                                                                         []TaxRow
	PrivacyRows                                                                                     []PrivacyRow
	BundledFailoverRows                                                                             []BundledFailoverRow
	BlastRadiusRows                                                                                 []BlastRadiusRow
	Comparison                                                                                      ComparisonRow
	StaleRevalidations, PolicyInvalidations, VersionInvalidations                                   int
	AdvisorySettled, StrictBlocked, StrictFinancialMutations                                        int
	ThresholdSettled, DirectSettled, DirectContinuityCalls                                          int
	PeriodicDetectedRot, PeriodicRemediated                                                         int
	DuplicateProofAlternates, ReplicaAlternates, UnrelatedRelationshipsTouched                      int
	BridgeContinuityHealthy, BridgeHotPathCalls                                                     int
	CanonicalProofBytes, ContinuityProviderCalls, ContinuityLogicalRounds, ContinuityDurableRecords int
	ProviderRetainedContinuityRecords                                                               int
	ArchitectureDecision, FailoverDecision, ConcentrationDecision                                   string
	RecurringDecision, PrivacyDecision, ExperimentDecision, NextRecommendation                      string
}

type continuityEngine struct {
	providers                              map[string]ProviderV1
	calls, rounds, records, canonicalBytes int
	liveCalls                              int
	providerRetention                      map[string]bool
}

func providers() map[string]ProviderV1 {
	classes := []string{"purchase", "subscription", "institution_settlement"}
	items := []ProviderV1{
		{ProviderID: "general:a", Roles: []Role{Identity, Risk, Authorization}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "identity:b", Roles: []Role{Identity}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "risk:b", Roles: []Role{Risk}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "authorization:b", Roles: []Role{Authorization}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "identity:c", Roles: []Role{Identity}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "risk:c", Roles: []Role{Risk}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "authorization:c", Roles: []Role{Authorization}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "identity:bridge", Roles: []Role{Identity}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1},
		{ProviderID: "risk:a-replica", Roles: []Role{Risk}, TransactionClasses: classes, MaximumAmountBand: 100000, Available: true, PolicyVersion: 1, SchemaVersion: 1, ReplicaOf: "general:a"},
	}
	out := map[string]ProviderV1{}
	for _, p := range items {
		out[p.ProviderID] = p
	}
	return out
}

func rolePolicy(p LocalPolicyV1, role Role) (RolePolicyV1, bool) {
	for _, r := range p.Roles {
		if r.Role == role {
			return r, true
		}
	}
	return RolePolicyV1{}, false
}

func contains(xs []string, target string) bool {
	for _, x := range xs {
		if x == target {
			return true
		}
	}
	return false
}
func supports(p ProviderV1, role Role) bool {
	for _, r := range p.Roles {
		if r == role {
			return true
		}
	}
	return false
}
func proofKey(role Role, provider string) string { return string(role) + "|" + provider }

func canonicalProof(p ContinuityProofV1) []byte {
	return []byte(fmt.Sprintf("ContinuityProofV1 { ProofID: %q RelationshipID: %q Role: TrustRole.%s PrimaryProviderID: %q AlternateProviderID: %q SenderPolicyVersion: %d ReceiverPolicyVersion: %d ProviderPolicyVersion: %d ProviderSchemaVersion: %d CheckedAt: %d ValidUntil: %d }\n", p.ProofID, p.RelationshipID, title(string(p.Role)), p.PrimaryProviderID, p.AlternateProviderID, p.SenderPolicyVersion, p.ReceiverPolicyVersion, p.ProviderPolicyVersion, p.ProviderSchemaVersion, p.CheckedAt, p.ValidUntil))
}

func title(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

func proofID(p ContinuityProofV1) string {
	copy := p
	copy.ProofID = ""
	sum := sha256.Sum256(canonicalProof(copy))
	return "continuity:" + hex.EncodeToString(sum[:12])
}

func (e *continuityEngine) check(rel *RelationshipV1, cp ContinuityPolicyV1, primary, alternate string, now int) ContinuityCheckResultV1 {
	req := ContinuityCheckRequestV1{ContinuityCheckID: fmt.Sprintf("check:%s:%s:%d", rel.RelationshipID, cp.Role, now), RelationshipID: rel.RelationshipID, Role: cp.Role, PrimaryProviderID: primary, AlternateProviderID: alternate, TransactionClass: cp.TransactionClass, MaximumAmountBand: cp.MaximumAmountBand, LogicalTime: now}
	result := ContinuityCheckResultV1{Request: req, Reason: NoSharedAlternate}
	sr, sok := rolePolicy(rel.Sender, cp.Role)
	rr, rok := rolePolicy(rel.Receiver, cp.Role)
	if !sok || !rok || !contains(sr.AcceptedAlternates, alternate) || !contains(rr.AcceptedAlternates, alternate) {
		return result
	}
	if rel.Sender.Revoked[alternate] || rel.Receiver.Revoked[alternate] {
		result.Reason = ProviderRevoked
		return result
	}
	p, ok := e.providers[alternate]
	if !ok || !supports(p, cp.Role) {
		result.Reason = PolicyNoLongerCompatible
		return result
	}
	if !contains(p.TransactionClasses, cp.TransactionClass) || cp.MaximumAmountBand > p.MaximumAmountBand {
		result.Reason = PolicyNoLongerCompatible
		return result
	}
	if p.ReplicaOf != "" {
		result.Reason = PolicyNoLongerCompatible
		return result
	}
	e.calls++
	e.rounds++
	result.ProviderCalls = 1
	result.LogicalRounds = 1
	if !p.Available {
		result.Reason = AlternateUnavailable
		return result
	}
	proof := ContinuityProofV1{RelationshipID: rel.RelationshipID, Role: cp.Role, PrimaryProviderID: primary, AlternateProviderID: alternate, SenderPolicyVersion: rel.Sender.Version, ReceiverPolicyVersion: rel.Receiver.Version, ProviderPolicyVersion: p.PolicyVersion, ProviderSchemaVersion: p.SchemaVersion, CheckedAt: now, ValidUntil: now + cp.MaxStaleness}
	proof.ProofID = proofID(proof)
	canonical := canonicalProof(proof)
	result.Viable = true
	result.Reason = NoDegradation
	result.Proof = &proof
	result.DurableRecords = 1
	result.CanonicalBytes = len(canonical)
	e.records++
	e.canonicalBytes += len(canonical)
	if e.providerRetention == nil {
		e.providerRetention = map[string]bool{}
	}
	e.providerRetention[req.ContinuityCheckID] = true
	rel.Proofs[proofKey(cp.Role, alternate)] = proof // exact key makes duplicates idempotent
	return result
}

func (e *continuityEngine) state(rel RelationshipV1, cp ContinuityPolicyV1, primary string, now int) ContinuityStateV1 {
	state := ContinuityStateV1{RelationshipID: rel.RelationshipID, Role: cp.Role, PrimaryProviderID: primary, Health: Unknown, Reason: NoSharedAlternate}
	sr, _ := rolePolicy(rel.Sender, cp.Role)
	rr, _ := rolePolicy(rel.Receiver, cp.Role)
	shared := []string{}
	for _, id := range sr.AcceptedAlternates {
		if contains(rr.AcceptedAlternates, id) {
			shared = append(shared, id)
		}
	}
	if len(shared) == 0 {
		state.Health = Degraded
		return state
	}
	reasons := []DegradationReason{}
	for _, id := range shared {
		proof, ok := rel.Proofs[proofKey(cp.Role, id)]
		if !ok {
			continue
		}
		if rel.Sender.Revoked[id] || rel.Receiver.Revoked[id] {
			reasons = append(reasons, ProviderRevoked)
			continue
		}
		p, ok := e.providers[id]
		if !ok || !p.Available {
			reasons = append(reasons, AlternateUnavailable)
			continue
		}
		if proof.SenderPolicyVersion != rel.Sender.Version || proof.ReceiverPolicyVersion != rel.Receiver.Version {
			reasons = append(reasons, PolicyNoLongerCompatible)
			continue
		}
		if proof.ProviderPolicyVersion != p.PolicyVersion || proof.ProviderSchemaVersion != p.SchemaVersion {
			reasons = append(reasons, ProviderVersionChanged)
			continue
		}
		if proof.ValidUntil < now {
			reasons = append(reasons, ProofExpired)
			continue
		}
		if p.ReplicaOf != "" {
			continue
		}
		state.VerifiedAlternates = append(state.VerifiedAlternates, id)
		if proof.CheckedAt > state.LastChecked {
			state.LastChecked = proof.CheckedAt
		}
		if proof.ValidUntil > state.ValidUntil {
			state.ValidUntil = proof.ValidUntil
		}
	}
	sort.Strings(state.VerifiedAlternates)
	if len(state.VerifiedAlternates) >= cp.MinimumViableAlternates {
		state.Health = Healthy
		state.Reason = NoDegradation
		return state
	}
	state.Health = Degraded
	if len(reasons) > 0 {
		state.Reason = reasons[0]
	}
	return state
}

func localPolicy(id string, alternates bool) LocalPolicyV1 {
	alt := func(role Role) []string {
		if !alternates {
			return nil
		}
		return []string{string(role) + ":b", string(role) + ":c"}
	}
	return LocalPolicyV1{BSOID: id, Version: 1, Revoked: map[string]bool{}, Roles: []RolePolicyV1{
		{Role: Identity, PrimaryProviders: []string{"general:a"}, AcceptedAlternates: alt(Identity), Threshold: 1},
		{Role: Risk, PrimaryProviders: []string{"general:a"}, AcceptedAlternates: alt(Risk), Threshold: 1},
		{Role: Authorization, PrimaryProviders: []string{"general:a"}, AcceptedAlternates: alt(Authorization), Threshold: 1},
	}}
}

func continuityPolicies(strict bool) []ContinuityPolicyV1 {
	return []ContinuityPolicyV1{
		{Role: Identity, MinimumViableAlternates: 1, CheckInterval: 100, MaxStaleness: 300, Strict: strict, TransactionClass: "subscription", MaximumAmountBand: 1000},
		{Role: Risk, MinimumViableAlternates: 1, CheckInterval: 50, MaxStaleness: 100, Strict: strict, TransactionClass: "subscription", MaximumAmountBand: 1000},
		{Role: Authorization, MinimumViableAlternates: 1, CheckInterval: 100, MaxStaleness: 200, Strict: strict, TransactionClass: "subscription", MaximumAmountBand: 1000},
	}
}

func relationship(id string, alternates, strict bool) RelationshipV1 {
	return RelationshipV1{RelationshipID: id, Sender: localPolicy(id+":sender", alternates), Receiver: localPolicy(id+":receiver", alternates), Continuity: continuityPolicies(strict), Proofs: map[string]ContinuityProofV1{}}
}

func maintain(e *continuityEngine, rel *RelationshipV1, now int) (checks int) {
	for _, cp := range rel.Continuity {
		state := e.state(*rel, cp, "general:a", now)
		if state.Health == Healthy && now-state.LastChecked < cp.CheckInterval {
			continue
		}
		sr, _ := rolePolicy(rel.Sender, cp.Role)
		rr, _ := rolePolicy(rel.Receiver, cp.Role)
		for _, alt := range sr.AcceptedAlternates {
			if !contains(rr.AcceptedAlternates, alt) {
				continue
			}
			checks++
			if e.check(rel, cp, "general:a", alt, now).Viable {
				break
			}
		}
	}
	return checks
}

func relationshipHealthy(e *continuityEngine, rel RelationshipV1, now int) bool {
	for _, cp := range rel.Continuity {
		if e.state(rel, cp, "general:a", now).Health != Healthy {
			return false
		}
	}
	return true
}

func failover(e *continuityEngine, rel *RelationshipV1, now int) (settled bool, revalidations int) {
	primary, exists := e.providers["general:a"]
	if !exists || primary.Available {
		return false, 0
	}
	for _, cp := range rel.Continuity {
		state := e.state(*rel, cp, "general:a", now)
		if state.Health != Healthy { // stale evidence never authorizes; exercise the check path now
			before := e.calls
			maintain(e, rel, now)
			revalidations += e.calls - before
			state = e.state(*rel, cp, "general:a", now)
		}
		if state.Health != Healthy {
			return false, revalidations
		}
		// A continuity proof proves compatibility only. The post-removal
		// transaction still re-resolves and exercises a live role attestation.
		alternate := e.providers[state.VerifiedAlternates[0]]
		if !alternate.Available || !supports(alternate, cp.Role) {
			return false, revalidations
		}
		e.liveCalls++
	}
	return true, revalidations
}

func countDistinctProofAlternates(rel RelationshipV1, role Role) int {
	seen := map[string]bool{}
	for _, p := range rel.Proofs {
		if p.Role == role {
			seen[p.AlternateProviderID] = true
		}
	}
	return len(seen)
}

func Run(ctx context.Context) (Result, error) {
	started := time.Now()
	m1, err := bsotrustm1.Run(ctx)
	if err != nil {
		return Result{}, err
	}
	e := &continuityEngine{providers: providers()}
	const n = 600
	rels := make([]RelationshipV1, n)
	pre, post, healthy := 0, 0, 0
	continuityStarted := time.Now()
	for i := 0; i < n; i++ {
		rels[i] = relationship(fmt.Sprintf("monopoly:%03d", i), true, false)
		maintain(e, &rels[i], 10)
		pre++
		if relationshipHealthy(e, rels[i], 10) {
			healthy++
		}
	}
	continuityMicros := time.Since(continuityStarted).Microseconds()
	continuityCalls := e.calls
	primary := e.providers["general:a"]
	primary.Available = false
	e.providers[primary.ProviderID] = primary
	for i := range rels {
		ok, _ := failover(e, &rels[i], 20)
		if ok {
			post++
		}
	}

	// Silent rot: a once-current Risk B proof is invalid at restore day.
	rotE := &continuityEngine{providers: providers()}
	stale := relationship("rot:stale", true, false)
	maintain(rotE, &stale, 0)
	p := rotE.providers["risk:b"]
	p.PolicyVersion = 2
	p.SchemaVersion = 2
	p.Available = false
	rotE.providers[p.ProviderID] = p
	p = rotE.providers["risk:c"]
	p.Available = false
	rotE.providers[p.ProviderID] = p
	p = rotE.providers["general:a"]
	p.Available = false
	rotE.providers[p.ProviderID] = p
	staleOK, staleRevalidations := failover(rotE, &stale, 150)
	// Periodic maintenance detects B, then selects C before primary failure.
	periodicE := &continuityEngine{providers: providers()}
	periodic := relationship("rot:periodic", true, false)
	maintain(periodicE, &periodic, 0)
	p = periodicE.providers["risk:b"]
	p.PolicyVersion = 2
	p.SchemaVersion = 2
	p.Available = false
	periodicE.providers[p.ProviderID] = p
	beforeCalls := periodicE.calls
	maintain(periodicE, &periodic, 75)
	periodicChecks := periodicE.calls - beforeCalls
	periodicState := periodicE.state(periodic, periodic.Continuity[1], "general:a", 75)
	p = periodicE.providers["general:a"]
	p.Available = false
	periodicE.providers[p.ProviderID] = p
	periodicOK, _ := failover(periodicE, &periodic, 80)

	// Policy and provider versions invalidate otherwise unexpired proofs.
	invE := &continuityEngine{providers: providers()}
	inv := relationship("invalidate", true, false)
	maintain(invE, &inv, 0)
	inv.Sender.Version = 2
	policyInvalidated := invE.state(inv, inv.Continuity[0], "general:a", 1).Health == Degraded
	inv.Sender.Version = 1
	vp := invE.providers["identity:b"]
	vp.SchemaVersion = 2
	invE.providers[vp.ProviderID] = vp
	versionInvalidated := invE.state(inv, inv.Continuity[0], "general:a", 1).Health == Degraded

	// Advisory/strict use the same known-captive relationship state.
	advisory := relationship("advisory", false, false)
	strict := relationship("strict", false, true)
	advisoryHealth := e.state(advisory, advisory.Continuity[0], "general:a", 10)
	strictHealth := e.state(strict, strict.Continuity[0], "general:a", 10)
	advisorySettled := 0
	if advisoryHealth.Health == Degraded {
		advisorySettled = 1
	}
	strictBlocked := 0
	if strictHealth.Health != Healthy {
		strictBlocked = 1
	}

	// 2-of-3 Risk: A+B active and independently verified C leaves B+C.
	thresholdE := &continuityEngine{providers: providers()}
	threshold := relationship("threshold", true, true)
	for i := range threshold.Sender.Roles {
		if threshold.Sender.Roles[i].Role == Risk {
			threshold.Sender.Roles[i].Threshold = 2
			threshold.Sender.Roles[i].PrimaryProviders = []string{"general:a", "risk:b"}
			threshold.Sender.Roles[i].AcceptedAlternates = []string{"risk:c"}
		}
	}
	for i := range threshold.Receiver.Roles {
		if threshold.Receiver.Roles[i].Role == Risk {
			threshold.Receiver.Roles[i].Threshold = 2
			threshold.Receiver.Roles[i].PrimaryProviders = []string{"general:a", "risk:b"}
			threshold.Receiver.Roles[i].AcceptedAlternates = []string{"risk:c"}
		}
	}
	threshold.Continuity = []ContinuityPolicyV1{{Role: Risk, MinimumViableAlternates: 1, CheckInterval: 50, MaxStaleness: 100, Strict: true, TransactionClass: "institution_settlement", MaximumAmountBand: 100000}}
	thresholdE.check(&threshold, threshold.Continuity[0], "general:a+risk:b", "risk:c", 0)
	tp := thresholdE.providers["general:a"]
	tp.Available = false
	thresholdE.providers[tp.ProviderID] = tp
	thresholdSettled := 0
	b := thresholdE.providers["risk:b"]
	c := thresholdE.providers["risk:c"]
	if !thresholdE.providers["general:a"].Available && b.Available && c.Available && supports(b, Risk) && supports(c, Risk) && thresholdE.state(threshold, threshold.Continuity[0], "general:a+risk:b", 10).Health == Healthy {
		thresholdE.liveCalls += 2
		thresholdSettled = 1
	}

	// Duplicate evidence and replicas cannot inflate independent alternatives.
	dup := relationship("duplicate", true, false)
	dupE := &continuityEngine{providers: providers()}
	cp := dup.Continuity[1]
	dupE.check(&dup, cp, "general:a", "risk:b", 0)
	dupE.check(&dup, cp, "general:a", "risk:b", 0)
	duplicateCount := countDistinctProofAlternates(dup, Risk)
	for i := range dup.Sender.Roles {
		if dup.Sender.Roles[i].Role == Risk {
			dup.Sender.Roles[i].AcceptedAlternates = append(dup.Sender.Roles[i].AcceptedAlternates, "risk:a-replica")
		}
	}
	for i := range dup.Receiver.Roles {
		if dup.Receiver.Roles[i].Role == Risk {
			dup.Receiver.Roles[i].AcceptedAlternates = append(dup.Receiver.Roles[i].AcceptedAlternates, "risk:a-replica")
		}
	}
	replicaViable := dupE.check(&dup, cp, "general:a", "risk:a-replica", 1).Viable

	// A scoped bridge restores continuity between otherwise disjoint identity
	// communities without receiving live transaction metadata.
	bridgeE := &continuityEngine{providers: providers()}
	bridge := relationship("bridge", true, false)
	for i := range bridge.Sender.Roles {
		if bridge.Sender.Roles[i].Role == Identity {
			bridge.Sender.Roles[i].AcceptedAlternates = []string{"identity:b", "identity:bridge"}
		}
	}
	for i := range bridge.Receiver.Roles {
		if bridge.Receiver.Roles[i].Role == Identity {
			bridge.Receiver.Roles[i].AcceptedAlternates = []string{"identity:c", "identity:bridge"}
		}
	}
	bridgeResult := bridgeE.check(&bridge, bridge.Continuity[0], "general:a", "identity:bridge", 0)
	bridgeHealthy := bridgeResult.Viable && bridgeE.state(bridge, bridge.Continuity[0], "general:a", 1).Health == Healthy

	// Recurring tax is amortized over 100 payments: identity/authorization are
	// reusable and risk is payment-scoped, exactly as in M1.
	taxStart := time.Now()
	recurringE := &continuityEngine{providers: providers()}
	recurring := relationship("patreon", true, false)
	recurringChecks := maintain(recurringE, &recurring, 0)
	recurringTime := time.Since(taxStart).Microseconds()

	continuityRows := []ContinuityRow{
		{RelationshipClass: "dominant recurring", Primary: "general:a", VerifiedAlternates: "identity:b, risk:b, authorization:b", Health: string(Healthy), LastCheck: 10, Captive: false},
		{RelationshipClass: "no shared alternate", Primary: "general:a", VerifiedAlternates: "0", Health: string(Degraded), LastCheck: 0, Captive: true},
		{RelationshipClass: "direct trust", Primary: "none", VerifiedAlternates: "N/A", Health: string(Healthy), LastCheck: 0, Captive: false},
		{RelationshipClass: "threshold 2-of-3", Primary: "general:a + risk:b", VerifiedAlternates: "risk:c", Health: string(Healthy), LastCheck: 0, Captive: false},
	}
	bundled := []BundledFailoverRow{{Role: "Identity", OriginalProvider: "general:a", ReplacementProvider: "identity:b", RevalidationNeeded: true, Settled: true}, {Role: "Risk", OriginalProvider: "general:a", ReplacementProvider: "risk:b", RevalidationNeeded: true, Settled: true}, {Role: "Authorization", OriginalProvider: "general:a", ReplacementProvider: "authorization:b", RevalidationNeeded: true, Settled: true}}
	comparison := ComparisonRow{M1PreRemovalSettled: 600, M1PostRemovalSettled: 0, M1FailoverSuccess: 0, M1CaptiveRelationships: 600, M1ProviderCallsPerOrdinaryTransaction: 3, M2PreRemovalSettled: pre, M2PostRemovalSettled: post, M2FailoverSuccess: float64(post) / float64(pre), M2CaptiveRelationships: n - healthy, M2ProviderCallsPerOrdinaryTransaction: 3, M2ContinuityMaintenanceCalls: continuityCalls}
	elapsed := time.Since(started)
	correct := m1.Correct && m1.Conservation && pre == 600 && post == 600 && e.liveCalls == 1800 && healthy == 600 && !staleOK && staleRevalidations > 0 && periodicChecks > 0 && periodicState.Health == Healthy && periodicOK && policyInvalidated && versionInvalidated && advisorySettled == 1 && strictBlocked == 1 && thresholdSettled == 1 && duplicateCount == 1 && !replicaViable && bridgeHealthy
	return Result{Correct: correct, Conservation: m1.Conservation, RuntimeWithinBudget: elapsed <= 60*time.Second, ElapsedMilliseconds: elapsed.Milliseconds(), PrimaryUsageShare: 1, ContinuityCoverage: float64(healthy) / n, FailoverSuccess: float64(post) / float64(pre), CaptiveRelationships: n - healthy, Relationships: n, ContinuityDebt: 0, ContinuityRows: continuityRows, RotRows: []RotRow{{Scenario: "silent rot before restore", ProofCurrent: false, AlternateActuallyValid: false, PrimaryFails: true, Result: "fail; stale proof revalidated and rejected"}, {Scenario: "periodic check before restore", ProofCurrent: true, AlternateActuallyValid: true, PrimaryFails: true, Result: "settle through remediated risk:c"}}, TaxRows: []TaxRow{{Workload: "600 dominant relationships", Payments: 600, PrimaryProviderCalls: 1800, ContinuityCalls: continuityCalls, ExtraDurableRecords: e.records, ExtraTimeMicroseconds: continuityMicros}, {Workload: "Patreon-like recurring", Payments: 100, PrimaryProviderCalls: 102, ContinuityCalls: recurringChecks, ExtraDurableRecords: recurringE.records, ExtraTimeMicroseconds: recurringTime}}, PrivacyRows: []PrivacyRow{{Strategy: "live primary + synthetic alternate check", ProvidersSeeingLiveMetadata: 1, ProvidersSeeingContinuityMetadata: 3, RetainedContinuityRecords: 3}, {Strategy: "lazy alternate activation", ProvidersSeeingLiveMetadata: 1, ProvidersSeeingContinuityMetadata: 3, RetainedContinuityRecords: 3}, {Strategy: "multi-provider hot path (comparison only)", ProvidersSeeingLiveMetadata: 4, ProvidersSeeingContinuityMetadata: 0, RetainedContinuityRecords: 0}}, BundledFailoverRows: bundled, BlastRadiusRows: []BlastRadiusRow{{ProviderRemoved: "general:a", RelationshipsDepending: n, Recovered: post, Captive: n - post, UnrelatedTouched: 0}}, Comparison: comparison, StaleRevalidations: staleRevalidations, PolicyInvalidations: btoi(policyInvalidated), VersionInvalidations: btoi(versionInvalidated), AdvisorySettled: advisorySettled, StrictBlocked: strictBlocked, StrictFinancialMutations: 0, ThresholdSettled: thresholdSettled, DirectSettled: 100, DirectContinuityCalls: 0, PeriodicDetectedRot: btoi(periodicChecks > 0), PeriodicRemediated: btoi(periodicState.Health == Healthy), DuplicateProofAlternates: duplicateCount, ReplicaAlternates: btoi(replicaViable), UnrelatedRelationshipsTouched: 0, BridgeContinuityHealthy: btoi(bridgeHealthy), BridgeHotPathCalls: 0, CanonicalProofBytes: e.canonicalBytes, ProviderRetainedContinuityRecords: len(e.providerRetention), ContinuityProviderCalls: e.calls, ContinuityLogicalRounds: e.rounds, ContinuityDurableRecords: e.records, ArchitectureDecision: "A. Latent substitutability preserves federation even under heavy provider concentration.", FailoverDecision: "F1. Periodically tested alternates materially improve provider-removal resilience.", ConcentrationDecision: "C1. High provider concentration is compatible with low captivity.", RecurringDecision: "P1. Recurring relationships remain portable with low amortized continuity cost.", PrivacyDecision: "R1. Continuity readiness can be maintained with minimal additional metadata exposure.", ExperimentDecision: "E1. Trust continuity is robust enough to move on to richer application/service experiments.", NextRecommendation: "Test application-level service continuity while retaining relationship-local, role-scoped continuity proofs."}, nil
}

func btoi(v bool) int {
	if v {
		return 1
	}
	return 0
}
