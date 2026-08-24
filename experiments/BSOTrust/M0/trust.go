package bsosim

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// TrustRole and TransactionClass are deliberately closed vocabularies. A
// ProviderID is semantic identity and is never a network or scheduler address.
type TrustRole string

const (
	TrustIdentity      TrustRole = "identity"
	TrustRisk          TrustRole = "risk"
	TrustAuthorization TrustRole = "authorization"
	TrustEscrow        TrustRole = "escrow"
	TrustDispute       TrustRole = "dispute"
)

type TransactionClass string

const (
	ClassDirect                TransactionClass = "direct"
	ClassPurchase              TransactionClass = "purchase"
	ClassSubscription          TransactionClass = "subscription"
	ClassDonation              TransactionClass = "donation"
	ClassMarketplace           TransactionClass = "marketplace"
	ClassInstitutionSettlement TransactionClass = "institution_settlement"
)

type IdentityLevel string

const (
	IdentityKnown         IdentityLevel = "known"
	IdentityVerified      IdentityLevel = "verified"
	IdentityInstitutional IdentityLevel = "institutional"
)

type RiskDecision string

const (
	RiskApprove   RiskDecision = "approve"
	RiskReject    RiskDecision = "reject"
	RiskChallenge RiskDecision = "challenge"
)

type TrustRuleV1 struct {
	Role              TrustRole
	AcceptedProviders []string
	Threshold         int
	MaxAmount         Money
	TransactionClass  TransactionClass
	ValidUntil        int
}

type TrustPolicyV1 struct {
	BSOID            string
	Version          int
	DirectLimit      Money
	ValidUntil       int
	Rules            []TrustRuleV1
	RevokedProviders []string
}

type TrustProviderCapabilitiesV1 struct {
	ProviderID    string
	Roles         []TrustRole
	PolicyVersion int
}

type IdentityAttestationV1 struct {
	ProviderID, SubjectBSO, PolicyVersion, AttestationID, Auth string
	IdentityLevel                                              IdentityLevel
	IssuedAt, ValidUntil                                       int
}

type RiskAttestationV1 struct {
	ProviderID, TransferID, PolicyVersion, AttestationID, ReasonCode, Auth string
	Decision                                                               RiskDecision
	IssuedAt, ValidUntil                                                   int
}

type AuthorizationAttestationV1 struct {
	ProviderID, SubjectBSO, PolicyVersion, AttestationID, ApplicationReference, Auth string
	TransactionClass                                                                 TransactionClass
	MaxAmount                                                                        Money
	IssuedAt, ValidUntil                                                             int
}

// M0 keeps escrow and dispute semantic: neither DTO grants financial custody
// nor mutates transfer history.
type EscrowAttestationV1 struct {
	ProviderID, TransferID, PolicyVersion, AttestationID, ReleasePolicyID, Auth string
	HoldAccepted                                                                bool
	IssuedAt, ValidUntil                                                        int
}

type DisputeDecision string

const (
	DisputeNoAction       DisputeDecision = "no_action"
	DisputeRefundApproved DisputeDecision = "refund_approved"
	DisputeRefundRejected DisputeDecision = "refund_rejected"
)

type DisputeAttestationV1 struct {
	ProviderID, OriginalTransferID, PolicyVersion, AttestationID, Auth string
	Decision                                                           DisputeDecision
	IssuedAt, ValidUntil                                               int
}

type TrustResolutionV1 struct {
	ResolutionID, TransferID, FailureReason                    string
	RequiredRoles                                              []TrustRole
	SelectedProviders, AttestationIDs                          []string
	Admitted                                                   bool
	SenderPolicyVersion, ReceiverPolicyVersion, IssuedAt       int
	ProvidersConsulted, FreshProviderCalls, ReusedAttestations int
}

// TrustRequestV1 is the whole provider-facing surface. It contains transaction
// metadata and an opaque application reference, never application content,
// BSO history, a database transaction, or a balance-mutating capability.
type TrustRequestV1 struct {
	Role                                TrustRole
	TransferID, SenderBSO, ReceiverBSO  string
	SubjectBSO, ApplicationReference    string
	Amount                              Money
	TransactionClass                    TransactionClass
	LogicalTime, RequestedPolicyVersion int
}

type ProviderRetentionV1 struct {
	AttestationID, ProviderID, TransferID, SubjectBSO string
	Role                                              TrustRole
	Decision, PolicyVersion, ReasonCode               string
	IssuedAt, ValidUntil                              int
}

type issuedAttestation struct {
	ID, ProviderID string
	Role           TrustRole
	Decision       string
	IssuedAt       int
	ValidUntil     int
	Canonical      []byte
}

func (a issuedAttestation) approves() bool {
	switch a.Role {
	case TrustRisk:
		return a.Decision == string(RiskApprove)
	case TrustEscrow:
		return a.Decision == "accepted"
	case TrustDispute:
		return a.Decision == string(DisputeNoAction) || a.Decision == string(DisputeRefundApproved)
	default:
		return a.Decision == "issued"
	}
}

type deterministicProvider struct {
	Capabilities   TrustProviderCapabilitiesV1
	UnavailableFor map[TransactionClass]bool
	RiskDefault    RiskDecision
	RiskOverrides  map[string]RiskDecision
	cache          map[string]issuedAttestation
	retention      map[string]ProviderRetentionV1
}

// TrustRegistry owns only provider capabilities, availability, idempotent
// attestations, and role-scoped retention. It has no BSO or scheduler handle.
type TrustRegistry struct {
	mu        sync.Mutex
	providers map[string]*deterministicProvider
	metrics   *metricStore
}

func newTrustRegistry(metrics *metricStore) *TrustRegistry {
	provider := func(id string, roles ...TrustRole) *deterministicProvider {
		return &deterministicProvider{Capabilities: TrustProviderCapabilitiesV1{ProviderID: id, Roles: roles, PolicyVersion: 1}, UnavailableFor: map[TransactionClass]bool{}, RiskDefault: RiskApprove, RiskOverrides: map[string]RiskDecision{}, cache: map[string]issuedAttestation{}, retention: map[string]ProviderRetentionV1{}}
	}
	r := &TrustRegistry{providers: map[string]*deterministicProvider{}, metrics: metrics}
	for _, p := range []*deterministicProvider{
		provider("trust:identity:acme", TrustIdentity),
		provider("trust:identity:backup", TrustIdentity),
		provider("trust:identity:other", TrustIdentity),
		provider("trust:risk:safepay", TrustRisk),
		provider("trust:risk:a", TrustRisk),
		provider("trust:risk:b", TrustRisk),
		provider("trust:risk:c", TrustRisk),
		provider("trust:authorization:patron", TrustAuthorization),
		provider("trust:authorization:institution", TrustAuthorization),
		provider("trust:dispute:marketplace", TrustDispute),
	} {
		r.providers[p.Capabilities.ProviderID] = p
	}
	r.providers["trust:identity:acme"].UnavailableFor[ClassMarketplace] = true
	r.providers["trust:risk:b"].RiskDefault = RiskReject
	return r
}

func (r *TrustRegistry) capabilities(id string) (TrustProviderCapabilitiesV1, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.providers[id]
	if p == nil {
		return TrustProviderCapabilitiesV1{}, false
	}
	return p.Capabilities, true
}

func hasRole(roles []TrustRole, role TrustRole) bool {
	for _, candidate := range roles {
		if candidate == role {
			return true
		}
	}
	return false
}

func (r *TrustRegistry) issue(providerID string, request TrustRequestV1) (issuedAttestation, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.metrics.add(func(m *Metrics) { m.ProvidersConsulted++ })
	p := r.providers[providerID]
	if p == nil {
		return issuedAttestation{}, false, fmt.Errorf("unknown provider %s", providerID)
	}
	if !hasRole(p.Capabilities.Roles, request.Role) {
		return issuedAttestation{}, false, fmt.Errorf("provider %s cannot attest role %s", providerID, request.Role)
	}
	if p.UnavailableFor[request.TransactionClass] {
		return issuedAttestation{}, false, nil
	}
	key := attestationSemanticKey(providerID, request, p.Capabilities.PolicyVersion)
	issuanceKey := key
	if cached, ok := p.cache[key]; ok {
		if cached.ValidUntil >= request.LogicalTime {
			r.metrics.add(func(m *Metrics) { m.ReusedAttestations++ })
			return cached, true, nil
		}
		r.metrics.add(func(m *Metrics) { m.ExpiredAttestationsRejected++ })
		issuanceKey = fmt.Sprintf("%s|refresh:%d", key, request.LogicalTime)
	}
	issued, retention, err := p.create(request, issuanceKey)
	if err != nil {
		return issuedAttestation{}, false, err
	}
	p.cache[key] = issued
	p.retention[issued.ID] = retention
	r.metrics.add(func(m *Metrics) { m.AttestationsIssued++ })
	return issued, false, nil
}

func attestationSemanticKey(providerID string, request TrustRequestV1, policyVersion int) string {
	base := fmt.Sprintf("%s|%s|%s|%d", providerID, request.Role, request.TransactionClass, policyVersion)
	switch request.Role {
	case TrustIdentity:
		return base + "|" + request.SubjectBSO
	case TrustAuthorization:
		return fmt.Sprintf("%s|%s|%s|%d", base, request.SubjectBSO, request.ApplicationReference, request.Amount)
	default:
		return base + "|" + request.TransferID
	}
}

func stableID(prefix, key string) string {
	sum := sha256.Sum256([]byte(key))
	return prefix + ":" + hex.EncodeToString(sum[:12])
}

func canonicalAuth(providerID string, canonical []byte) string {
	sum := sha256.Sum256(append(append([]byte(nil), canonical...), []byte("provider-secret/"+providerID)...))
	return hex.EncodeToString(sum[:])
}

func (p *deterministicProvider) create(q TrustRequestV1, key string) (issuedAttestation, ProviderRetentionV1, error) {
	id := stableID("attestation", key)
	validUntil := q.LogicalTime + 32
	decision, reason := "issued", ""
	var canonical []byte
	switch q.Role {
	case TrustIdentity:
		a := IdentityAttestationV1{ProviderID: p.Capabilities.ProviderID, SubjectBSO: q.SubjectBSO, IdentityLevel: IdentityVerified, IssuedAt: q.LogicalTime, ValidUntil: validUntil, PolicyVersion: "1", AttestationID: id}
		canonical = EncodeIdentityAttestation(a)
		a.Auth = canonicalAuth(a.ProviderID, canonical)
		canonical = EncodeIdentityAttestation(a)
	case TrustRisk:
		d := p.RiskDefault
		if override, ok := p.RiskOverrides[q.TransferID]; ok {
			d = override
		}
		if strings.Contains(q.TransferID, "threshold-fail") && p.Capabilities.ProviderID != "trust:risk:a" {
			d = RiskReject
		}
		if strings.Contains(q.TransferID, "challenge") {
			d = RiskChallenge
		}
		decision = string(d)
		if d != RiskApprove {
			reason = "deterministic_" + string(d)
		}
		a := RiskAttestationV1{ProviderID: p.Capabilities.ProviderID, TransferID: q.TransferID, Decision: d, PolicyVersion: "1", IssuedAt: q.LogicalTime, ValidUntil: validUntil, ReasonCode: reason, AttestationID: id}
		canonical = EncodeRiskAttestation(a)
		a.Auth = canonicalAuth(a.ProviderID, canonical)
		canonical = EncodeRiskAttestation(a)
	case TrustAuthorization:
		validUntil = q.LogicalTime + 96
		a := AuthorizationAttestationV1{ProviderID: p.Capabilities.ProviderID, SubjectBSO: q.SubjectBSO, TransactionClass: q.TransactionClass, MaxAmount: q.Amount, IssuedAt: q.LogicalTime, ValidUntil: validUntil, PolicyVersion: "1", AttestationID: id, ApplicationReference: q.ApplicationReference}
		canonical = EncodeAuthorizationAttestation(a)
		a.Auth = canonicalAuth(a.ProviderID, canonical)
		canonical = EncodeAuthorizationAttestation(a)
	default:
		return issuedAttestation{}, ProviderRetentionV1{}, fmt.Errorf("role %s issuance is not enabled in M0 workloads", q.Role)
	}
	issued := issuedAttestation{ID: id, ProviderID: p.Capabilities.ProviderID, Role: q.Role, Decision: decision, IssuedAt: q.LogicalTime, ValidUntil: validUntil, Canonical: canonical}
	retention := ProviderRetentionV1{AttestationID: id, ProviderID: p.Capabilities.ProviderID, Role: q.Role, Decision: decision, PolicyVersion: "1", ReasonCode: reason, IssuedAt: q.LogicalTime, ValidUntil: validUntil}
	switch q.Role {
	case TrustIdentity, TrustAuthorization:
		retention.SubjectBSO = q.SubjectBSO
	case TrustRisk:
		retention.TransferID = q.TransferID
	}
	return issued, retention, nil
}

func (r *TrustRegistry) retained() []ProviderRetentionV1 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []ProviderRetentionV1
	for _, provider := range r.providers {
		for _, item := range provider.retention {
			out = append(out, item)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AttestationID < out[j].AttestationID })
	return out
}

type roleRequirement struct {
	Role       TrustRole
	Threshold  int
	Candidates []string
}

func policyRule(policy TrustPolicyV1, role TrustRole, attempt Attempt, now int) (TrustRuleV1, bool) {
	for _, rule := range policy.Rules {
		if rule.Role == role && rule.TransactionClass == attempt.Class && rule.ValidUntil >= now && attempt.Amount <= rule.MaxAmount {
			return rule, true
		}
	}
	return TrustRuleV1{}, false
}

func revoked(policy TrustPolicyV1, providerID string) bool {
	for _, id := range policy.RevokedProviders {
		if id == providerID {
			return true
		}
	}
	return false
}

func intersectProviders(left, right []string, sender, receiver TrustPolicyV1, registry *TrustRegistry, role TrustRole) []string {
	rightSet := map[string]bool{}
	for _, id := range right {
		rightSet[id] = true
	}
	var out []string
	for _, id := range left {
		if !rightSet[id] || revoked(sender, id) || revoked(receiver, id) {
			continue
		}
		capabilities, ok := registry.capabilities(id)
		if ok && hasRole(capabilities.Roles, role) {
			out = append(out, id)
		}
	}
	return out
}

func resolvePolicyIntersection(sender, receiver TrustPolicyV1, attempt Attempt, now int, registry *TrustRegistry) ([]roleRequirement, string) {
	if sender.ValidUntil < now || receiver.ValidUntil < now {
		return nil, "policy expired"
	}
	if attempt.Class == ClassDirect && attempt.Amount <= sender.DirectLimit && attempt.Amount <= receiver.DirectLimit {
		return []roleRequirement{}, ""
	}
	var requirements []roleRequirement
	for _, role := range []TrustRole{TrustIdentity, TrustRisk, TrustAuthorization, TrustEscrow, TrustDispute} {
		left, leftOK := policyRule(sender, role, attempt, now)
		right, rightOK := policyRule(receiver, role, attempt, now)
		if !leftOK && !rightOK {
			continue
		}
		if !leftOK || !rightOK {
			return nil, fmt.Sprintf("role %s is not mutually accepted", role)
		}
		threshold := left.Threshold
		if right.Threshold > threshold {
			threshold = right.Threshold
		}
		if threshold < 1 {
			return nil, fmt.Sprintf("role %s has invalid threshold", role)
		}
		providers := intersectProviders(left.AcceptedProviders, right.AcceptedProviders, sender, receiver, registry, role)
		if len(providers) < threshold {
			return nil, fmt.Sprintf("role %s has no sufficient provider intersection", role)
		}
		requirements = append(requirements, roleRequirement{Role: role, Threshold: threshold, Candidates: providers})
	}
	if len(requirements) == 0 {
		return nil, "transaction class has no direct limit or trust rule"
	}
	return requirements, ""
}

func defaultPolicies() map[string]TrustPolicyV1 {
	rule := func(role TrustRole, class TransactionClass, max Money, threshold int, providers ...string) TrustRuleV1 {
		return TrustRuleV1{Role: role, TransactionClass: class, MaxAmount: max, Threshold: threshold, AcceptedProviders: providers, ValidUntil: 10_000}
	}
	common := []TrustRuleV1{
		rule(TrustIdentity, ClassPurchase, 500, 1, "trust:identity:acme", "trust:identity:backup"),
		rule(TrustRisk, ClassPurchase, 500, 1, "trust:risk:safepay"),
		rule(TrustIdentity, ClassSubscription, 50, 1, "trust:identity:acme"),
		rule(TrustRisk, ClassSubscription, 50, 1, "trust:risk:safepay"),
		rule(TrustAuthorization, ClassSubscription, 50, 1, "trust:authorization:patron"),
		rule(TrustIdentity, ClassMarketplace, 500, 1, "trust:identity:acme", "trust:identity:backup"),
		rule(TrustRisk, ClassMarketplace, 500, 1, "trust:risk:safepay"),
		rule(TrustIdentity, ClassInstitutionSettlement, 100_000, 1, "trust:identity:acme"),
		rule(TrustRisk, ClassInstitutionSettlement, 100_000, 2, "trust:risk:a", "trust:risk:b", "trust:risk:c"),
		rule(TrustAuthorization, ClassInstitutionSettlement, 100_000, 1, "trust:authorization:institution"),
	}
	policies := map[string]TrustPolicyV1{}
	for i := 0; i < 8; i++ {
		id := bsoID(i)
		policies[id] = TrustPolicyV1{BSOID: id, Version: 1, DirectLimit: 5, ValidUntil: 10_000, Rules: append([]TrustRuleV1(nil), common...)}
	}
	sender := policies[bsoID(6)]
	sender.Rules = append(sender.Rules, rule(TrustIdentity, ClassDonation, 100, 1, "trust:identity:acme"))
	policies[bsoID(6)] = sender
	receiver := policies[bsoID(7)]
	receiver.Rules = append(receiver.Rules, rule(TrustIdentity, ClassDonation, 100, 1, "trust:identity:other"))
	policies[bsoID(7)] = receiver
	return policies
}

func trustSuiteAttempts() []Attempt {
	return []Attempt{
		{ID: "trust:direct", From: bsoID(0), To: bsoID(1), Amount: 3, Class: ClassDirect, ApplicationReference: "direct:micro"},
		{ID: "trust:purchase", From: bsoID(0), To: bsoID(1), Amount: 20, Class: ClassPurchase, ApplicationReference: "order:123"},
		{ID: "trust:subscription:1", From: bsoID(2), To: bsoID(3), Amount: 10, Class: ClassSubscription, ApplicationReference: "subscription:alice:bob"},
		{ID: "trust:subscription:2", From: bsoID(2), To: bsoID(3), Amount: 10, Class: ClassSubscription, ApplicationReference: "subscription:alice:bob"},
		{ID: "trust:subscription:3", From: bsoID(2), To: bsoID(3), Amount: 10, Class: ClassSubscription, ApplicationReference: "subscription:alice:bob"},
		{ID: "trust:fallback", From: bsoID(4), To: bsoID(5), Amount: 40, Class: ClassMarketplace, ApplicationReference: "order:fallback"},
		{ID: "trust:high-value", From: bsoID(0), To: bsoID(1), Amount: 1_000, Class: ClassInstitutionSettlement, ApplicationReference: "institution:batch:1"},
		{ID: "trust:incompatible", From: bsoID(6), To: bsoID(7), Amount: 10, Class: ClassDonation, ApplicationReference: "donation:1"},
		{ID: "trust:migration", From: bsoID(4), To: bsoID(5), Amount: 25, Class: ClassPurchase, ApplicationReference: "order:migration"},
	}
}

func validatePolicy(policy TrustPolicyV1) error {
	if policy.BSOID == "" || policy.Version < 1 || policy.ValidUntil < 1 || policy.DirectLimit < 0 {
		return errors.New("invalid trust policy")
	}
	for _, rule := range policy.Rules {
		if rule.Threshold < 1 || len(rule.AcceptedProviders) < rule.Threshold || rule.MaxAmount < 0 || rule.ValidUntil < 1 {
			return fmt.Errorf("invalid %s trust rule", rule.Role)
		}
	}
	return nil
}
