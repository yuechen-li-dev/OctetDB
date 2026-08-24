// Package bsotrustm3 models bounded, versioned financial policy without a
// contract VM. Policy evaluation is pure; only Authority can mutate money.
package bsotrustm3

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Money int64

type PolicyClass string

const (
	TrustPolicy         PolicyClass = "trust"
	AuthorizationPolicy PolicyClass = "authorization"
	TransactionPolicy   PolicyClass = "transaction"
	ApplicationPolicy   PolicyClass = "application"
)

type TransactionClass string

const (
	Subscription        TransactionClass = "subscription"
	MarketplacePurchase TransactionClass = "marketplace_purchase"
	EscrowRelease       TransactionClass = "escrow_release"
	Refund              TransactionClass = "refund"
	HighValueTransfer   TransactionClass = "high_value_transfer"
	DelegatedTransfer   TransactionClass = "delegated_transfer"
)

type EscrowCondition string

const (
	NoEscrowCondition          EscrowCondition = "none"
	DeliveryOrTimeoutNoDispute EscrowCondition = "delivery_or_timeout_no_dispute"
)

type ReasonCode string

const (
	ReasonAllowed           ReasonCode = "allowed"
	AmountExceeded          ReasonCode = "amount_exceeded"
	CumulativeLimitExceeded ReasonCode = "cumulative_limit_exceeded"
	ExpiredAuthorization    ReasonCode = "expired_authorization"
	WrongCounterparty       ReasonCode = "wrong_counterparty"
	WrongTransactionClass   ReasonCode = "wrong_transaction_class"
	WrongSubject            ReasonCode = "wrong_subject"
	WrongDelegate           ReasonCode = "wrong_delegate"
	PolicyVersionMismatch   ReasonCode = "policy_version_mismatch"
	PolicyDisabled          ReasonCode = "policy_disabled"
	MissingTrust            ReasonCode = "missing_trust"
	MissingEscrow           ReasonCode = "missing_escrow"
	StateConflict           ReasonCode = "state_conflict"
	InvalidPolicy           ReasonCode = "invalid_policy"
	IncompatiblePolicies    ReasonCode = "incompatible_policies"
	InsufficientBalance     ReasonCode = "insufficient_balance"
)

type RequiredAction string

const (
	RequireTrustAttestation RequiredAction = "trust_attestation"
	RequireEscrow           RequiredAction = "escrow"
)

type ChangeKind string

const (
	AmountLimitIncreased      ChangeKind = "amount_limit_increased"
	AmountLimitDecreased      ChangeKind = "amount_limit_decreased"
	CumulativeLimitAdded      ChangeKind = "cumulative_limit_added"
	CumulativeLimitRemoved    ChangeKind = "cumulative_limit_removed"
	CumulativeLimitIncreased  ChangeKind = "cumulative_limit_increased"
	CumulativeLimitDecreased  ChangeKind = "cumulative_limit_decreased"
	CounterpartyScopeExpanded ChangeKind = "counterparty_scope_expanded"
	ValidityExtended          ChangeKind = "validity_extended"
	RequiredTrustReduced      ChangeKind = "required_trust_reduced"
)

type PolicyIdentityV1 struct {
	PolicyID      string `json:"policy_id"`
	PolicyVersion int    `json:"policy_version"`
	PolicyDigest  string `json:"policy_digest"`
}

type AuthorizationScopeV1 struct {
	AuthorizationID     string           `json:"authorization_id"`
	Subject             string           `json:"subject"`
	Delegate            string           `json:"delegate"`
	Counterparty        string           `json:"counterparty"`
	TransactionClass    TransactionClass `json:"transaction_class"`
	MaxAmount           Money            `json:"max_amount"`
	MaxCumulativeAmount Money            `json:"max_cumulative_amount"`
	ValidUntil          int              `json:"valid_until"`
}

type PolicyV1 struct {
	Identity                 PolicyIdentityV1     `json:"identity"`
	Class                    PolicyClass          `json:"class"`
	OwnerBSO                 string               `json:"owner_bso"`
	Scope                    AuthorizationScopeV1 `json:"scope"`
	RequiredTrustRoles       []string             `json:"required_trust_roles"`
	EscrowRequiredAbove      Money                `json:"escrow_required_above"`
	EscrowCondition          EscrowCondition      `json:"escrow_condition"`
	IntendedCumulativeAmount Money                `json:"intended_cumulative_amount"`
}

type PolicyFactsV1 struct {
	FactID           string           `json:"fact_id"`
	FactVersion      int              `json:"fact_version"`
	TransferID       string           `json:"transfer_id"`
	Subject          string           `json:"subject"`
	Actor            string           `json:"actor"`
	Counterparty     string           `json:"counterparty"`
	TransactionClass TransactionClass `json:"transaction_class"`
	Amount           Money            `json:"amount"`
	LogicalTime      int              `json:"logical_time"`
	ConsumedAmount   Money            `json:"consumed_amount"`
	ReservedAmount   Money            `json:"reserved_amount"`
	TrustRoles       []string         `json:"trust_roles"`
	EscrowPresent    bool             `json:"escrow_present"`
}

type PolicyDecisionV1 struct {
	Identity        PolicyIdentityV1 `json:"identity"`
	DecisionID      string           `json:"decision_id"`
	FactsDigest     string           `json:"facts_digest"`
	RequestDigest   string           `json:"request_digest"`
	FactID          string           `json:"fact_id"`
	FactVersion     int              `json:"fact_version"`
	Allowed         bool             `json:"allowed"`
	Reason          ReasonCode       `json:"reason"`
	RequiredActions []RequiredAction `json:"required_actions"`
}

type PolicyDiffV1 struct {
	PolicyID          string       `json:"policy_id"`
	OldVersion        int          `json:"old_version"`
	NewVersion        int          `json:"new_version"`
	Changes           []ChangeKind `json:"changes"`
	AuthorityExpanded bool         `json:"authority_expanded"`
}

type PolicyProjectionV1 struct {
	Identity            PolicyIdentityV1 `json:"identity"`
	TransactionClass    TransactionClass `json:"transaction_class"`
	MaxAmount           Money            `json:"max_amount"`
	EscrowRequiredAbove Money            `json:"escrow_required_above"`
	RequiredTrustRoles  []string         `json:"required_trust_roles"`
}

type TransferRequestV1 struct {
	TransferID, OriginalTransferID, Subject, Actor, Counterparty, AuthorizationID string
	Class                                                                         TransactionClass
	Amount                                                                        Money
	LogicalTime                                                                   int
	TrustRoles                                                                    []string
	AttestationIDs                                                                []string
	EscrowPresent                                                                 bool
	Kind                                                                          string
}

func ValidatePolicy(p PolicyV1) error {
	if p.Identity.PolicyID == "" || p.Identity.PolicyVersion < 1 || p.OwnerBSO == "" {
		return errors.New("policy identity, version, and owner are required")
	}
	if p.Class != TrustPolicy && p.Class != AuthorizationPolicy && p.Class != TransactionPolicy && p.Class != ApplicationPolicy {
		return errors.New("unknown policy class")
	}
	if p.Scope.AuthorizationID == "" || p.Scope.Subject == "" || p.Scope.Counterparty == "" || p.Scope.TransactionClass == "" || p.Scope.MaxAmount <= 0 || p.Scope.ValidUntil < 0 {
		return errors.New("invalid authorization scope")
	}
	if p.Scope.MaxCumulativeAmount < 0 || p.EscrowRequiredAbove < 0 || p.IntendedCumulativeAmount < 0 {
		return errors.New("limits must be non-negative")
	}
	if p.EscrowRequiredAbove > 0 && p.EscrowCondition != DeliveryOrTimeoutNoDispute {
		return errors.New("escrow threshold requires a typed release condition")
	}
	digest := DigestPolicy(p)
	if p.Identity.PolicyDigest != "" && p.Identity.PolicyDigest != digest {
		return errors.New("policy digest mismatch")
	}
	return nil
}

// CanonicalPolicyBytes is a data-only, fixed-field Octagon representation.
// It deliberately does not depend on Go map order or JSON stringification.
func CanonicalPolicyBytes(p PolicyV1) []byte {
	roles := append([]string(nil), p.RequiredTrustRoles...)
	sort.Strings(roles)
	quoted := make([]string, len(roles))
	for i := range roles {
		quoted[i] = strconv.Quote(roles[i])
	}
	return []byte(fmt.Sprintf("PolicyV1 { PolicyID: %s PolicyVersion: %d Class: PolicyClass.%s OwnerBSO: %s AuthorizationID: %s Subject: %s Delegate: %s Counterparty: %s TransactionClass: TransactionClass.%s MaxAmount: %d MaxCumulativeAmount: %d ValidUntil: %d RequiredTrustRoles: [%s] EscrowRequiredAbove: %d EscrowCondition: EscrowCondition.%s IntendedCumulativeAmount: %d }\n",
		strconv.Quote(p.Identity.PolicyID), p.Identity.PolicyVersion, enumName(string(p.Class)), strconv.Quote(p.OwnerBSO), strconv.Quote(p.Scope.AuthorizationID), strconv.Quote(p.Scope.Subject), strconv.Quote(p.Scope.Delegate), strconv.Quote(p.Scope.Counterparty), enumName(string(p.Scope.TransactionClass)), p.Scope.MaxAmount, p.Scope.MaxCumulativeAmount, p.Scope.ValidUntil, strings.Join(quoted, ", "), p.EscrowRequiredAbove, enumName(string(p.EscrowCondition)), p.IntendedCumulativeAmount))
}

func DigestPolicy(p PolicyV1) string {
	copy := p
	copy.Identity.PolicyDigest = ""
	sum := sha256.Sum256(CanonicalPolicyBytes(copy))
	return hex.EncodeToString(sum[:])
}

func SealPolicy(p PolicyV1) PolicyV1 {
	p.Identity.PolicyDigest = DigestPolicy(p)
	return p
}

func EvaluatePolicy(p PolicyV1, f PolicyFactsV1) PolicyDecisionV1 {
	d := PolicyDecisionV1{Identity: p.Identity, FactID: f.FactID, FactVersion: f.FactVersion, Reason: ReasonAllowed}
	d.FactsDigest = digestFields(f.FactID, strconv.Itoa(f.FactVersion), f.TransferID, f.Subject, f.Actor, f.Counterparty, string(f.TransactionClass), strconv.FormatInt(int64(f.Amount), 10), strconv.Itoa(f.LogicalTime), strconv.FormatInt(int64(f.ConsumedAmount), 10), strconv.FormatInt(int64(f.ReservedAmount), 10), strings.Join(f.TrustRoles, ","), strconv.FormatBool(f.EscrowPresent))
	d.RequestDigest = requestDigest(f.TransferID, f.Subject, f.Actor, f.Counterparty, f.TransactionClass, f.Amount, f.LogicalTime, f.TrustRoles, f.EscrowPresent)
	d.DecisionID = stableID("decision", p.Identity.PolicyID, strconv.Itoa(p.Identity.PolicyVersion), p.Identity.PolicyDigest, d.FactsDigest)
	d.Allowed = false
	switch {
	case ValidatePolicy(p) != nil:
		d.Reason = InvalidPolicy
	case f.Subject != p.Scope.Subject:
		d.Reason = WrongSubject
	case p.Scope.Delegate != "" && f.Actor != p.Scope.Delegate:
		d.Reason = WrongDelegate
	case f.Counterparty != p.Scope.Counterparty:
		d.Reason = WrongCounterparty
	case f.TransactionClass != p.Scope.TransactionClass:
		d.Reason = WrongTransactionClass
	case f.LogicalTime > p.Scope.ValidUntil:
		d.Reason = ExpiredAuthorization
	case f.Amount <= 0 || f.Amount > p.Scope.MaxAmount:
		d.Reason = AmountExceeded
	case p.Scope.MaxCumulativeAmount > 0 && f.ConsumedAmount+f.ReservedAmount+f.Amount > p.Scope.MaxCumulativeAmount:
		d.Reason = CumulativeLimitExceeded
	case !containsAll(f.TrustRoles, p.RequiredTrustRoles):
		d.Reason = MissingTrust
		d.RequiredActions = []RequiredAction{RequireTrustAttestation}
	case p.EscrowRequiredAbove > 0 && f.Amount > p.EscrowRequiredAbove && !f.EscrowPresent:
		d.Reason = MissingEscrow
		d.RequiredActions = []RequiredAction{RequireEscrow}
	default:
		d.Allowed = true
	}
	return d
}

func requestDigest(transferID, subject, actor, counterparty string, class TransactionClass, amount Money, logicalTime int, trustRoles []string, escrow bool) string {
	return digestFields(transferID, subject, actor, counterparty, string(class), strconv.FormatInt(int64(amount), 10), strconv.Itoa(logicalTime), strings.Join(trustRoles, ","), strconv.FormatBool(escrow))
}

func digestFields(fields ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(fields, "\x00")))
	return hex.EncodeToString(sum[:])
}

func ComposeDecisions(decisions ...PolicyDecisionV1) PolicyDecisionV1 {
	if len(decisions) == 0 {
		return PolicyDecisionV1{Reason: InvalidPolicy}
	}
	composed := decisions[0]
	ids := make([]string, len(decisions))
	for i, d := range decisions {
		ids[i] = d.DecisionID
		if !d.Allowed {
			composed.Allowed = false
			composed.Reason = d.Reason
			composed.RequiredActions = append([]RequiredAction(nil), d.RequiredActions...)
			composed.DecisionID = stableID("composition-deny", ids...)
			return composed
		}
	}
	composed.Allowed = true
	composed.Reason = ReasonAllowed
	composed.DecisionID = stableID("composition-allow", ids...)
	return composed
}

func DiffPolicies(oldPolicy, newPolicy PolicyV1) PolicyDiffV1 {
	d := PolicyDiffV1{PolicyID: oldPolicy.Identity.PolicyID, OldVersion: oldPolicy.Identity.PolicyVersion, NewVersion: newPolicy.Identity.PolicyVersion}
	add := func(kind ChangeKind, expanded bool) {
		d.Changes = append(d.Changes, kind)
		d.AuthorityExpanded = d.AuthorityExpanded || expanded
	}
	if newPolicy.Scope.MaxAmount > oldPolicy.Scope.MaxAmount {
		add(AmountLimitIncreased, true)
	}
	if newPolicy.Scope.MaxAmount < oldPolicy.Scope.MaxAmount {
		add(AmountLimitDecreased, false)
	}
	switch {
	case oldPolicy.Scope.MaxCumulativeAmount == 0 && newPolicy.Scope.MaxCumulativeAmount > 0:
		add(CumulativeLimitAdded, false)
	case oldPolicy.Scope.MaxCumulativeAmount > 0 && newPolicy.Scope.MaxCumulativeAmount == 0:
		add(CumulativeLimitRemoved, true)
	case newPolicy.Scope.MaxCumulativeAmount > oldPolicy.Scope.MaxCumulativeAmount:
		add(CumulativeLimitIncreased, true)
	case newPolicy.Scope.MaxCumulativeAmount < oldPolicy.Scope.MaxCumulativeAmount:
		add(CumulativeLimitDecreased, false)
	}
	if oldPolicy.Scope.Counterparty != newPolicy.Scope.Counterparty {
		add(CounterpartyScopeExpanded, true)
	}
	if newPolicy.Scope.ValidUntil > oldPolicy.Scope.ValidUntil {
		add(ValidityExtended, true)
	}
	if len(newPolicy.RequiredTrustRoles) < len(oldPolicy.RequiredTrustRoles) {
		add(RequiredTrustReduced, true)
	}
	return d
}

func PublishProjection(p PolicyV1) PolicyProjectionV1 {
	return PolicyProjectionV1{Identity: p.Identity, TransactionClass: p.Scope.TransactionClass, MaxAmount: p.Scope.MaxAmount, EscrowRequiredAbove: p.EscrowRequiredAbove, RequiredTrustRoles: append([]string(nil), p.RequiredTrustRoles...)}
}

func containsAll(have, required []string) bool {
	set := map[string]bool{}
	for _, x := range have {
		set[x] = true
	}
	for _, x := range required {
		if !set[x] {
			return false
		}
	}
	return true
}

func stableID(prefix string, fields ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(append([]string{prefix}, fields...), "\x00")))
	return prefix + ":" + hex.EncodeToString(sum[:16])
}

func enumName(s string) string {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' })
	for i := range parts {
		if parts[i] != "" {
			parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
		}
	}
	if len(parts) == 0 {
		return "None"
	}
	return strings.Join(parts, "")
}
