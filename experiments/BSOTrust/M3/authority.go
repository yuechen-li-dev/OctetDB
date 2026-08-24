package bsotrustm3

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/yuechen-li-dev/octetdb"
)

const authorityStateKey = "state"

type TransferStatus string

const (
	TransferReserved  TransferStatus = "reserved"
	TransferCommitted TransferStatus = "committed"
	TransferCredited  TransferStatus = "credited"
	TransferRejected  TransferStatus = "rejected"
)

type AuthorizationUsageV1 struct {
	AuthorizationID string `json:"authorization_id"`
	Consumed        Money  `json:"consumed"`
	Reserved        Money  `json:"reserved"`
	FactVersion     int    `json:"fact_version"`
}

type FinancialTransferV1 struct {
	TransferID         string           `json:"transfer_id"`
	OriginalTransferID string           `json:"original_transfer_id,omitempty"`
	From               string           `json:"from"`
	To                 string           `json:"to"`
	Amount             Money            `json:"amount"`
	Class              TransactionClass `json:"class"`
	Kind               string           `json:"kind"`
	Status             TransferStatus   `json:"status"`
	Policy             PolicyIdentityV1 `json:"policy"`
	DecisionID         string           `json:"decision_id"`
}

type PolicyAuditV1 struct {
	TransferID     string     `json:"transfer_id"`
	PolicyID       string     `json:"policy_id"`
	PolicyVersion  int        `json:"policy_version"`
	PolicyDigest   string     `json:"policy_digest"`
	DecisionID     string     `json:"decision_id"`
	Admission      ReasonCode `json:"admission"`
	AttestationIDs []string   `json:"attestation_ids"`
}

type AuthorityStateV1 struct {
	AuthorityID    string                          `json:"authority_id"`
	Balance        Money                           `json:"balance"`
	Policies       map[string]PolicyV1             `json:"policies"`
	Active         map[string]int                  `json:"active"`
	Disabled       map[string]bool                 `json:"disabled"`
	Authorizations map[string]AuthorizationUsageV1 `json:"authorizations"`
	Transfers      map[string]FinancialTransferV1  `json:"transfers"`
	Audit          []PolicyAuditV1                 `json:"audit"`
}

type ReservationResultV1 struct {
	Reserved  bool                `json:"reserved"`
	Duplicate bool                `json:"duplicate"`
	Reason    ReasonCode          `json:"reason"`
	Transfer  FinancialTransferV1 `json:"transfer"`
}

type mutationResultV1 struct {
	Reason   ReasonCode
	Transfer FinancialTransferV1
}

// Authority is the only API in this experiment that owns mutable financial
// state. Policy evaluators receive values, never this handle.
type Authority struct {
	id    string
	db    *octetdb.Database
	state *octetdb.Dataset
	mu    sync.Mutex
}

func OpenAuthority(ctx context.Context, root, id string, balance Money) (*Authority, error) {
	path := filepath.Join(root, strings.ReplaceAll(id, ":", "_"))
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return nil, err
	}
	bucket, err := db.Bucket(ctx, "bso-policy")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	dataset, err := bucket.Dataset(ctx, "authority", octetdb.DatasetOptions{TypeIdentity: "bso-trust-m3.AuthorityState/v1"})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	a := &Authority{id: id, db: db, state: dataset}
	var existing AuthorityStateV1
	found, err := dataset.Get(ctx, authorityStateKey, &existing)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	if !found {
		initial := AuthorityStateV1{AuthorityID: id, Balance: balance, Policies: map[string]PolicyV1{}, Active: map[string]int{}, Disabled: map[string]bool{}, Authorizations: map[string]AuthorizationUsageV1{}, Transfers: map[string]FinancialTransferV1{}}
		_, err = db.Mutate(ctx, octetdb.KeyedCommand{ID: "initialize/" + id}, func(tx *octetdb.Tx) (any, error) { return initial, tx.Put(dataset, authorityStateKey, initial) })
		if err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	return a, nil
}

func (a *Authority) Close() error {
	if a.db == nil {
		return nil
	}
	return a.db.Close()
}

func (a *Authority) Load(ctx context.Context) (AuthorityStateV1, error) {
	var s AuthorityStateV1
	ok, err := a.state.Get(ctx, authorityStateKey, &s)
	if err == nil && !ok {
		err = errors.New("authority state missing")
	}
	return s, err
}

func policyKey(id string, version int) string { return id + "/" + strconv.Itoa(version) }

func (a *Authority) mutate(ctx context.Context, commandID string, fn func(*AuthorityStateV1) mutationResultV1) (mutationResultV1, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	d, err := a.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var s AuthorityStateV1
		ok, e := tx.Get(a.state, authorityStateKey, &s)
		if e != nil || !ok {
			return nil, e
		}
		r := fn(&s)
		return r, tx.Put(a.state, authorityStateKey, s)
	})
	if err != nil {
		return mutationResultV1{}, false, err
	}
	var r mutationResultV1
	if err := octetdb.DecodeResult(d, &r); err != nil {
		return r, d.Duplicate, err
	}
	return r, d.Duplicate, nil
}

func (a *Authority) ProposePolicy(ctx context.Context, p PolicyV1) (PolicyV1, error) {
	p = SealPolicy(p)
	if err := ValidatePolicy(p); err != nil {
		return PolicyV1{}, err
	}
	if p.OwnerBSO != a.id || p.Scope.Subject != a.id {
		return PolicyV1{}, errors.New("policy authority scope does not belong to BSO")
	}
	key := policyKey(p.Identity.PolicyID, p.Identity.PolicyVersion)
	var conflict bool
	_, _, err := a.mutate(ctx, "propose/"+key+"/"+p.Identity.PolicyDigest, func(s *AuthorityStateV1) mutationResultV1 {
		if existing, ok := s.Policies[key]; ok && existing.Identity.PolicyDigest != p.Identity.PolicyDigest {
			conflict = true
			return mutationResultV1{Reason: InvalidPolicy}
		}
		s.Policies[key] = p
		return mutationResultV1{Reason: ReasonAllowed}
	})
	if err != nil {
		return PolicyV1{}, err
	}
	if conflict {
		return PolicyV1{}, errors.New("policy version history is immutable")
	}
	return p, nil
}

func (a *Authority) ActivatePolicy(ctx context.Context, policyID string, version int, expandedAuthorityApproved bool) (PolicyDiffV1, error) {
	key := policyKey(policyID, version)
	var diff PolicyDiffV1
	var activationErr error
	_, _, err := a.mutate(ctx, "activate/"+key+"/approved="+strconv.FormatBool(expandedAuthorityApproved), func(s *AuthorityStateV1) mutationResultV1 {
		p, ok := s.Policies[key]
		if !ok {
			activationErr = errors.New("proposed policy version not found")
			return mutationResultV1{Reason: InvalidPolicy}
		}
		if current := s.Active[policyID]; current > 0 {
			diff = DiffPolicies(s.Policies[policyKey(policyID, current)], p)
			if diff.AuthorityExpanded && !expandedAuthorityApproved {
				activationErr = errors.New("authority expansion requires explicit local approval")
				return mutationResultV1{Reason: InvalidPolicy}
			}
		}
		s.Active[policyID] = version
		return mutationResultV1{Reason: ReasonAllowed}
	})
	if err != nil {
		return diff, err
	}
	return diff, activationErr
}

func (a *Authority) DisablePolicy(ctx context.Context, policyID string, version int) error {
	key := policyKey(policyID, version)
	var missing bool
	_, _, err := a.mutate(ctx, "disable/"+key, func(s *AuthorityStateV1) mutationResultV1 {
		if _, ok := s.Policies[key]; !ok {
			missing = true
			return mutationResultV1{Reason: InvalidPolicy}
		}
		s.Disabled[key] = true
		return mutationResultV1{Reason: ReasonAllowed}
	})
	if err != nil {
		return err
	}
	if missing {
		return errors.New("policy version not found")
	}
	return nil
}

func (a *Authority) Policy(ctx context.Context, policyID string, version int) (PolicyV1, error) {
	s, err := a.Load(ctx)
	if err != nil {
		return PolicyV1{}, err
	}
	p, ok := s.Policies[policyKey(policyID, version)]
	if !ok {
		return PolicyV1{}, errors.New("policy version not found")
	}
	return p, nil
}

func (a *Authority) ActivePolicy(ctx context.Context, policyID string) (PolicyV1, error) {
	s, err := a.Load(ctx)
	if err != nil {
		return PolicyV1{}, err
	}
	version := s.Active[policyID]
	if version == 0 {
		return PolicyV1{}, errors.New("policy not active")
	}
	return s.Policies[policyKey(policyID, version)], nil
}

func factsFrom(s AuthorityStateV1, r TransferRequestV1) PolicyFactsV1 {
	u := s.Authorizations[r.AuthorizationID]
	return PolicyFactsV1{FactID: stableID("facts", s.AuthorityID, r.AuthorizationID, strconv.Itoa(u.FactVersion)), FactVersion: u.FactVersion, TransferID: r.TransferID, Subject: r.Subject, Actor: r.Actor, Counterparty: r.Counterparty, TransactionClass: r.Class, Amount: r.Amount, LogicalTime: r.LogicalTime, ConsumedAmount: u.Consumed, ReservedAmount: u.Reserved, TrustRoles: append([]string(nil), r.TrustRoles...), EscrowPresent: r.EscrowPresent}
}

func (a *Authority) Evaluate(ctx context.Context, p PolicyV1, r TransferRequestV1) (PolicyDecisionV1, error) {
	s, err := a.Load(ctx)
	if err != nil {
		return PolicyDecisionV1{}, err
	}
	return EvaluatePolicy(p, factsFrom(s, r)), nil
}

// Reserve rechecks active policy, digest, decision provenance, mutable fact
// version, balance, and cumulative capacity in the authority-owned critical
// section. It establishes the local invariant before any external effect.
func (a *Authority) Reserve(ctx context.Context, p PolicyV1, d PolicyDecisionV1, r TransferRequestV1) (ReservationResultV1, error) {
	// A rejected stale decision must not poison a later retry made from fresh
	// facts. Successful admission is still idempotent by the transfer record.
	command := "reserve/" + r.TransferID + "/" + d.DecisionID
	result, duplicate, err := a.mutate(ctx, command, func(s *AuthorityStateV1) mutationResultV1 {
		if existing, ok := s.Transfers[r.TransferID]; ok {
			if existing.From != r.Subject || existing.To != r.Counterparty || existing.Amount != r.Amount || existing.Class != r.Class || existing.Policy != p.Identity {
				return mutationResultV1{Reason: StateConflict}
			}
			return mutationResultV1{Reason: ReasonAllowed, Transfer: existing}
		}
		key := policyKey(p.Identity.PolicyID, p.Identity.PolicyVersion)
		stored, ok := s.Policies[key]
		if !ok || stored.Identity.PolicyDigest != p.Identity.PolicyDigest || d.Identity != p.Identity {
			return mutationResultV1{Reason: PolicyVersionMismatch}
		}
		if s.Active[p.Identity.PolicyID] != p.Identity.PolicyVersion {
			return mutationResultV1{Reason: PolicyVersionMismatch}
		}
		if s.Disabled[key] {
			return mutationResultV1{Reason: PolicyDisabled}
		}
		facts := factsFrom(*s, r)
		if !d.Allowed || d.RequestDigest != requestDigest(r.TransferID, r.Subject, r.Actor, r.Counterparty, r.Class, r.Amount, r.LogicalTime, r.TrustRoles, r.EscrowPresent) {
			return mutationResultV1{Reason: PolicyVersionMismatch}
		}
		if d.FactVersion != facts.FactVersion || d.FactID != facts.FactID {
			return mutationResultV1{Reason: StateConflict}
		}
		current := EvaluatePolicy(p, facts)
		if !current.Allowed {
			return mutationResultV1{Reason: current.Reason}
		}
		if s.Balance < r.Amount {
			return mutationResultV1{Reason: InsufficientBalance}
		}
		u := s.Authorizations[r.AuthorizationID]
		u.AuthorizationID = r.AuthorizationID
		u.Reserved += r.Amount
		u.FactVersion++
		s.Authorizations[r.AuthorizationID] = u
		t := FinancialTransferV1{TransferID: r.TransferID, From: r.Subject, To: r.Counterparty, Amount: r.Amount, Class: r.Class, Kind: r.Kind, Status: TransferReserved, Policy: p.Identity, DecisionID: d.DecisionID}
		t.OriginalTransferID = r.OriginalTransferID
		s.Transfers[r.TransferID] = t
		return mutationResultV1{Reason: ReasonAllowed, Transfer: t}
	})
	if err != nil {
		return ReservationResultV1{}, err
	}
	return ReservationResultV1{Reserved: result.Transfer.Status == TransferReserved || result.Transfer.Status == TransferCommitted, Duplicate: duplicate, Reason: result.Reason, Transfer: result.Transfer}, nil
}

func (a *Authority) Finalize(ctx context.Context, authorizationID, transferID string, attestationIDs []string) (FinancialTransferV1, error) {
	var finalizeErr error
	r, _, err := a.mutate(ctx, "finalize/"+transferID, func(s *AuthorityStateV1) mutationResultV1 {
		t, ok := s.Transfers[transferID]
		if !ok {
			finalizeErr = errors.New("reservation not found")
			return mutationResultV1{Reason: StateConflict}
		}
		if t.Status == TransferCommitted {
			return mutationResultV1{Reason: ReasonAllowed, Transfer: t}
		}
		u := s.Authorizations[authorizationID]
		if u.Reserved < t.Amount || s.Balance < t.Amount {
			finalizeErr = errors.New("reserved invariant missing")
			return mutationResultV1{Reason: StateConflict}
		}
		u.Reserved -= t.Amount
		u.Consumed += t.Amount
		u.FactVersion++
		s.Authorizations[authorizationID] = u
		s.Balance -= t.Amount
		t.Status = TransferCommitted
		s.Transfers[transferID] = t
		s.Audit = append(s.Audit, PolicyAuditV1{TransferID: transferID, PolicyID: t.Policy.PolicyID, PolicyVersion: t.Policy.PolicyVersion, PolicyDigest: t.Policy.PolicyDigest, DecisionID: t.DecisionID, Admission: ReasonAllowed, AttestationIDs: append([]string(nil), attestationIDs...)})
		return mutationResultV1{Reason: ReasonAllowed, Transfer: t}
	})
	if err != nil {
		return FinancialTransferV1{}, err
	}
	return r.Transfer, finalizeErr
}

// Credit is an idempotent bilateral protocol effect, not a policy callback.
func (a *Authority) Credit(ctx context.Context, original FinancialTransferV1) error {
	var creditErr error
	_, _, err := a.mutate(ctx, "credit/"+original.TransferID, func(s *AuthorityStateV1) mutationResultV1 {
		if existing, ok := s.Transfers[original.TransferID]; ok && existing.Status == TransferCredited {
			if existing.From != original.From || existing.To != original.To || existing.Amount != original.Amount || existing.DecisionID != original.DecisionID {
				creditErr = errors.New("duplicate credit identity mismatch")
				return mutationResultV1{Reason: StateConflict}
			}
			return mutationResultV1{Reason: ReasonAllowed, Transfer: existing}
		}
		if original.Status != TransferCommitted || original.To != a.id {
			creditErr = fmt.Errorf("invalid committed transfer for %s", a.id)
			return mutationResultV1{Reason: StateConflict}
		}
		t := original
		t.Status = TransferCredited
		s.Balance += t.Amount
		s.Transfers[t.TransferID] = t
		return mutationResultV1{Reason: ReasonAllowed, Transfer: t}
	})
	if err != nil {
		return err
	}
	return creditErr
}

func Settle(ctx context.Context, sender, receiver *Authority, p PolicyV1, r TransferRequestV1) (FinancialTransferV1, PolicyDecisionV1, ReasonCode, error) {
	d, err := sender.Evaluate(ctx, p, r)
	if err != nil {
		return FinancialTransferV1{}, d, StateConflict, err
	}
	if !d.Allowed {
		return FinancialTransferV1{}, d, d.Reason, nil
	}
	reserved, err := sender.Reserve(ctx, p, d, r)
	if err != nil || !reserved.Reserved {
		return reserved.Transfer, d, reserved.Reason, err
	}
	committed, err := sender.Finalize(ctx, r.AuthorizationID, r.TransferID, r.AttestationIDs)
	if err != nil {
		return committed, d, StateConflict, err
	}
	if err := receiver.Credit(ctx, committed); err != nil {
		return committed, d, StateConflict, err
	}
	return committed, d, ReasonAllowed, nil
}
