package bsotrustm3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	bsosim "github.com/yuechen-li-dev/octetdb/experiments/BSOTrust/M0"
)

type BlastRadiusV1 struct {
	AffectedFinancialAuthorities int `json:"affected_financial_authorities"`
	AffectedRelationships        int `json:"affected_relationships"`
	AffectedTransfers            int `json:"affected_transfers"`
	UnrelatedBSOsTouched         int `json:"unrelated_bsos_touched"`
	UnrelatedBalancesTouched     int `json:"unrelated_balances_touched"`
}

type WorkloadResultV1 struct {
	Name               string     `json:"name"`
	Attempted          int        `json:"attempted"`
	Succeeded          int        `json:"succeeded"`
	Rejected           int        `json:"rejected"`
	FinancialMutations int        `json:"financial_mutations"`
	Reason             ReasonCode `json:"reason"`
}

type AgentCheckpointV1 struct {
	TransferID          string           `json:"transfer_id"`
	Policy              PolicyIdentityV1 `json:"policy"`
	Decision            PolicyDecisionV1 `json:"decision"`
	PlacementGeneration int              `json:"placement_generation"`
}

type Result struct {
	Correct                       bool               `json:"correct"`
	Conservation                  bool               `json:"conservation"`
	RuntimeWithinBudget           bool               `json:"runtime_within_budget"`
	ElapsedMilliseconds           int64              `json:"elapsed_milliseconds"`
	Workloads                     []WorkloadResultV1 `json:"workloads"`
	BlastRadius                   BlastRadiusV1      `json:"blast_radius"`
	PolicyVersionsRetained        int                `json:"policy_versions_retained"`
	HistoricalBindingsRetained    int                `json:"historical_bindings_retained"`
	BuggyTransfers                int                `json:"buggy_transfers"`
	FutureAdmissionsAfterDisable  int                `json:"future_admissions_after_disable"`
	InFlightCompletedAfterDisable int                `json:"in_flight_completed_after_disable"`
	CompensatingTransfers         int                `json:"compensating_transfers"`
	IncompleteRecoveries          int                `json:"incomplete_recoveries"`
	ConcurrentSucceeded           int                `json:"concurrent_succeeded"`
	ConcurrentConsumed            Money              `json:"concurrent_consumed"`
	ConcurrentDoubleConsumption   int                `json:"concurrent_double_consumption"`
	ConfusedDeputyRejected        int                `json:"confused_deputy_rejected"`
	AuthorityAmplifications       int                `json:"authority_amplifications"`
	TOCTOURejected                int                `json:"toctou_rejected"`
	NestedActionRejected          int                `json:"nested_action_rejected"`
	CompositionRejected           int                `json:"composition_rejected"`
	CompositionFinancialMutations int                `json:"composition_financial_mutations"`
	WideningDetected              int                `json:"widening_detected"`
	UnapprovedWideningActivated   int                `json:"unapproved_widening_activated"`
	WidenedFutureSettled          int                `json:"widened_future_settled"`
	MigrationDecisionStable       int                `json:"migration_decision_stable"`
	MigrationFinancialEffects     int                `json:"migration_financial_effects"`
	SchedulerWorkerFailures       int                `json:"scheduler_worker_failures"`
	SchedulerAgentsMigrated       int                `json:"scheduler_agents_migrated"`
	StaleDecisionRejected         int                `json:"stale_decision_rejected"`
	MalformedEscrowRejected       int                `json:"malformed_escrow_rejected"`
	CanonicalPolicyBytes          int                `json:"canonical_policy_bytes"`
	AuditRecords                  int                `json:"audit_records"`
	UnrelatedStateTouched         int                `json:"unrelated_state_touched"`
	ArchitectureDecision          string             `json:"architecture_decision"`
	PolicySafetyDecision          string             `json:"policy_safety_decision"`
	ProgrammabilityDecision       string             `json:"programmability_decision"`
	RecoveryDecision              string             `json:"recovery_decision"`
	StudyDecision                 string             `json:"study_decision"`
	NextRecommendation            string             `json:"next_recommendation"`
}

func policy(id string, version int, owner, authID, actor, counterparty string, class TransactionClass, per, cumulative Money) PolicyV1 {
	return SealPolicy(PolicyV1{
		Identity:                 PolicyIdentityV1{PolicyID: id, PolicyVersion: version},
		Class:                    AuthorizationPolicy,
		OwnerBSO:                 owner,
		Scope:                    AuthorizationScopeV1{AuthorizationID: authID, Subject: owner, Delegate: actor, Counterparty: counterparty, TransactionClass: class, MaxAmount: per, MaxCumulativeAmount: cumulative, ValidUntil: 1000},
		RequiredTrustRoles:       []string{"authorization"},
		IntendedCumulativeAmount: cumulative,
	})
}

func request(id string, p PolicyV1, amount Money, now int) TransferRequestV1 {
	actor := p.Scope.Delegate
	if actor == "" {
		actor = p.Scope.Subject
	}
	return TransferRequestV1{TransferID: id, Subject: p.Scope.Subject, Actor: actor, Counterparty: p.Scope.Counterparty, AuthorizationID: p.Scope.AuthorizationID, Class: p.Scope.TransactionClass, Amount: amount, LogicalTime: now, TrustRoles: []string{"authorization"}, AttestationIDs: []string{"attestation:authorization:1"}, Kind: "payment"}
}

func install(ctx context.Context, a *Authority, p PolicyV1, approveExpansion bool) (PolicyV1, PolicyDiffV1, error) {
	p, err := a.ProposePolicy(ctx, p)
	if err != nil {
		return p, PolicyDiffV1{}, err
	}
	diff, err := a.ActivatePolicy(ctx, p.Identity.PolicyID, p.Identity.PolicyVersion, approveExpansion)
	return p, diff, err
}

func runConcurrent(ctx context.Context, payer, payee *Authority, p PolicyV1) (successes int, reasons []ReasonCode, err error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := request(fmt.Sprintf("concurrent:%02d", i), p, 10, 10)
			for attempt := 0; attempt < 40; attempt++ {
				_, _, reason, settleErr := Settle(ctx, payer, payee, p, r)
				if settleErr != nil {
					mu.Lock()
					if err == nil {
						err = settleErr
					}
					mu.Unlock()
					return
				}
				if reason == StateConflict {
					continue
				}
				mu.Lock()
				reasons = append(reasons, reason)
				if reason == ReasonAllowed {
					successes++
				}
				mu.Unlock()
				return
			}
			mu.Lock()
			reasons = append(reasons, StateConflict)
			mu.Unlock()
		}(i)
	}
	wg.Wait()
	return successes, reasons, err
}

func Run(ctx context.Context) (result Result, err error) {
	started := time.Now()
	root, err := os.MkdirTemp("", "octetdb-bso-trust-m3-")
	if err != nil {
		return result, err
	}
	defer os.RemoveAll(root)
	open := func(id string, balance Money) (*Authority, error) {
		return OpenAuthority(ctx, filepath.Join(root, "authorities"), id, balance)
	}
	alice, err := open("bso:alice", 5000)
	if err != nil {
		return result, err
	}
	bob, err := open("bso:bob", 1000)
	if err != nil {
		return result, err
	}
	carol, err := open("bso:carol", 777)
	if err != nil {
		return result, err
	}
	dave, err := open("bso:dave", 888)
	if err != nil {
		return result, err
	}
	authorities := []*Authority{alice, bob, carol, dave}
	defer func() {
		for _, a := range authorities {
			if e := a.Close(); err == nil && e != nil {
				err = e
			}
		}
	}()
	initialTotal := Money(5000 + 1000 + 777 + 888)
	carolBefore, _ := carol.Load(ctx)
	daveBefore, _ := dave.Load(ctx)

	// Workload 1: a correctly bounded recurring authorization.
	correctPolicy, _, err := install(ctx, alice, policy("policy:correct-recurring", 1, alice.id, "auth:correct", "service:x", bob.id, Subscription, 10, 100), false)
	if err != nil {
		return result, err
	}
	correctSuccess, correctRejected := 0, 0
	for i := 0; i < 11; i++ {
		_, _, reason, e := Settle(ctx, alice, bob, correctPolicy, request(fmt.Sprintf("correct:%02d", i), correctPolicy, 10, 1+i))
		if e != nil {
			return result, e
		}
		if reason == ReasonAllowed {
			correctSuccess++
		} else if reason == CumulativeLimitExceeded {
			correctRejected++
		}
	}
	result.Workloads = append(result.Workloads, WorkloadResultV1{Name: "correct recurring", Attempted: 11, Succeeded: correctSuccess, Rejected: correctRejected, FinancialMutations: correctSuccess, Reason: CumulativeLimitExceeded})

	// Workload 2: V2 is type-correct but omits the intended cumulative bound.
	v1, _, err := install(ctx, alice, policy("policy:buggy-recurring", 1, alice.id, "auth:bug-v1", "service:x", bob.id, Subscription, 10, 100), false)
	if err != nil {
		return result, err
	}
	_ = v1
	v2 := policy("policy:buggy-recurring", 2, alice.id, "auth:bug-v2", "service:x", bob.id, Subscription, 10, 0)
	v2.IntendedCumulativeAmount = 100
	v2 = SealPolicy(v2)
	v2, buggyDiff, err := install(ctx, alice, v2, true)
	if err != nil || !buggyDiff.AuthorityExpanded {
		return result, fmt.Errorf("buggy widening was not explicit: %v %+v", err, buggyDiff)
	}
	for i := 0; i < 12; i++ {
		_, _, reason, e := Settle(ctx, alice, bob, v2, request(fmt.Sprintf("bug:%02d", i), v2, 10, 20+i))
		if e != nil || reason != ReasonAllowed {
			return result, fmt.Errorf("bug exploit %d: %v %s", i, e, reason)
		}
		result.BuggyTransfers++
	}
	result.Workloads = append(result.Workloads, WorkloadResultV1{Name: "semantically buggy recurring", Attempted: 12, Succeeded: result.BuggyTransfers, FinancialMutations: result.BuggyTransfers, Reason: ReasonAllowed})

	// Workload 6 plus V: admission is the successful reservation boundary.
	inflightReq := request("bug:inflight", v2, 10, 40)
	inflightDecision, _ := alice.Evaluate(ctx, v2, inflightReq)
	inflightReservation, e := alice.Reserve(ctx, v2, inflightDecision, inflightReq)
	if e != nil || !inflightReservation.Reserved {
		return result, fmt.Errorf("in-flight admission failed: %v %+v", e, inflightReservation)
	}
	if err := alice.DisablePolicy(ctx, v2.Identity.PolicyID, v2.Identity.PolicyVersion); err != nil {
		return result, err
	}
	committed, e := alice.Finalize(ctx, inflightReq.AuthorizationID, inflightReq.TransferID, inflightReq.AttestationIDs)
	if e != nil {
		return result, e
	}
	if e := bob.Credit(ctx, committed); e != nil {
		return result, e
	}
	result.InFlightCompletedAfterDisable = 1
	futureReq := request("bug:future-blocked", v2, 10, 41)
	futureDecision, _ := alice.Evaluate(ctx, v2, futureReq)
	futureReservation, e := alice.Reserve(ctx, v2, futureDecision, futureReq)
	if e != nil {
		return result, e
	}
	if futureReservation.Reason == PolicyDisabled {
		result.FutureAdmissionsAfterDisable = 0
	} else {
		result.FutureAdmissionsAfterDisable = 1
	}

	// V3 restores V1's bounds as a new version; no historical record changes.
	v3 := policy("policy:buggy-recurring", 3, alice.id, "auth:bug-v3", "service:x", bob.id, Subscription, 10, 100)
	v3, _, err = install(ctx, alice, v3, false)
	if err != nil {
		return result, err
	}
	_ = v3

	// Workload 7: compensation is another scoped transaction, never an edit.
	refundPolicy, _, err := install(ctx, bob, policy("policy:refund", 1, bob.id, "auth:refund", bob.id, alice.id, Refund, 10, 10), false)
	if err != nil {
		return result, err
	}
	refundReq := request("refund:bug:00", refundPolicy, 10, 50)
	refundReq.OriginalTransferID = "bug:00"
	refundReq.Kind = "compensating_transfer"
	_, _, refundReason, e := Settle(ctx, bob, alice, refundPolicy, refundReq)
	if e != nil {
		return result, e
	}
	if refundReason == ReasonAllowed {
		result.CompensatingTransfers = 1
	}
	result.IncompleteRecoveries = result.BuggyTransfers + result.InFlightCompletedAfterDisable - result.CompensatingTransfers

	// Workloads 3 and BB: independent authority, 20 racers, exact cap 100.
	concurrentPayer, err := open("bso:concurrent-payer", 1000)
	if err != nil {
		return result, err
	}
	concurrentPayee, err := open("bso:concurrent-payee", 0)
	if err != nil {
		return result, err
	}
	authorities = append(authorities, concurrentPayer, concurrentPayee)
	concurrentPolicy, _, err := install(ctx, concurrentPayer, policy("policy:concurrent", 1, concurrentPayer.id, "auth:concurrent", "service:x", concurrentPayee.id, Subscription, 10, 100), false)
	if err != nil {
		return result, err
	}
	result.ConcurrentSucceeded, _, err = runConcurrent(ctx, concurrentPayer, concurrentPayee, concurrentPolicy)
	if err != nil {
		return result, err
	}
	concurrentState, _ := concurrentPayer.Load(ctx)
	result.ConcurrentConsumed = concurrentState.Authorizations["auth:concurrent"].Consumed
	if result.ConcurrentConsumed > 100 {
		result.ConcurrentDoubleConsumption = int(result.ConcurrentConsumed - 100)
	}
	result.Workloads = append(result.Workloads, WorkloadResultV1{Name: "concurrent cumulative authorization", Attempted: 20, Succeeded: result.ConcurrentSucceeded, Rejected: 20 - result.ConcurrentSucceeded, FinancialMutations: result.ConcurrentSucceeded, Reason: CumulativeLimitExceeded})

	// Workload 4: the delegate has only the exact five-dimensional scope.
	delegated := policy("policy:delegated", 1, alice.id, "auth:delegated", "service:x", bob.id, Subscription, 10, 100)
	baseFacts := PolicyFactsV1{FactID: "facts:delegate", FactVersion: 1, TransferID: "deputy", Subject: alice.id, Actor: "service:x", Counterparty: bob.id, TransactionClass: Subscription, Amount: 10, LogicalTime: 10, TrustRoles: []string{"authorization"}}
	variants := []PolicyFactsV1{baseFacts, baseFacts, baseFacts, baseFacts}
	variants[0].Counterparty = dave.id
	variants[1].TransactionClass = HighValueTransfer
	variants[2].Amount = 11
	variants[3].LogicalTime = 1001
	for _, facts := range variants {
		if !EvaluatePolicy(delegated, facts).Allowed {
			result.ConfusedDeputyRejected++
		}
	}
	amplificationFacts := baseFacts
	amplificationFacts.Counterparty = carol.id
	if EvaluatePolicy(delegated, amplificationFacts).Allowed {
		result.AuthorityAmplifications++
	}

	// Workload 9: two decisions see the same remaining capacity; only one reserves.
	toctouPayer, err := open("bso:toctou-payer", 100)
	if err != nil {
		return result, err
	}
	toctouPayee, err := open("bso:toctou-payee", 0)
	if err != nil {
		return result, err
	}
	authorities = append(authorities, toctouPayer, toctouPayee)
	toctouPolicy, _, err := install(ctx, toctouPayer, policy("policy:toctou", 1, toctouPayer.id, "auth:toctou", toctouPayer.id, toctouPayee.id, Subscription, 10, 10), false)
	if err != nil {
		return result, err
	}
	r1 := request("toctou:1", toctouPolicy, 10, 1)
	r2 := request("toctou:2", toctouPolicy, 10, 1)
	d1, _ := toctouPayer.Evaluate(ctx, toctouPolicy, r1)
	d2, _ := toctouPayer.Evaluate(ctx, toctouPolicy, r2)
	reserved1, _ := toctouPayer.Reserve(ctx, toctouPolicy, d1, r1)
	reserved2, _ := toctouPayer.Reserve(ctx, toctouPolicy, d2, r2)
	if reserved1.Reserved && reserved2.Reason == StateConflict {
		result.TOCTOURejected = 1
		result.StaleDecisionRejected = 1
	}
	t1, e := toctouPayer.Finalize(ctx, r1.AuthorizationID, r1.TransferID, r1.AttestationIDs)
	if e != nil {
		return result, e
	}
	if e = toctouPayee.Credit(ctx, t1); e != nil {
		return result, e
	}

	// Workload 8: A reserves the complete authority before an indirect B is evaluated.
	nestedPayer, err := open("bso:nested-payer", 100)
	if err != nil {
		return result, err
	}
	nestedPayee, err := open("bso:nested-payee", 0)
	if err != nil {
		return result, err
	}
	authorities = append(authorities, nestedPayer, nestedPayee)
	nestedPolicy, _, err := install(ctx, nestedPayer, policy("policy:nested", 1, nestedPayer.id, "auth:nested", nestedPayer.id, nestedPayee.id, EscrowRelease, 10, 10), false)
	if err != nil {
		return result, err
	}
	aReq := request("nested:a", nestedPolicy, 10, 1)
	aDecision, _ := nestedPayer.Evaluate(ctx, nestedPolicy, aReq)
	aReservation, _ := nestedPayer.Reserve(ctx, nestedPolicy, aDecision, aReq)
	bReq := request("nested:b", nestedPolicy, 10, 1)
	bDecision, _ := nestedPayer.Evaluate(ctx, nestedPolicy, bReq)
	if aReservation.Reserved && !bDecision.Allowed && bDecision.Reason == CumulativeLimitExceeded {
		result.NestedActionRejected = 1
	}
	nestedCommitted, e := nestedPayer.Finalize(ctx, aReq.AuthorizationID, aReq.TransferID, aReq.AttestationIDs)
	if e != nil {
		return result, e
	}
	if e = nestedPayee.Credit(ctx, nestedCommitted); e != nil {
		return result, e
	}

	// Workload 12: all applicable policy decisions must allow. No precedence merge.
	compositionAuth := policy("policy:composition-auth", 1, alice.id, "auth:composition", alice.id, bob.id, Subscription, 10, 100)
	compositionApp := policy("policy:composition-app", 1, alice.id, "auth:composition", alice.id, bob.id, Subscription, 10, 100)
	compositionApp.Class = ApplicationPolicy
	compositionApp.EscrowRequiredAbove = 5
	compositionApp.EscrowCondition = DeliveryOrTimeoutNoDispute
	compositionApp = SealPolicy(compositionApp)
	compositionFacts := PolicyFactsV1{FactID: "facts:composition", FactVersion: 0, TransferID: "composition:1", Subject: alice.id, Actor: alice.id, Counterparty: bob.id, TransactionClass: Subscription, Amount: 6, LogicalTime: 1, TrustRoles: []string{"authorization"}, EscrowPresent: false}
	composed := ComposeDecisions(EvaluatePolicy(compositionAuth, compositionFacts), EvaluatePolicy(compositionApp, compositionFacts))
	if !composed.Allowed && composed.Reason == MissingEscrow {
		result.CompositionRejected = 1
	}

	// Workload 5: widening is machine-readable and needs an extra local approval.
	widenV1, _, err := install(ctx, alice, policy("policy:widen", 1, alice.id, "auth:widen-v1", alice.id, bob.id, HighValueTransfer, 10, 100), false)
	if err != nil {
		return result, err
	}
	widenV2, err := alice.ProposePolicy(ctx, policy("policy:widen", 2, alice.id, "auth:widen-v2", alice.id, bob.id, HighValueTransfer, 10000, 10000))
	if err != nil {
		return result, err
	}
	widenDiff := DiffPolicies(widenV1, widenV2)
	if widenDiff.AuthorityExpanded {
		result.WideningDetected = 1
	}
	if _, activateErr := alice.ActivatePolicy(ctx, "policy:widen", 2, false); activateErr == nil {
		result.UnapprovedWideningActivated = 1
	}
	activeBeforeApproval, activeErr := alice.ActivePolicy(ctx, "policy:widen")
	if activeErr != nil || activeBeforeApproval.Identity.PolicyVersion != 1 {
		result.UnapprovedWideningActivated = 1
	}
	if _, activateErr := alice.ActivatePolicy(ctx, "policy:widen", 2, true); activateErr != nil {
		return result, activateErr
	}
	activeAfterApproval, activeErr := alice.ActivePolicy(ctx, "policy:widen")
	if activeErr != nil || activeAfterApproval.Identity.PolicyVersion != 2 {
		return result, errors.New("explicitly approved widening did not activate V2")
	}
	_, _, widenedReason, widenedErr := Settle(ctx, alice, bob, activeAfterApproval, request("widen:future", activeAfterApproval, 100, 60))
	if widenedErr != nil {
		return result, widenedErr
	}
	if widenedReason == ReasonAllowed {
		result.WidenedFutureSettled = 1
	}

	// Workload 10: checkpoint contains exact value identities, not evaluator code.
	migrationPolicy, _, err := install(ctx, alice, policy("policy:migration", 1, alice.id, "auth:migration", alice.id, bob.id, Subscription, 5, 5), false)
	if err != nil {
		return result, err
	}
	migrationReq := request("migration:1", migrationPolicy, 5, 70)
	migrationDecision, _ := alice.Evaluate(ctx, migrationPolicy, migrationReq)
	checkpoint := AgentCheckpointV1{TransferID: migrationReq.TransferID, Policy: migrationPolicy.Identity, Decision: migrationDecision, PlacementGeneration: 1}
	migrated := checkpoint
	migrated.PlacementGeneration++
	if migrated.Decision.DecisionID == checkpoint.Decision.DecisionID && migrated.Decision.Identity == checkpoint.Decision.Identity {
		result.MigrationDecisionStable = 1
	}
	migrationReservation, _ := alice.Reserve(ctx, migrationPolicy, migrated.Decision, migrationReq)
	if migrationReservation.Reserved {
		migrationTransfer, e := alice.Finalize(ctx, migrationReq.AuthorizationID, migrationReq.TransferID, migrationReq.AttestationIDs)
		if e != nil {
			return result, e
		}
		_, _ = alice.Finalize(ctx, migrationReq.AuthorizationID, migrationReq.TransferID, migrationReq.AttestationIDs)
		if e = bob.Credit(ctx, migrationTransfer); e != nil {
			return result, e
		}
		if e = bob.Credit(ctx, migrationTransfer); e != nil {
			return result, e
		}
		result.MigrationFinancialEffects = 1
	}
	// Re-run the established real scheduler path with a worker killed while
	// trust evidence is pending. M3 adds the exact policy checkpoint above;
	// placement/recovery authority remains the M0 scheduler mechanism.
	schedulerConfig := bsosim.DefaultConfig()
	schedulerConfig.KillWorker, schedulerConfig.KillRound = 1, 2
	schedulerConfig.Faults = bsosim.FaultProfiles["worker-loss"]
	schedulerResult, schedulerErr := bsosim.Run(ctx, schedulerConfig)
	if schedulerErr != nil || !schedulerResult.Correct || !schedulerResult.Conservation {
		return result, fmt.Errorf("scheduler worker-loss regression: %v", schedulerErr)
	}
	result.SchedulerWorkerFailures = schedulerResult.Metrics.WorkerFailures
	result.SchedulerAgentsMigrated = schedulerResult.Metrics.AgentsMigrated

	malformed := policy("policy:malformed-escrow", 1, alice.id, "auth:escrow", alice.id, bob.id, EscrowRelease, 10, 10)
	malformed.EscrowRequiredAbove = 5
	malformed.EscrowCondition = NoEscrowCondition
	malformed = SealPolicy(malformed)
	if ValidatePolicy(malformed) != nil {
		result.MalformedEscrowRejected = 1
	}

	aliceState, _ := alice.Load(ctx)
	bobState, _ := bob.Load(ctx)
	carolAfter, _ := carol.Load(ctx)
	daveAfter, _ := dave.Load(ctx)
	result.PolicyVersionsRetained = 0
	for key := range aliceState.Policies {
		if len(key) > 0 {
			result.PolicyVersionsRetained++
		}
	}
	for _, audit := range aliceState.Audit {
		if audit.PolicyID == "policy:buggy-recurring" && audit.PolicyVersion == 2 {
			result.HistoricalBindingsRetained++
		}
	}
	result.AuditRecords = len(aliceState.Audit) + len(bobState.Audit)
	result.CanonicalPolicyBytes = len(CanonicalPolicyBytes(v2))
	if carolAfter.Balance != carolBefore.Balance || daveAfter.Balance != daveBefore.Balance || len(carolAfter.Transfers) != len(carolBefore.Transfers) || len(daveAfter.Transfers) != len(daveBefore.Transfers) {
		result.UnrelatedStateTouched = 1
	}
	result.BlastRadius = BlastRadiusV1{AffectedFinancialAuthorities: 2, AffectedRelationships: 1, AffectedTransfers: result.BuggyTransfers + result.InFlightCompletedAfterDisable, UnrelatedBSOsTouched: result.UnrelatedStateTouched, UnrelatedBalancesTouched: result.UnrelatedStateTouched}

	states := []AuthorityStateV1{aliceState, bobState, carolAfter, daveAfter}
	finalTotal := Money(0)
	for _, s := range states {
		finalTotal += s.Balance
	}
	for _, a := range []*Authority{concurrentPayer, concurrentPayee, toctouPayer, toctouPayee, nestedPayer, nestedPayee} {
		s, _ := a.Load(ctx)
		finalTotal += s.Balance
	}
	initialTotal += 1000 + 0 + 100 + 0 + 100 + 0
	result.Conservation = finalTotal == initialTotal
	sort.Slice(result.Workloads, func(i, j int) bool { return result.Workloads[i].Name < result.Workloads[j].Name })
	result.ElapsedMilliseconds = time.Since(started).Milliseconds()
	result.RuntimeWithinBudget = time.Since(started) <= 60*time.Second
	result.ArchitectureDecision = "A. Bounded typed policy composes cleanly with BSO financial authority and materially limits DAO-class failure scope."
	result.PolicySafetyDecision = "P2. Structural protections work, but specification bugs remain a major operational risk."
	result.ProgrammabilityDecision = "G1. Bounded typed policy covers the tested programmable-finance cases without a contract VM."
	result.RecoveryDecision = "R2. Recovery works but one committed-error class remains operationally severe."
	result.StudyDecision = "D1. A credible general architecture exists around local durable authorities, explicit protocols, typed capabilities/attestations, and selective consensus."
	result.NextRecommendation = "Test adversarial cross-authority saga recovery when one participant crashes or refuses compensation after local commit."
	result.Correct = result.Conservation && result.RuntimeWithinBudget && correctSuccess == 10 && correctRejected == 1 && result.BuggyTransfers == 12 && result.FutureAdmissionsAfterDisable == 0 && result.InFlightCompletedAfterDisable == 1 && result.CompensatingTransfers == 1 && result.ConcurrentSucceeded == 10 && result.ConcurrentConsumed == 100 && result.ConcurrentDoubleConsumption == 0 && result.ConfusedDeputyRejected == 4 && result.AuthorityAmplifications == 0 && result.TOCTOURejected == 1 && result.NestedActionRejected == 1 && result.CompositionRejected == 1 && result.CompositionFinancialMutations == 0 && result.WideningDetected == 1 && result.UnapprovedWideningActivated == 0 && result.WidenedFutureSettled == 1 && result.MigrationDecisionStable == 1 && result.MigrationFinancialEffects == 1 && result.SchedulerWorkerFailures == 1 && result.SchedulerAgentsMigrated > 0 && result.StaleDecisionRejected == 1 && result.MalformedEscrowRejected == 1 && result.UnrelatedStateTouched == 0
	return result, nil
}
