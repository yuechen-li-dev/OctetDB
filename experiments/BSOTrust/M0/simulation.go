package bsosim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

type simulation struct {
	ctx             context.Context
	config          Config
	root            string
	bsos            map[string]*durableBSO
	activeIDs       []string
	agents          map[string]*TransactionAgent
	coordinator     *SchedulerCoordinator
	transport       *transport
	metrics         *metricStore
	round           int
	recoveryTouched map[string]bool
	recoveryAgents  map[string]bool
	recoveryMu      sync.Mutex
	trustRegistry   *TrustRegistry
	policies        map[string]TrustPolicyV1
	trustMu         sync.Mutex
	resolutions     map[string]TrustResolutionV1
}

func Run(ctx context.Context, c Config) (result Result, err error) {
	c, err = normalizeConfig(c)
	if err != nil {
		return result, err
	}
	root := c.DataDir
	cleanup := func() {}
	if root == "" {
		root, err = os.MkdirTemp("", "octetdb-bso-trust-m0-")
		if err != nil {
			return result, err
		}
		cleanup = func() { _ = os.RemoveAll(root) }
	}
	defer cleanup()
	s := &simulation{ctx: ctx, config: c, root: filepath.Join(root, "bso"), bsos: map[string]*durableBSO{}, agents: map[string]*TransactionAgent{}, recoveryTouched: map[string]bool{}, recoveryAgents: map[string]bool{}, metrics: &metricStore{}, policies: defaultPolicies(), resolutions: map[string]TrustResolutionV1{}}
	s.trustRegistry = newTrustRegistry(s.metrics)
	s.coordinator = newCoordinator(c.Workers, s.metrics)
	s.transport = newTransport(c.Seed+17, c.Faults, s.metrics)
	s.coordinator.transport = s.transport
	defer func() {
		for _, b := range s.bsos {
			if e := b.close(); err == nil && e != nil {
				err = e
			}
		}
	}()
	attempts := GenerateWorkload(c)
	c.Transfers = len(attempts)
	s.config.Transfers = len(attempts)
	s.metrics.add(func(m *Metrics) { m.Attempted = len(attempts) })
	start := time.Now()
	for _, a := range attempts {
		if _, e := s.activate(a.From); e != nil {
			return result, e
		}
		if _, e := s.activate(a.To); e != nil {
			return result, e
		}
		agent := &TransactionAgent{ProtocolVersion: 1, TransferID: a.ID, SenderBSO: a.From, ReceiverBSO: a.To, Amount: a.Amount, TransactionClass: a.Class, ApplicationReference: a.ApplicationReference, Phase: PhaseCreated}
		s.agents[a.ID] = agent
		if e := s.coordinator.assign(agent); e != nil {
			return result, e
		}
	}
	s.metrics.add(func(m *Metrics) { m.PeakActiveAgents = len(s.agents); m.GoroutinesIdle = runtime.NumGoroutine() })
	s.coordinator.start(s)
	defer s.coordinator.shutdown()
	s.metrics.add(func(m *Metrics) { m.GoroutinesPeak = runtime.NumGoroutine() })
	for round := 0; round < c.MaxRounds; round++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		s.round = round
		if c.KillWorker >= 0 && round == c.KillRound {
			if e := s.coordinator.kill(c.KillWorker); e != nil {
				return result, e
			}
			// The checkpoint is the new authoritative protocol record. Replace the
			// simulation's observation index so no stale worker-local pointer can
			// influence completion or the semantic digest.
			for transferID := range s.agents {
				if restored, _, ok := s.coordinator.agent(transferID); ok {
					s.agents[transferID] = restored
				}
			}
		}
		if c.RestartBSO != "" && round == c.RestartRound {
			if e := s.restartBSO(c.RestartBSO); e != nil {
				return result, e
			}
		}
		if e := s.runWorkers(); e != nil {
			return result, e
		}
		if e := s.transport.drain(s.routeDelivery); e != nil {
			return result, e
		}
		if s.allTerminal() {
			break
		}
		s.wakeDeadlines()
	}
	if !s.allTerminal() {
		phases := map[AgentPhase]int{}
		for _, a := range s.agents {
			if !a.Phase.terminal() {
				phases[a.Phase]++
			}
		}
		return result, fmt.Errorf("%d agents unresolved after %d rounds: %v", s.unresolved(), c.MaxRounds, phases)
	}
	s.coordinator.shutdown()
	s.metrics.add(func(m *Metrics) { m.GoroutinesShutdown = runtime.NumGoroutine() })
	elapsed := time.Since(start)
	return s.result(attempts, elapsed)
}

func normalizeConfig(c Config) (Config, error) {
	d := DefaultConfig()
	if c.Seed == 0 {
		c.Seed = d.Seed
	}
	if c.BSOs == 0 {
		c.BSOs = d.BSOs
	}
	if c.Transfers == 0 {
		c.Transfers = d.Transfers
	}
	if c.Workers == 0 {
		c.Workers = d.Workers
	}
	if c.InitialBalance == 0 {
		c.InitialBalance = d.InitialBalance
	}
	if c.Workload == "" {
		c.Workload = d.Workload
	}
	if c.Faults.Name == "" {
		c.Faults = d.Faults
	}
	if c.MaxRounds == 0 {
		c.MaxRounds = d.MaxRounds
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = d.RetryDelay
	}
	if c.ReservationExpiry == 0 {
		c.ReservationExpiry = d.ReservationExpiry
	}
	if c.BSOs < 2 || c.Transfers < 1 || c.Workers < 1 {
		return c, errors.New("BSOs >= 2, transfers >= 1, workers >= 1 required")
	}
	return c, nil
}
func GenerateWorkload(c Config) []Attempt {
	if c.Workload == "trust-suite" {
		return trustSuiteAttempts()
	}
	rng := rand.New(rand.NewSource(c.Seed))
	a := make([]Attempt, 0, c.Transfers)
	for i := 0; i < c.Transfers; i++ {
		from, to := 0, 1
		amount := Money(1 + rng.Intn(1000))
		switch c.Workload {
		case "affected-set":
			from, to, amount = 37%c.BSOs, 829%c.BSOs, 42
			if from == to {
				to = (to + 1) % c.BSOs
			}
		case "hot-merchant":
			to = c.BSOs - 1
			from = i % (c.BSOs - 1)
		case "hot-payer":
			from = 0
			to = 1 + i%(c.BSOs-1)
			amount = Money(1 + rng.Intn(200))
		default:
			from = rng.Intn(c.BSOs)
			to = rng.Intn(c.BSOs - 1)
			if to >= from {
				to++
			}
		}
		a = append(a, Attempt{ID: fmt.Sprintf("transfer:%08d", i), From: bsoID(from), To: bsoID(to), Amount: amount, Class: ClassDirect})
	}
	return a
}
func bsoID(i int) string { return fmt.Sprintf("bso:%06d", i) }
func (s *simulation) activate(id string) (*durableBSO, error) {
	if b := s.bsos[id]; b != nil {
		return b, nil
	}
	policy, ok := s.policies[id]
	if !ok {
		policy = TrustPolicyV1{BSOID: id, Version: 1, DirectLimit: 100_000, ValidUntil: 10_000}
	}
	b, err := openBSO(s.ctx, s.root, id, s.config.InitialBalance, policy, s.metrics)
	if err != nil {
		return nil, err
	}
	s.bsos[id] = b
	s.activeIDs = append(s.activeIDs, id)
	sort.Strings(s.activeIDs)
	s.metrics.add(func(m *Metrics) { m.OpenBSODatabases = len(s.bsos) })
	return b, nil
}

func (s *simulation) runWorkers() error {
	done := make(chan workerResult, len(s.coordinator.workers))
	active := 0
	for _, w := range s.coordinator.workers {
		w.mu.Lock()
		alive := w.Alive
		w.mu.Unlock()
		if !alive {
			continue
		}
		active++
		if err := sendRound(s.ctx, w, s.round, done); err != nil {
			return err
		}
	}
	var first error
	for i := 0; i < active; i++ {
		if r := <-done; first == nil && r.err != nil {
			first = r.err
		}
	}
	return first
}
func (s *simulation) step(w *SchedulerWorker, a *TransactionAgent, round int) error {
	sender := s.bsos[a.SenderBSO]
	switch a.Phase {
	case PhaseCreated:
		if predecessor := subscriptionPredecessor(a.TransferID); predecessor != "" {
			s.trustMu.Lock()
			prior, ready := s.resolutions[predecessor]
			s.trustMu.Unlock()
			if !ready || !prior.Admitted {
				return nil
			}
		}
		senderPolicy, err := sender.trustPolicy(s.ctx)
		if err != nil {
			return err
		}
		receiverPolicy, err := s.bsos[a.ReceiverBSO].trustPolicy(s.ctx)
		if err != nil {
			return err
		}
		a.SenderPolicyVersion, a.ReceiverPolicyVersion = senderPolicy.Version, receiverPolicy.Version
		a.TrustResolutionID = stableID("resolution", fmt.Sprintf("%s|%d|%d", a.TransferID, senderPolicy.Version, receiverPolicy.Version))
		requirements, failure := resolvePolicyIntersection(senderPolicy, receiverPolicy, Attempt{ID: a.TransferID, From: a.SenderBSO, To: a.ReceiverBSO, Amount: a.Amount, Class: a.TransactionClass, ApplicationReference: a.ApplicationReference}, round, s.trustRegistry)
		if failure != "" {
			s.metrics.add(func(m *Metrics) { m.PolicyIntersectionFailures++ })
			return s.finishTrust(a, false, failure, round)
		}
		for _, requirement := range requirements {
			a.RequiredRoles = append(a.RequiredRoles, requirement.Role)
			a.TrustThresholds = append(a.TrustThresholds, requirement.Threshold)
			for _, providerID := range requirement.Candidates {
				a.TrustCandidates = append(a.TrustCandidates, string(requirement.Role)+"|"+providerID)
			}
		}
		if len(requirements) == 0 {
			return s.finishTrust(a, true, "", round)
		}
		a.Phase = PhaseTrustCollect
	case PhaseTrustCollect:
		return s.collectTrust(a, round)
	case PhaseTrustAdmitted:
		if a.TrustResolutionID == "" {
			s.metrics.add(func(m *Metrics) { m.SettlementBeforeTrust++ })
			return errors.New("settlement attempted without trust resolution")
		}
		state, err := sender.reserve(s.ctx, Attempt{ID: a.TransferID, From: a.SenderBSO, To: a.ReceiverBSO, Amount: a.Amount}, round)
		if err != nil {
			return err
		}
		a.LastObservedSenderVersion++
		if state == StateRejected {
			a.Phase = PhaseRejected
			a.CompletedRound = round
			return nil
		}
		a.Phase = PhaseOfferReceiver
	case PhaseOfferReceiver:
		s.transport.send(w.ID, a.PlacementGeneration, newEnvelope(a.TransferID, a.SenderBSO, a.ReceiverBSO, a.Amount, MessageOffer, StateReserved))
		a.LastMessageKind = MessageOffer
		a.Phase = PhaseAwaitAccept
		// Delivery is routed into the owning worker's queue and therefore needs
		// one additional logical round compared with M1's inline delivery.
		a.NextLogicalDeadline = round + s.config.RetryDelay + 1
	case PhaseCommitSender:
		e := newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, MessageAccept, StateAccepted)
		data, _ := EncodeEnvelope(e)
		decoded, err := DecodeEnvelope(data)
		if err != nil {
			return err
		}
		state, err := sender.commitSender(s.ctx, decoded)
		if err != nil {
			return err
		}
		a.LastObservedSenderVersion++
		if state != StateCommitted && state != StateAcknowledged {
			a.Phase = PhaseReconcile
		} else {
			a.Phase = PhaseCommitReceiver
		}
	case PhaseCommitReceiver:
		s.transport.send(w.ID, a.PlacementGeneration, newEnvelope(a.TransferID, a.SenderBSO, a.ReceiverBSO, a.Amount, MessageCommit, StateCommitted))
		a.LastMessageKind = MessageCommit
		a.Phase = PhaseAwaitAcknowledge
		a.NextLogicalDeadline = round + s.config.RetryDelay + 1
	case PhaseReconcile:
		return s.reconcileAgent(w, a)
	}
	return nil
}

func subscriptionPredecessor(transferID string) string {
	switch transferID {
	case "trust:subscription:2":
		return "trust:subscription:1"
	case "trust:subscription:3":
		return "trust:subscription:2"
	default:
		return ""
	}
}

func roleAndProvider(candidate string) (TrustRole, string) {
	parts := strings.SplitN(candidate, "|", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return TrustRole(parts[0]), parts[1]
}

func (a *TransactionAgent) threshold(role TrustRole) int {
	for i := range a.RequiredRoles {
		if a.RequiredRoles[i] == role {
			return a.TrustThresholds[i]
		}
	}
	return 0
}

func (a *TransactionAgent) approvals(role TrustRole) int {
	n := 0
	seen := map[string]bool{}
	for _, selected := range a.SelectedProviders {
		selectedRole, providerID := roleAndProvider(selected)
		if selectedRole == role && !seen[providerID] {
			seen[providerID] = true
			n++
		}
	}
	return n
}

func containsString(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}

func (s *simulation) collectTrust(a *TransactionAgent, round int) error {
	for a.TrustProviderIndex < len(a.TrustCandidates) {
		candidate := a.TrustCandidates[a.TrustProviderIndex]
		a.TrustProviderIndex++
		role, providerID := roleAndProvider(candidate)
		if role == "" {
			return errors.New("invalid trust candidate")
		}
		if a.approvals(role) >= a.threshold(role) {
			continue
		}
		request := TrustRequestV1{Role: role, TransferID: a.TransferID, SenderBSO: a.SenderBSO, ReceiverBSO: a.ReceiverBSO, SubjectBSO: a.SenderBSO, Amount: a.Amount, TransactionClass: a.TransactionClass, ApplicationReference: a.ApplicationReference, LogicalTime: round, RequestedPolicyVersion: 1}
		attestation, reused, err := s.trustRegistry.issue(providerID, request)
		a.TrustProvidersConsulted++
		if err != nil {
			return err
		}
		if attestation.ID == "" {
			continue
		}
		if reused {
			a.ReusedTrustAttestations++
		} else {
			a.FreshProviderCalls++
		}
		if attestation.ValidUntil < round {
			s.metrics.add(func(m *Metrics) { m.ExpiredAttestationsRejected++ })
			continue
		}
		if attestation.approves() {
			if containsString(a.CollectedAttestationIDs, attestation.ID) {
				s.metrics.add(func(m *Metrics) { m.DuplicateAttestationsSuppressed++ })
			} else {
				a.CollectedAttestationIDs = append(a.CollectedAttestationIDs, attestation.ID)
				a.SelectedProviders = append(a.SelectedProviders, string(role)+"|"+providerID)
				s.metrics.add(func(m *Metrics) { m.AttestationsUsed++ })
				if reused { /* ReusedAttestations is counted by the registry. */
				}
				firstForRole := ""
				for _, planned := range a.TrustCandidates {
					plannedRole, plannedID := roleAndProvider(planned)
					if plannedRole == role {
						firstForRole = plannedID
						break
					}
				}
				if a.threshold(role) == 1 && firstForRole != providerID {
					s.metrics.add(func(m *Metrics) { m.FallbackProviderUses++ })
				}
			}
		}
		// One provider call per agent step makes trust-pending migration an
		// observable checkpoint seam rather than an artificial test hook.
		return nil
	}
	for _, role := range a.RequiredRoles {
		if a.approvals(role) < a.threshold(role) {
			return s.finishTrust(a, false, fmt.Sprintf("role %s received %d/%d approvals", role, a.approvals(role), a.threshold(role)), round)
		}
	}
	return s.finishTrust(a, true, "", round)
}

func (s *simulation) finishTrust(a *TransactionAgent, admitted bool, failure string, round int) error {
	providers := make([]string, 0, len(a.SelectedProviders))
	for _, selected := range a.SelectedProviders {
		_, providerID := roleAndProvider(selected)
		providers = append(providers, providerID)
	}
	resolution := TrustResolutionV1{ResolutionID: a.TrustResolutionID, TransferID: a.TransferID, RequiredRoles: append([]TrustRole(nil), a.RequiredRoles...), SelectedProviders: providers, AttestationIDs: append([]string(nil), a.CollectedAttestationIDs...), Admitted: admitted, FailureReason: failure, SenderPolicyVersion: a.SenderPolicyVersion, ReceiverPolicyVersion: a.ReceiverPolicyVersion, IssuedAt: round, ProvidersConsulted: a.TrustProvidersConsulted, FreshProviderCalls: a.FreshProviderCalls, ReusedAttestations: a.ReusedTrustAttestations}
	bytes := EncodeTrustResolution(resolution)
	directory := filepath.Join(filepath.Dir(s.root), "trust_resolutions")
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(directory, strings.ReplaceAll(a.TransferID, ":", "_")+".octagon"), bytes, 0o644); err != nil {
		return err
	}
	s.trustMu.Lock()
	s.resolutions[a.TransferID] = resolution
	s.trustMu.Unlock()
	s.metrics.add(func(m *Metrics) {
		m.TrustResolutions++
		if admitted {
			m.TrustAdmitted++
		} else {
			m.TrustRejected++
		}
	})
	if admitted {
		a.Phase = PhaseTrustAdmitted
	} else {
		a.TrustFailureReason = failure
		a.Phase = PhaseRejected
		a.CompletedRound = round
	}
	return nil
}

// routeDelivery performs no financial work and does not consult the
// coordinator. The transport event carries the owning worker and placement
// generation captured at submission (and retargeted only during migration).
func (s *simulation) routeDelivery(workerID, generation int, e ProtocolEnvelopeV1) error {
	if workerID < 0 || workerID >= len(s.coordinator.workers) {
		return fmt.Errorf("invalid delivery worker %d", workerID)
	}
	s.coordinator.workers[workerID].enqueueEnvelope(e, generation)
	return nil
}

func (s *simulation) deliver(w *SchedulerWorker, a *TransactionAgent, e ProtocolEnvelopeV1, round int) error {
	if err := validateEnvelope(e); err != nil {
		s.metrics.add(func(m *Metrics) { m.AuthenticationFailures++ })
		return nil
	}
	receiver := s.bsos[e.To]
	if receiver == nil {
		return nil
	}
	s.metrics.add(func(m *Metrics) { m.ParticipantsTouched++ })
	switch e.Kind {
	case MessageOffer:
		state, err := receiver.offer(s.ctx, e)
		if err != nil {
			return err
		}
		a.LastObservedReceiverVersion++
		reply := MessageAccept
		if state != StateAccepted && state != StateCommitted {
			reply = MessageReject
		}
		s.transport.send(w.ID, a.PlacementGeneration, newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, reply, state))
	case MessageAccept:
		if a.Phase == PhaseAwaitAccept {
			a.Phase = PhaseCommitSender
			w.enqueue(a.TransferID)
		}
	case MessageReject:
		a.Phase = PhaseRejected
		a.CompletedRound = round
		s.coordinator.complete(w.ID, a.TransferID)
	case MessageCommit:
		state, err := receiver.commitReceiver(s.ctx, e)
		if err != nil {
			return err
		}
		a.LastObservedReceiverVersion++
		if state == StateCommitted {
			s.transport.send(w.ID, a.PlacementGeneration, newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, MessageAcknowledge, state))
		}
	case MessageAcknowledge:
		state, err := receiver.ackSender(s.ctx, e)
		if err != nil {
			return err
		}
		a.LastObservedSenderVersion++
		if state == StateAcknowledged {
			a.Phase = PhaseDone
			a.CompletedRound = round
			s.coordinator.complete(w.ID, a.TransferID)
		}
	}
	return nil
}

func (s *simulation) wakeDeadlines() {
	for _, a := range s.agents {
		if a.Phase.terminal() || (a.Phase != PhaseAwaitAccept && a.Phase != PhaseAwaitAcknowledge) || a.NextLogicalDeadline > s.round+1 {
			continue
		}
		_, wid, ok := s.coordinator.agent(a.TransferID)
		if !ok {
			continue
		}
		a.RetryCount++
		s.metrics.add(func(m *Metrics) { m.Retries++ })
		if s.round-a.NextLogicalDeadline >= s.config.ReservationExpiry {
			a.Phase = PhaseReconcile
		} else if a.Phase == PhaseAwaitAccept {
			a.Phase = PhaseOfferReceiver
		} else if a.Phase == PhaseAwaitAcknowledge {
			a.Phase = PhaseCommitReceiver
		}
		s.coordinator.workers[wid].enqueue(a.TransferID)
	}
}
func (s *simulation) reconcileAgent(w *SchedulerWorker, a *TransactionAgent) error {
	s.metrics.add(func(m *Metrics) { m.ReconcileEntriesExamined++ })
	s.recoveryMu.Lock()
	s.recoveryTouched[a.SenderBSO] = true
	s.recoveryTouched[a.ReceiverBSO] = true
	s.recoveryAgents[a.TransferID] = true
	s.recoveryMu.Unlock()
	sender, err := s.bsos[a.SenderBSO].load(s.ctx)
	if err != nil {
		return err
	}
	receiver, err := s.bsos[a.ReceiverBSO].load(s.ctx)
	if err != nil {
		return err
	}
	out := sender.Outgoing[a.TransferID]
	in := receiver.Incoming[a.TransferID]
	switch {
	case out.State == StateAcknowledged && in.State == StateCommitted:
		a.Phase = PhaseDone
		a.CompletedRound = s.round
	case out.State == StateCommitted && in.State == StateCommitted:
		a.Phase = PhaseAwaitAcknowledge
		s.transport.send(w.ID, a.PlacementGeneration, newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, MessageAcknowledge, StateCommitted))
	case out.State == StateCommitted:
		a.Phase = PhaseCommitReceiver
	case in.State == StateAccepted:
		a.Phase = PhaseCommitSender
	case out.State == StateReserved:
		a.Phase = PhaseOfferReceiver
	default:
		state, e := s.bsos[a.SenderBSO].expire(s.ctx, a.TransferID)
		if e != nil {
			return e
		}
		if state == StateExpired {
			a.Phase = PhaseExpired
		} else {
			a.Phase = PhaseRejected
		}
		a.CompletedRound = s.round
	}
	return nil
}

func (s *simulation) restartBSO(id string) error {
	b := s.bsos[id]
	if b == nil {
		return nil
	}
	if err := b.restart(s.ctx); err != nil {
		return err
	}
	ids, err := b.pending(s.ctx)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, tid := range ids {
		if seen[tid] {
			continue
		}
		seen[tid] = true
		a, wid, ok := s.coordinator.agent(tid)
		if !ok {
			continue
		}
		a.Phase = PhaseReconcile
		s.coordinator.workers[wid].enqueue(tid)
		s.recoveryMu.Lock()
		s.recoveryAgents[tid] = true
		s.recoveryMu.Unlock()
	}
	s.recoveryMu.Lock()
	s.recoveryTouched[id] = true
	s.recoveryMu.Unlock()
	return nil
}
func (s *simulation) allTerminal() bool {
	for _, a := range s.agents {
		if !a.Phase.terminal() {
			return false
		}
	}
	return true
}
func (s *simulation) unresolved() int {
	n := 0
	for _, a := range s.agents {
		if !a.Phase.terminal() {
			n++
		}
	}
	return n
}

func (s *simulation) result(attempts []Attempt, elapsed time.Duration) (Result, error) {
	metrics := s.metrics.snapshot()
	balances := []string{}
	states := []string{}
	total := Money(s.config.BSOs-len(s.bsos)) * s.config.InitialBalance
	debits, credits := map[string]int{}, map[string]int{}
	for _, id := range s.activeIDs {
		st, err := s.bsos[id].load(s.ctx)
		if err != nil {
			return Result{}, err
		}
		if st.Balance < 0 || st.Reserved < 0 {
			return Result{}, fmt.Errorf("negative value at %s", id)
		}
		total += st.Balance
		balances = append(balances, fmt.Sprintf("%s=%d", id, st.Balance))
		for _, x := range st.Audit {
			if x.Delta < 0 {
				debits[x.TransferID]++
			} else if x.Delta > 0 {
				credits[x.TransferID]++
			}
		}
	}
	for _, a := range s.agents {
		states = append(states, fmt.Sprintf("%s=%s", a.TransferID, a.Phase))
		switch a.Phase {
		case PhaseDone:
			metrics.Successful++
		case PhaseRejected, PhaseExpired:
			metrics.Rejected++
		default:
			metrics.Unresolved++
		}
		if debits[a.TransferID] > 1 {
			metrics.DoubleDebits += debits[a.TransferID] - 1
		}
		if credits[a.TransferID] > 1 {
			metrics.DoubleCredits += credits[a.TransferID] - 1
		}
	}
	sort.Strings(balances)
	sort.Strings(states)
	blob, _ := json.Marshal(struct{ Balances, States []string }{balances, states})
	sum := sha256.Sum256(blob)
	metrics.RecoveryBSOsTouched = len(s.recoveryTouched)
	metrics.RecoveryAgentsTouched = len(s.recoveryAgents)
	metrics.WorkerSteps = make([]int, len(s.coordinator.workers))
	metrics.WorkerPeakActive = make([]int, len(s.coordinator.workers))
	for i, w := range s.coordinator.workers {
		w.mu.Lock()
		steps, peak, queued := w.Steps, w.Peak, w.QueuePeak
		w.mu.Unlock()
		metrics.WorkerSteps[i] = steps
		metrics.WorkerPeakActive[i] = peak
		if queued > metrics.PeakQueuedAgents {
			metrics.PeakQueuedAgents = queued
		}
	}
	if metrics.Successful > 0 {
		n := float64(metrics.Successful)
		// This is the semantic participant set, not the number of participant
		// operations. Every bilateral transfer names exactly two financial
		// authorities regardless of retries or network population.
		metrics.ParticipantsTouched = metrics.Successful * 2
		metrics.MessagesPerSuccess = float64(metrics.MessagesSent) / n
		metrics.DurableMutationsPerSuccess = float64(metrics.LocalDurableMutations) / n
		metrics.CoordinatorOpsPerSuccess = float64(metrics.CoordinatorOps) / n
		metrics.WorkerStepsPerSuccess = float64(metrics.AgentSteps) / n
		metrics.ParticipantsPerSuccess = float64(metrics.ParticipantsTouched) / n
		metrics.ReconcileEntriesPerSuccess = float64(metrics.ReconcileEntriesExamined) / n
	}
	metrics.TransferCompletions = metrics.Successful + metrics.Rejected
	metrics.TransfersPerSecond = float64(len(attempts)) / elapsed.Seconds()
	initial := Money(s.config.BSOs) * s.config.InitialBalance
	correct := total == initial && metrics.Unresolved == 0 && metrics.DoubleDebits == 0 && metrics.DoubleCredits == 0 && metrics.SettlementBeforeTrust == 0
	metrics.JSONBaselineBytes = len(mustJSON(newEnvelope("transfer:00000000", "bso:000000", "bso:000001", 1, MessageOffer, StateReserved)))
	s.trustMu.Lock()
	resolutions := make([]TrustResolutionV1, 0, len(s.resolutions))
	for _, resolution := range s.resolutions {
		resolutions = append(resolutions, resolution)
	}
	s.trustMu.Unlock()
	sort.Slice(resolutions, func(i, j int) bool { return resolutions[i].TransferID < resolutions[j].TransferID })
	costs := make([]CoordinationCostV1, 0, len(s.agents))
	for _, agent := range s.agents {
		cost := CoordinationCostV1{TransferID: agent.TransferID, ProviderCalls: agent.TrustProvidersConsulted, ResolutionDurableTransitions: 1, LogicalRounds: agent.CompletedRound + 1}
		if agent.Phase == PhaseDone {
			cost.SettlementMessages = 4
			cost.FinancialDurableTransitions = 5
		}
		costs = append(costs, cost)
	}
	sort.Slice(costs, func(i, j int) bool { return costs[i].TransferID < costs[j].TransferID })
	return Result{Config: s.config, Metrics: metrics, InitialTotal: initial, FinalTotal: total, Conservation: total == initial, Correct: correct, CorrectnessDigest: hex.EncodeToString(sum[:]), Elapsed: elapsed, ElapsedMilliseconds: elapsed.Milliseconds(), TrustResolutions: resolutions, ProviderRetention: s.trustRegistry.retained(), CoordinationCosts: costs}, nil
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
