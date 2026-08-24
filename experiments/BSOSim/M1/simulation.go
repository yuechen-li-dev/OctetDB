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
	"sort"
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
	metrics         Metrics
	round           int
	recoveryTouched map[string]bool
}

func Run(ctx context.Context, c Config) (result Result, err error) {
	c, err = normalizeConfig(c)
	if err != nil {
		return result, err
	}
	root := c.DataDir
	cleanup := func() {}
	if root == "" {
		root, err = os.MkdirTemp("", "octetdb-bso-sim-m1-")
		if err != nil {
			return result, err
		}
		cleanup = func() { _ = os.RemoveAll(root) }
	}
	defer cleanup()
	s := &simulation{ctx: ctx, config: c, root: filepath.Join(root, "bso"), bsos: map[string]*durableBSO{}, agents: map[string]*TransactionAgent{}, recoveryTouched: map[string]bool{}}
	s.coordinator = newCoordinator(c.Workers, &s.metrics)
	s.transport = newTransport(c.Seed+17, c.Faults, &s.metrics)
	defer func() {
		for _, b := range s.bsos {
			if e := b.close(); err == nil && e != nil {
				err = e
			}
		}
	}()
	attempts := GenerateWorkload(c)
	s.metrics.Attempted = len(attempts)
	start := time.Now()
	for _, a := range attempts {
		if _, e := s.activate(a.From); e != nil {
			return result, e
		}
		if _, e := s.activate(a.To); e != nil {
			return result, e
		}
		agent := &TransactionAgent{ProtocolVersion: 1, TransferID: a.ID, SenderBSO: a.From, ReceiverBSO: a.To, Amount: a.Amount, Phase: PhaseCreated}
		s.agents[a.ID] = agent
		if e := s.coordinator.assign(agent); e != nil {
			return result, e
		}
	}
	s.metrics.PeakActiveAgents = len(s.agents)
	for round := 0; round < c.MaxRounds; round++ {
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
		if e := s.transport.drain(s.deliver); e != nil {
			return result, e
		}
		if s.allTerminal() {
			break
		}
		s.wakeDeadlines()
	}
	if !s.allTerminal() {
		return result, fmt.Errorf("%d agents unresolved after %d rounds", s.unresolved(), c.MaxRounds)
	}
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
		a = append(a, Attempt{ID: fmt.Sprintf("transfer:%08d", i), From: bsoID(from), To: bsoID(to), Amount: amount})
	}
	return a
}
func bsoID(i int) string { return fmt.Sprintf("bso:%06d", i) }
func (s *simulation) activate(id string) (*durableBSO, error) {
	if b := s.bsos[id]; b != nil {
		return b, nil
	}
	b, err := openBSO(s.ctx, s.root, id, s.config.InitialBalance, &s.metrics)
	if err != nil {
		return nil, err
	}
	s.bsos[id] = b
	s.activeIDs = append(s.activeIDs, id)
	sort.Strings(s.activeIDs)
	s.metrics.OpenBSODatabases = len(s.bsos)
	return b, nil
}

func (s *simulation) runWorkers() error {
	for _, w := range s.coordinator.workers {
		budget := len(w.queue)
		for i := 0; i < budget; i++ {
			id, ok := w.pop()
			if !ok {
				break
			}
			a := w.agents[id]
			if a == nil || a.Phase.terminal() {
				continue
			}
			w.Steps++
			s.metrics.AgentSteps++
			if err := s.step(w, a); err != nil {
				return err
			}
			if a.Phase.terminal() {
				s.coordinator.complete(w.ID, id)
			} else if a.Phase != PhaseAwaitAccept && a.Phase != PhaseAwaitAcknowledge {
				w.enqueue(id)
			}
		}
	}
	return nil
}
func (s *simulation) step(w *SchedulerWorker, a *TransactionAgent) error {
	sender := s.bsos[a.SenderBSO]
	switch a.Phase {
	case PhaseCreated:
		state, err := sender.reserve(s.ctx, Attempt{ID: a.TransferID, From: a.SenderBSO, To: a.ReceiverBSO, Amount: a.Amount}, s.round)
		if err != nil {
			return err
		}
		a.LastObservedSenderVersion++
		if state == StateRejected {
			a.Phase = PhaseRejected
			return nil
		}
		a.Phase = PhaseOfferReceiver
	case PhaseOfferReceiver:
		s.transport.send(newEnvelope(a.TransferID, a.SenderBSO, a.ReceiverBSO, a.Amount, MessageOffer, StateReserved))
		a.LastMessageKind = MessageOffer
		a.Phase = PhaseAwaitAccept
		a.NextLogicalDeadline = s.round + s.config.RetryDelay
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
		s.transport.send(newEnvelope(a.TransferID, a.SenderBSO, a.ReceiverBSO, a.Amount, MessageCommit, StateCommitted))
		a.LastMessageKind = MessageCommit
		a.Phase = PhaseAwaitAcknowledge
		a.NextLogicalDeadline = s.round + s.config.RetryDelay
	case PhaseReconcile:
		return s.reconcileAgent(w, a)
	}
	return nil
}

func (s *simulation) deliver(e ProtocolEnvelopeV1) error {
	if err := validateEnvelope(e); err != nil {
		s.metrics.AuthenticationFailures++
		return nil
	}
	a, wid, ok := s.coordinator.agent(e.TransferID)
	if !ok {
		return nil
	}
	receiver := s.bsos[e.To]
	if receiver == nil {
		return nil
	}
	s.metrics.ParticipantsTouched++
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
		s.transport.send(newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, reply, state))
	case MessageAccept:
		if a.Phase == PhaseAwaitAccept {
			a.Phase = PhaseCommitSender
			s.coordinator.workers[wid].enqueue(a.TransferID)
		}
	case MessageReject:
		a.Phase = PhaseRejected
		s.coordinator.complete(wid, a.TransferID)
	case MessageCommit:
		state, err := receiver.commitReceiver(s.ctx, e)
		if err != nil {
			return err
		}
		a.LastObservedReceiverVersion++
		if state == StateCommitted {
			s.transport.send(newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, MessageAcknowledge, state))
		}
	case MessageAcknowledge:
		state, err := receiver.ackSender(s.ctx, e)
		if err != nil {
			return err
		}
		a.LastObservedSenderVersion++
		if state == StateAcknowledged {
			a.Phase = PhaseDone
			s.coordinator.complete(wid, a.TransferID)
		}
	}
	return nil
}

func (s *simulation) wakeDeadlines() {
	for _, a := range s.agents {
		if a.Phase.terminal() || a.NextLogicalDeadline > s.round+1 {
			continue
		}
		_, wid, ok := s.coordinator.agent(a.TransferID)
		if !ok {
			continue
		}
		a.RetryCount++
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
	s.metrics.ReconcileEntriesExamined++
	s.metrics.RecoveryAgentsTouched++
	s.recoveryTouched[a.SenderBSO] = true
	s.recoveryTouched[a.ReceiverBSO] = true
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
	case out.State == StateCommitted && in.State == StateCommitted:
		a.Phase = PhaseAwaitAcknowledge
		s.transport.send(newEnvelope(a.TransferID, a.ReceiverBSO, a.SenderBSO, a.Amount, MessageAcknowledge, StateCommitted))
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
		s.metrics.RecoveryAgentsTouched++
	}
	s.recoveryTouched[id] = true
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
			s.metrics.Successful++
		case PhaseRejected, PhaseExpired:
			s.metrics.Rejected++
		default:
			s.metrics.Unresolved++
		}
		if debits[a.TransferID] > 1 {
			s.metrics.DoubleDebits += debits[a.TransferID] - 1
		}
		if credits[a.TransferID] > 1 {
			s.metrics.DoubleCredits += credits[a.TransferID] - 1
		}
	}
	sort.Strings(balances)
	sort.Strings(states)
	blob, _ := json.Marshal(struct{ Balances, States []string }{balances, states})
	sum := sha256.Sum256(blob)
	s.metrics.RecoveryBSOsTouched = len(s.recoveryTouched)
	s.metrics.WorkerSteps = make([]int, len(s.coordinator.workers))
	s.metrics.WorkerPeakActive = make([]int, len(s.coordinator.workers))
	for i, w := range s.coordinator.workers {
		s.metrics.WorkerSteps[i] = w.Steps
		s.metrics.WorkerPeakActive[i] = w.Peak
		if len(w.queue) > s.metrics.PeakQueuedAgents {
			s.metrics.PeakQueuedAgents = len(w.queue)
		}
	}
	if s.metrics.Successful > 0 {
		n := float64(s.metrics.Successful)
		// This is the semantic participant set, not the number of participant
		// operations. Every bilateral transfer names exactly two financial
		// authorities regardless of retries or network population.
		s.metrics.ParticipantsTouched = s.metrics.Successful * 2
		s.metrics.MessagesPerSuccess = float64(s.metrics.MessagesSent) / n
		s.metrics.DurableMutationsPerSuccess = float64(s.metrics.LocalDurableMutations) / n
		s.metrics.CoordinatorOpsPerSuccess = float64(s.metrics.CoordinatorOps) / n
		s.metrics.WorkerStepsPerSuccess = float64(s.metrics.AgentSteps) / n
		s.metrics.ParticipantsPerSuccess = float64(s.metrics.ParticipantsTouched) / n
		s.metrics.ReconcileEntriesPerSuccess = float64(s.metrics.ReconcileEntriesExamined) / n
	}
	s.metrics.TransfersPerSecond = float64(len(attempts)) / elapsed.Seconds()
	initial := Money(s.config.BSOs) * s.config.InitialBalance
	correct := total == initial && s.metrics.Unresolved == 0 && s.metrics.DoubleDebits == 0 && s.metrics.DoubleCredits == 0
	s.metrics.JSONBaselineBytes = len(mustJSON(newEnvelope("transfer:00000000", "bso:000000", "bso:000001", 1, MessageOffer, StateReserved)))
	return Result{Config: s.config, Metrics: s.metrics, InitialTotal: initial, FinalTotal: total, Conservation: total == initial, Correct: correct, CorrectnessDigest: hex.EncodeToString(sum[:]), Elapsed: elapsed, ElapsedMilliseconds: elapsed.Milliseconds()}, nil
}
func mustJSON(v any) []byte { b, _ := json.Marshal(v); return b }
