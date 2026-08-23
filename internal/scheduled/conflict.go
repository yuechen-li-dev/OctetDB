package scheduled

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/yuechen-li-dev/octetdb/internal/workload"
)

type ConflictDomain uint8

const (
	ConflictNone ConflictDomain = iota
	ConflictOrders
	ConflictInventory
)

type ConflictToken struct {
	Domain ConflictDomain `json:"domain"`
	Key    int64          `json:"key"`
}

func (t ConflictToken) String() string { return fmt.Sprintf("%d:%d", t.Domain, t.Key) }
func (t ConflictToken) valid() bool    { return t.Domain != ConflictNone }

// conflictToken projects a runtime identity through the command's statically
// generated conflict class. Reads remain non-owning under PostgreSQL MVCC;
// write commands own exactly one token, preventing cycles by construction.
func conflictToken(plan planLookup, op workload.Operation) (ConflictToken, bool) {
	d, ok := plan.command(op.Kind)
	if !ok {
		return ConflictToken{}, false
	}
	if d.Transaction == 0 {
		return ConflictToken{}, false
	}
	switch d.Conflict {
	case 1:
		return ConflictToken{Domain: ConflictOrders, Key: op.CustomerID}, op.Kind == workload.OrderWrite
	case 2:
		return ConflictToken{Domain: ConflictInventory, Key: op.ProductID}, op.Kind == workload.InventoryWrite
	default:
		return ConflictToken{}, false
	}
}

type controlStrategy uint8

const (
	controlNone controlStrategy = iota
	controlCentralized
	controlParity
	controlPriority
	controlPlantCharacterization
	controlObserverShadow
	controlReactive
	controlUtility
	controlAgentic
)

const (
	actionOldest = iota
	actionHighPriority
	actionAged
	actionBatch
	actionWait
)

const (
	policyHysteresis = 25
	policyMinCommit  = 2
)

type conventionalPolicy struct {
	hasCurrent bool
	current    int
	score      int
	commitAge  int
}

type RequestState uint8

const (
	StateCreated RequestState = iota
	StateAdmitted
	StateReady
	StateWaitingConflict
	StateDispatched
	StateRunning
	StateCompleted
	StateFailed
)

type MessageKind uint8

const (
	MessageWake MessageKind = iota
	MessageCompletion
)

type AgentMessage struct {
	Kind   MessageKind
	Sender int64
}

type ActuatorKind uint8

const (
	ActuatorAcquireConflict ActuatorKind = iota
	ActuatorDispatch
	ActuatorReleaseConflict
	ActuatorComplete
)

const mailboxCapacity = 2

type requestAgent struct {
	state      RequestState
	history    [8]RequestState
	historyLen uint8
	mailbox    [mailboxCapacity]AgentMessage
	mailboxLen uint8
}

func newRequestAgent() requestAgent {
	a := requestAgent{}
	a.transition(StateCreated)
	a.transition(StateAdmitted)
	a.transition(StateReady)
	return a
}

func (a *requestAgent) transition(next RequestState) {
	a.state = next
	if int(a.historyLen) < len(a.history) {
		a.history[a.historyLen] = next
		a.historyLen++
	}
}

func (a *requestAgent) send(message AgentMessage) bool {
	if int(a.mailboxLen) >= len(a.mailbox) {
		return false
	}
	a.mailbox[a.mailboxLen] = message
	a.mailboxLen++
	return true
}

func (a *requestAgent) receive() (AgentMessage, bool) {
	if a.mailboxLen == 0 {
		return AgentMessage{}, false
	}
	m := a.mailbox[0]
	copy(a.mailbox[:], a.mailbox[1:a.mailboxLen])
	a.mailboxLen--
	return m, true
}

type ConflictMetrics struct {
	ConflictsDetected       int64         `json:"conflicts_detected"`
	RequestsBlocked         int64         `json:"requests_blocked"`
	ConflictWaitTotal       time.Duration `json:"conflict_wait_total"`
	ConflictWaitMax         time.Duration `json:"conflict_wait_max"`
	OwnershipTotal          time.Duration `json:"ownership_total"`
	OwnershipMax            time.Duration `json:"ownership_max"`
	PeakHotKeyQueueDepth    int64         `json:"peak_hot_key_queue_depth"`
	Wakeups                 int64         `json:"wakeups"`
	Messages                int64         `json:"messages"`
	PeakMailboxOccupancy    int64         `json:"peak_mailbox_occupancy"`
	MailboxOverflows        int64         `json:"mailbox_overflows"`
	UtilityDecisions        int64         `json:"utility_decisions"`
	UtilityCandidates       int64         `json:"utility_candidates"`
	StarvationPromotions    int64         `json:"starvation_promotions"`
	ControllerConstructions int64         `json:"controller_constructions"`
	PolicySwitches          int64         `json:"policy_switches"`
	HysteresisHolds         int64         `json:"hysteresis_holds"`
	MinimumCommitHolds      int64         `json:"minimum_commit_holds"`
	FairnessOverrides       int64         `json:"fairness_overrides"`
	Overtakes               int64         `json:"overtakes"`
	MaximumDispatchAge      time.Duration `json:"maximum_dispatch_age"`
	OwnershipLeaks          int64         `json:"ownership_leaks"`
	DoubleReleases          int64         `json:"double_releases"`
}

type conflictCounters struct {
	conflicts, blocked, waitTotal, waitMax, ownTotal, ownMax atomic.Int64
	peakHot, wakeups, messages, peakMailbox, overflow        atomic.Int64
	utilityDecisions, utilityCandidates, promotions          atomic.Int64
	controllerConstructions, policySwitches                  atomic.Int64
	hysteresisHolds, minimumCommitHolds, fairnessOverrides   atomic.Int64
	overtakes, maximumDispatchAge                            atomic.Int64
	leaks, doubleReleases                                    atomic.Int64
}

func updateMax(target *atomic.Int64, value int64) {
	for old := target.Load(); value > old && !target.CompareAndSwap(old, value); old = target.Load() {
	}
}

func (c *conflictCounters) snapshot() ConflictMetrics {
	return ConflictMetrics{
		ConflictsDetected: c.conflicts.Load(), RequestsBlocked: c.blocked.Load(),
		ConflictWaitTotal: time.Duration(c.waitTotal.Load()), ConflictWaitMax: time.Duration(c.waitMax.Load()),
		OwnershipTotal: time.Duration(c.ownTotal.Load()), OwnershipMax: time.Duration(c.ownMax.Load()),
		PeakHotKeyQueueDepth: c.peakHot.Load(), Wakeups: c.wakeups.Load(), Messages: c.messages.Load(),
		PeakMailboxOccupancy: c.peakMailbox.Load(), MailboxOverflows: c.overflow.Load(),
		UtilityDecisions: c.utilityDecisions.Load(), UtilityCandidates: c.utilityCandidates.Load(),
		StarvationPromotions: c.promotions.Load(), OwnershipLeaks: c.leaks.Load(), DoubleReleases: c.doubleReleases.Load(),
		ControllerConstructions: c.controllerConstructions.Load(), PolicySwitches: c.policySwitches.Load(),
		HysteresisHolds: c.hysteresisHolds.Load(), MinimumCommitHolds: c.minimumCommitHolds.Load(),
		FairnessOverrides: c.fairnessOverrides.Load(), Overtakes: c.overtakes.Load(),
		MaximumDispatchAge: time.Duration(c.maximumDispatchAge.Load()),
	}
}

type TraceEvent struct {
	RequestID int64  `json:"request_id"`
	Event     string `json:"event"`
	Reason    string `json:"reason"`
}

// PlantEvent keeps physical scheduler facts separate from later observer
// estimates and predictions. It is populated only when plant tracing is
// explicitly enabled and is bounded to prevent diagnostics from becoming an
// unbounded workload artifact.
type PlantEvent struct {
	AtNS            int64  `json:"at_ns"`
	Kind            string `json:"kind"`
	RequestID       int64  `json:"request_id,omitempty"`
	CommandKind     string `json:"command_kind,omitempty"`
	Pending         int    `json:"pending"`
	Active          int    `json:"active"`
	Admitted        int64  `json:"admitted"`
	BatchSize       int    `json:"batch_size,omitempty"`
	QueuedAtNS      int64  `json:"queued_at_ns,omitempty"`
	DispatchedAtNS  int64  `json:"dispatched_at_ns,omitempty"`
	StartedAtNS     int64  `json:"started_at_ns,omitempty"`
	CompletedAtNS   int64  `json:"completed_at_ns,omitempty"`
	QueueNS         int64  `json:"queue_ns,omitempty"`
	ServiceNS       int64  `json:"service_ns,omitempty"`
	DispatchDelayNS int64  `json:"dispatch_to_completion_ns,omitempty"`
}

func (s *Scheduler) recordPlant(event PlantEvent) {
	s.plantMu.Lock()
	defer s.plantMu.Unlock()
	if len(s.plantEvents) < 65536 {
		s.plantEvents = append(s.plantEvents, event)
	}
}

type owner struct {
	request    *request
	acquiredAt time.Time
}

type completion struct {
	job               batch
	err               error
	started, finished time.Time
}

func (s *Scheduler) conflictDispatch() {
	defer s.wg.Done()
	defer close(s.jobs)
	pending := make([]*request, 0, s.capacity)
	owned := make(map[ConflictToken]owner, s.workers)
	active := 0
	exposure := s.workers
	arrivalsSinceTick, completionsSinceTick := 0, 0
	dispatchDelaySinceTick := int64(0)
	var observer observerRuntime
	observerActive := s.strategy == controlObserverShadow || s.strategy == controlReactive
	tickEvery := s.batchWait / 2
	if tickEvery <= 0 || tickEvery > time.Millisecond {
		tickEvery = time.Millisecond
	}
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()

	dispatchReady := func(now time.Time) {
		for active < exposure {
			index := s.selectRequest(pending, owned, now)
			if index < 0 {
				return
			}
			first := pending[index]
			pending = append(pending[:index], pending[index+1:]...)
			group := []*request{first}
			if first.op.Kind <= workload.RangeRead {
				for i := 0; i < len(pending) && len(group) < s.maxBatch; {
					if s.plan.compatible(first.op.Kind, pending[i].op.Kind) {
						group = append(group, pending[i])
						pending = append(pending[:i], pending[i+1:]...)
						continue
					}
					i++
				}
			}
			if first.token.valid() {
				owned[first.token] = owner{request: first, acquiredAt: now}
				s.trace(first, "ConflictAcquired", first.token.String())
			}
			for _, r := range group {
				if r.waitingSince.IsZero() == false {
					wait := now.Sub(r.waitingSince)
					r.conflictWait = wait
					s.conflicts.waitTotal.Add(int64(wait))
					updateMax(&s.conflicts.waitMax, int64(wait))
				}
				if s.strategy == controlAgentic {
					r.agent.transition(StateDispatched)
					r.agent.transition(StateRunning)
				}
				s.trace(r, "Dispatch", fmt.Sprintf("batch=%d", len(group)))
			}
			active++
			if s.strategy == controlPlantCharacterization {
				s.recordPlant(PlantEvent{AtNS: now.UnixNano(), Kind: "dispatch", RequestID: first.op.Sequence, CommandKind: first.op.Kind.String(), Pending: len(pending), Active: active, Admitted: s.used.Load(), BatchSize: len(group), QueuedAtNS: first.queuedAt.UnixNano(), DispatchedAtNS: now.UnixNano()})
			}
			s.jobs <- batch{requests: group, dispatched: now, pending: len(pending), active: active}
		}
	}

	for {
		select {
		case r := <-s.input:
			if observerActive {
				arrivalsSinceTick++
			}
			r.token, _ = conflictToken(s.plan, r.op)
			if s.strategy == controlAgentic {
				r.agent = newRequestAgent()
			}
			pending = append(pending, r)
			sort.SliceStable(pending, func(i, j int) bool { return pending[i].op.Sequence < pending[j].op.Sequence })
			now := time.Now()
			if s.strategy == controlPlantCharacterization {
				s.recordPlant(PlantEvent{AtNS: now.UnixNano(), Kind: "arrival", RequestID: r.op.Sequence, CommandKind: r.op.Kind.String(), Pending: len(pending), Active: active, Admitted: s.used.Load(), QueuedAtNS: r.queuedAt.UnixNano()})
			}
			dispatchReady(now)
		case done := <-s.completions:
			if observerActive {
				completionsSinceTick += len(done.job.requests)
				dispatchDelaySinceTick += done.finished.Sub(done.job.dispatched).Microseconds() * int64(len(done.job.requests))
			}
			active--
			now := done.finished
			if s.strategy == controlPlantCharacterization {
				s.recordPlant(PlantEvent{AtNS: now.UnixNano(), Kind: "completion", RequestID: done.job.requests[0].op.Sequence, CommandKind: done.job.requests[0].op.Kind.String(), Pending: len(pending), Active: active, Admitted: s.used.Load(), BatchSize: len(done.job.requests), QueuedAtNS: done.job.requests[0].queuedAt.UnixNano(), DispatchedAtNS: done.job.dispatched.UnixNano(), StartedAtNS: done.started.UnixNano(), CompletedAtNS: done.finished.UnixNano(), QueueNS: done.started.Sub(done.job.requests[0].queuedAt).Nanoseconds(), ServiceNS: done.finished.Sub(done.started).Nanoseconds(), DispatchDelayNS: done.finished.Sub(done.job.dispatched).Nanoseconds()})
			}
			for _, r := range done.job.requests {
				if r.token.valid() {
					o, ok := owned[r.token]
					if !ok || o.request != r {
						s.conflicts.doubleReleases.Add(1)
					} else {
						held := now.Sub(o.acquiredAt)
						s.conflicts.ownTotal.Add(int64(held))
						updateMax(&s.conflicts.ownMax, int64(held))
						delete(owned, r.token)
						s.trace(r, "ConflictReleased", r.token.String())
						s.wakeNext(pending, r)
					}
				}
				if s.strategy == controlAgentic {
					if !r.agent.send(AgentMessage{Kind: MessageCompletion, Sender: r.op.Sequence}) {
						s.conflicts.overflow.Add(1)
					} else {
						s.conflicts.messages.Add(1)
						updateMax(&s.conflicts.peakMailbox, int64(r.agent.mailboxLen))
						_, _ = r.agent.receive()
						if done.err != nil {
							r.agent.transition(StateFailed)
						} else {
							r.agent.transition(StateCompleted)
						}
					}
				}
				d, _ := s.plan.command(r.op.Kind)
				r.done <- Result{Err: done.err, QueueTime: done.started.Sub(r.queuedAt), Service: done.finished.Sub(done.started), ConflictWait: r.conflictWait, BatchSize: len(done.job.requests), Priority: d.Priority}
				s.used.Add(-1)
			}
			dispatchReady(now)
		case now := <-ticker.C:
			if observerActive {
				meanDelay := 0
				if completionsSinceTick > 0 {
					meanDelay = int(dispatchDelaySinceTick / int64(completionsSinceTick))
				}
				event := observer.update(len(pending), arrivalsSinceTick, completionsSinceTick, meanDelay)
				event.AtNS = now.UnixNano()
				if s.strategy == controlReactive {
					previous := exposure
					exposure = reactiveExposure(exposure, s.workers, event.PersistenceDelayMicros)
					observer.recordControl(exposure, s.workers, exposure != previous)
				}
				s.recordObserver(event, observer.metrics())
			}
			if observerActive {
				arrivalsSinceTick, completionsSinceTick, dispatchDelaySinceTick = 0, 0, 0
			}
			dispatchReady(now)
		case <-s.closed:
			if len(owned) != 0 {
				s.conflicts.leaks.Add(int64(len(owned)))
			}
			return
		}
	}
}

func (s *Scheduler) selectRequest(pending []*request, owned map[ConflictToken]owner, now time.Time) int {
	type candidate struct {
		index, priority, batchCount int
		age                         time.Duration
	}
	legal := make([]candidate, 0, len(pending))
	for i, r := range pending {
		if r.token.valid() {
			if o, busy := owned[r.token]; busy {
				if r.waitingSince.IsZero() {
					s.conflicts.conflicts.Add(1)
					r.waitingSince = now
					s.conflicts.blocked.Add(1)
					if s.strategy == controlAgentic {
						r.agent.transition(StateWaitingConflict)
					}
					s.trace(r, "WaitingConflict", fmt.Sprintf("token=%s owner=%d", r.token, o.request.op.Sequence))
				}
				depth := int64(0)
				for _, queued := range pending {
					if queued.token == r.token {
						depth++
					}
				}
				updateMax(&s.conflicts.peakHot, depth)
				continue
			}
		}
		age := now.Sub(r.queuedAt)
		batchCount := 1
		if r.op.Kind <= workload.RangeRead {
			for j, other := range pending {
				if i != j && s.plan.compatible(r.op.Kind, other.op.Kind) {
					batchCount++
				}
			}
			if batchCount < s.maxBatch && age < s.batchWait {
				continue
			}
		}
		d, _ := s.plan.command(r.op.Kind)
		legal = append(legal, candidate{index: i, priority: d.Priority, batchCount: batchCount, age: age})
	}
	if len(legal) == 0 {
		return -1
	}
	oldest := legal[0]
	if s.strategy == controlCentralized || s.strategy == controlAgentic {
		return oldest.index
	}
	if s.strategy == controlParity {
		started := time.Now()
		s.parityPolicy.board.OldestEligible = true
		s.parityPolicy.__octStep()
		s.policyNanos.Add(int64(time.Since(started)))
		s.conflicts.utilityDecisions.Add(1)
		s.conflicts.utilityCandidates.Add(1)
		if s.parityPolicy.board.Chosen != actionOldest {
			return -1
		}
		return oldest.index
	}

	starvationThreshold := 5 * s.batchWait
	if starvationThreshold < 5*time.Millisecond {
		starvationThreshold = 5 * time.Millisecond
	}
	high := legal[0]
	batchCandidate := candidate{index: -1}
	aged := candidate{index: -1}
	for _, c := range legal {
		if c.priority > high.priority || (c.priority == high.priority && (c.age > high.age || (c.age == high.age && pending[c.index].op.Sequence < pending[high.index].op.Sequence))) {
			high = c
		}
		if c.batchCount > 1 && (batchCandidate.index < 0 || c.batchCount > batchCandidate.batchCount || (c.batchCount == batchCandidate.batchCount && c.age > batchCandidate.age)) {
			batchCandidate = c
		}
		if c.age >= starvationThreshold && (aged.index < 0 || c.age > aged.age || (c.age == aged.age && pending[c.index].op.Sequence < pending[aged.index].op.Sequence)) {
			aged = c
		}
	}
	ageUnits := func(age time.Duration) int {
		units := int(age / (100 * time.Microsecond))
		if units > 100 {
			return 100
		}
		return units
	}
	oldestScore := 100 + ageUnits(oldest.age)
	highScore := 300 + ageUnits(high.age)
	batchScore := 0
	if batchCandidate.index >= 0 {
		batchScore = 200 + 25*batchCandidate.batchCount
	}
	rawAction, rawScore := actionOldest, oldestScore
	if highScore > rawScore {
		rawAction, rawScore = actionHighPriority, highScore
	}
	if batchCandidate.index >= 0 && batchScore > rawScore {
		rawAction, rawScore = actionBatch, batchScore
	}
	if aged.index >= 0 {
		rawAction, rawScore = actionAged, 1_000
	}

	started := time.Now()
	chosen := rawAction
	if s.strategy == controlPriority || s.strategy == controlPlantCharacterization || s.strategy == controlObserverShadow || s.strategy == controlReactive {
		chosen = s.goFairPolicy.selectAction(rawAction, rawScore, aged.index >= 0, func(action int) bool {
			return action == actionOldest || action == actionHighPriority || (action == actionBatch && batchCandidate.index >= 0)
		}, &s.conflicts)
	} else {
		previous, previousScore, commitAge := s.fairPolicy.utilitySite1.Current, s.fairPolicy.utilitySite1.Score, s.fairPolicy.utilitySite1.CommitAge
		hadPrevious := s.fairPolicy.utilitySite1.HasCurrent
		s.fairPolicy.board.OldestEligible = true
		s.fairPolicy.board.HighEligible = true
		s.fairPolicy.board.AgedEligible = aged.index >= 0
		s.fairPolicy.board.BatchEligible = batchCandidate.index >= 0
		s.fairPolicy.board.OldestScore = oldestScore
		s.fairPolicy.board.HighScore = highScore
		s.fairPolicy.board.BatchScore = batchScore
		s.fairPolicy.__octStep()
		chosen = s.fairPolicy.board.Chosen
		if aged.index >= 0 {
			s.conflicts.fairnessOverrides.Add(1)
		} else if hadPrevious && rawAction != previous && chosen == previous {
			if commitAge < policyMinCommit {
				s.conflicts.minimumCommitHolds.Add(1)
			} else if rawScore <= previousScore+policyHysteresis {
				s.conflicts.hysteresisHolds.Add(1)
			}
		}
		if hadPrevious && chosen != previous {
			s.conflicts.policySwitches.Add(1)
		}
	}
	s.policyNanos.Add(int64(time.Since(started)))
	s.conflicts.utilityDecisions.Add(1)
	s.conflicts.utilityCandidates.Add(4)
	selected := oldest
	switch chosen {
	case actionHighPriority:
		selected = high
	case actionAged:
		selected = aged
		s.conflicts.promotions.Add(1)
	case actionBatch:
		selected = batchCandidate
	case actionWait:
		return -1
	}
	if selected.index < 0 {
		return -1
	}
	if selected.index != oldest.index {
		s.conflicts.overtakes.Add(int64(selected.index - oldest.index))
	}
	updateMax(&s.conflicts.maximumDispatchAge, int64(selected.age))
	if s.traceEnabled.Load() {
		s.trace(pending[selected.index], "PolicyDecision", fmt.Sprintf("eligible=%d action=%d raw=%d scores(oldest=%d high=%d batch=%d) previous_persistent=true age_us=%d", len(legal), chosen, rawAction, oldestScore, highScore, batchScore, selected.age/time.Microsecond))
	}
	return selected.index
}

func (p *conventionalPolicy) selectAction(rawAction, rawScore int, emergency bool, eligible func(int) bool, counters *conflictCounters) int {
	if emergency {
		counters.fairnessOverrides.Add(1)
		return actionAged
	}
	chosen, score := rawAction, rawScore
	if p.hasCurrent && eligible(p.current) && (p.commitAge < policyMinCommit || rawScore <= p.score+policyHysteresis) {
		chosen, score = p.current, p.score
		if rawAction != p.current {
			if p.commitAge < policyMinCommit {
				counters.minimumCommitHolds.Add(1)
			} else {
				counters.hysteresisHolds.Add(1)
			}
		}
	}
	if !p.hasCurrent || chosen != p.current {
		if p.hasCurrent {
			counters.policySwitches.Add(1)
		}
		p.current, p.score, p.commitAge, p.hasCurrent = chosen, score, 1, true
	} else {
		p.score = score
		p.commitAge++
	}
	return chosen
}

func (s *Scheduler) wakeNext(pending []*request, sender *request) {
	if s.strategy != controlAgentic {
		return
	}
	for _, r := range pending {
		if r.token == sender.token && r.agent.state == StateWaitingConflict {
			if !r.agent.send(AgentMessage{Kind: MessageWake, Sender: sender.op.Sequence}) {
				s.conflicts.overflow.Add(1)
				return
			}
			s.conflicts.messages.Add(1)
			s.conflicts.wakeups.Add(1)
			updateMax(&s.conflicts.peakMailbox, int64(r.agent.mailboxLen))
			_, _ = r.agent.receive()
			r.agent.transition(StateReady)
			s.trace(r, "Wake", fmt.Sprintf("released_by=%d", sender.op.Sequence))
			return
		}
	}
}
