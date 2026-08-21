package scheduled

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/yuechen-li-dev/database-scheduler/internal/workload"
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
	controlUtility
	controlAgentic
)

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
	ConflictsDetected    int64         `json:"conflicts_detected"`
	RequestsBlocked      int64         `json:"requests_blocked"`
	ConflictWaitTotal    time.Duration `json:"conflict_wait_total"`
	ConflictWaitMax      time.Duration `json:"conflict_wait_max"`
	OwnershipTotal       time.Duration `json:"ownership_total"`
	OwnershipMax         time.Duration `json:"ownership_max"`
	PeakHotKeyQueueDepth int64         `json:"peak_hot_key_queue_depth"`
	Wakeups              int64         `json:"wakeups"`
	Messages             int64         `json:"messages"`
	PeakMailboxOccupancy int64         `json:"peak_mailbox_occupancy"`
	MailboxOverflows     int64         `json:"mailbox_overflows"`
	UtilityDecisions     int64         `json:"utility_decisions"`
	UtilityCandidates    int64         `json:"utility_candidates"`
	StarvationPromotions int64         `json:"starvation_promotions"`
	OwnershipLeaks       int64         `json:"ownership_leaks"`
	DoubleReleases       int64         `json:"double_releases"`
}

type conflictCounters struct {
	conflicts, blocked, waitTotal, waitMax, ownTotal, ownMax atomic.Int64
	peakHot, wakeups, messages, peakMailbox, overflow        atomic.Int64
	utilityDecisions, utilityCandidates, promotions          atomic.Int64
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
	}
}

type TraceEvent struct {
	RequestID int64  `json:"request_id"`
	Event     string `json:"event"`
	Reason    string `json:"reason"`
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
	tickEvery := s.batchWait / 2
	if tickEvery <= 0 || tickEvery > time.Millisecond {
		tickEvery = time.Millisecond
	}
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()

	dispatchReady := func(now time.Time) {
		for active < s.workers {
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
			s.jobs <- batch{requests: group}
		}
	}

	for {
		select {
		case r := <-s.input:
			r.token, _ = conflictToken(s.plan, r.op)
			if s.strategy == controlAgentic {
				r.agent = newRequestAgent()
			}
			pending = append(pending, r)
			sort.SliceStable(pending, func(i, j int) bool { return pending[i].op.Sequence < pending[j].op.Sequence })
			dispatchReady(time.Now())
		case done := <-s.completions:
			active--
			now := done.finished
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
				r.done <- Result{Err: done.err, QueueTime: done.started.Sub(r.queuedAt), Service: done.finished.Sub(done.started), ConflictWait: r.conflictWait, BatchSize: len(done.job.requests)}
				s.used.Add(-1)
			}
			dispatchReady(now)
		case now := <-ticker.C:
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
	best := -1
	bestScore := -1
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
			if batchCount < s.maxBatch && age < s.batchWait && s.strategy != controlUtility {
				continue
			}
		}
		score := 0
		if s.strategy == controlUtility {
			d, _ := s.plan.command(r.op.Kind)
			started := time.Now()
			action := utilityDecision(true, d.Priority, int(age/time.Microsecond), batchCount, s.maxBatch)
			s.policyNanos.Add(int64(time.Since(started)))
			s.conflicts.utilityDecisions.Add(1)
			s.conflicts.utilityCandidates.Add(4)
			if action == utilityDefer {
				continue
			}
			score = d.Priority*1_000_000 + int(age/time.Microsecond)
			if action == utilityPromote {
				score += 2_000_000
				s.conflicts.promotions.Add(1)
			}
			s.trace(r, "UtilityDecision", fmt.Sprintf("action=%d priority=%d age_us=%d batch=%d score=%d", action, d.Priority, age/time.Microsecond, batchCount, score))
		} else {
			score = int(age / time.Microsecond)
		}
		if best < 0 || score > bestScore || (score == bestScore && r.op.Sequence < pending[best].op.Sequence) {
			best, bestScore = i, score
		}
	}
	return best
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
