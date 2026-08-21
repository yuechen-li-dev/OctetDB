package scheduled

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yuechen-li-dev/database-scheduler/internal/db"
	"github.com/yuechen-li-dev/database-scheduler/internal/workload"
)

var ErrRejected = errors.New("scheduler capacity exhausted")

type Result struct {
	Err          error
	QueueTime    time.Duration
	Service      time.Duration
	ConflictWait time.Duration
	BatchSize    int
}

type request struct {
	ctx          context.Context
	op           workload.Operation
	queuedAt     time.Time
	done         chan Result
	token        ConflictToken
	waitingSince time.Time
	conflictWait time.Duration
	agent        requestAgent
}

type batch struct{ requests []*request }

type Scheduler struct {
	store          *db.Store
	capacity       int64
	maxBatch       int
	batchWait      time.Duration
	workers        int
	used           atomic.Int64
	input          chan *request
	jobs           chan batch
	completions    chan completion
	closed         chan struct{}
	closeOnce      sync.Once
	wg             sync.WaitGroup
	plan           planLookup
	plainBatch     bool
	initialization InitializationMetrics
	peakQueue      atomic.Int64
	policyNanos    atomic.Int64
	strategy       controlStrategy
	conflicts      conflictCounters
	traceMu        sync.Mutex
	traceEvents    []TraceEvent
	traceEnabled   atomic.Bool
	executeOne     func(context.Context, workload.Operation) error
	executeBatch   func(context.Context, []workload.Operation) error
}

func New(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	return NewStatic(store, capacity, maxBatch, workers, batchWait)
}

func NewRuntime(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	return newScheduler(store, capacity, maxBatch, workers, batchWait, "runtime")
}

func NewStatic(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	if capacity != staticExecutionPlan.QueueCapacity || maxBatch != staticExecutionPlan.MaxBatch || workers != staticExecutionPlan.Workers {
		panic("static scheduler configuration differs from the Oct execution plan")
	}
	return newScheduler(store, staticExecutionPlan.QueueCapacity, staticExecutionPlan.MaxBatch, staticExecutionPlan.Workers, batchWait, "static")
}

func NewFixedBatch(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	return newScheduler(store, capacity, maxBatch, workers, batchWait, "fixed")
}

func NewConflictAware(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	validateStaticEnvelope(capacity, maxBatch, workers)
	return newScheduler(store, capacity, maxBatch, workers, batchWait, "conflict")
}

func NewUtility(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	validateStaticEnvelope(capacity, maxBatch, workers)
	return newScheduler(store, capacity, maxBatch, workers, batchWait, "utility")
}

func NewAgentic(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	validateStaticEnvelope(capacity, maxBatch, workers)
	return newScheduler(store, capacity, maxBatch, workers, batchWait, "agentic")
}

func validateStaticEnvelope(capacity, maxBatch, workers int) {
	if capacity != staticExecutionPlan.QueueCapacity || maxBatch != staticExecutionPlan.MaxBatch || workers != staticExecutionPlan.Workers {
		panic("M2 scheduler configuration differs from the Oct execution plan")
	}
}

func newScheduler(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration, mode string) *Scheduler {
	wallStarted := time.Now()
	before := readMem()
	if capacity < 1 {
		capacity = 1
	}
	if maxBatch < 1 {
		maxBatch = 1
	}
	if workers < 1 {
		workers = 1
	}
	metadataStarted := time.Now()
	var plan planLookup
	if mode == "runtime" {
		plan = buildRuntimePlan(capacity, maxBatch, workers)
	} else if mode == "static" || mode == "conflict" || mode == "utility" || mode == "agentic" {
		plan = staticPlanLookup{plan: &staticExecutionPlan}
	}
	metadataTime := time.Since(metadataStarted)
	afterMetadata := readMem()
	catalogStarted := time.Now()
	catalogCount := 0
	if plan != nil {
		catalogCount = plan.statementCount()
	}
	catalogTime := time.Since(catalogStarted)
	schedulerStarted := time.Now()
	s := &Scheduler{
		store: store, capacity: int64(capacity), maxBatch: maxBatch,
		batchWait: batchWait, workers: workers,
		input: make(chan *request, capacity), jobs: make(chan batch, workers), completions: make(chan completion, workers), closed: make(chan struct{}),
		plan: plan, plainBatch: mode == "fixed",
	}
	if store != nil {
		s.executeOne = store.Execute
		s.executeBatch = store.ExecuteReadBatch
	}
	switch mode {
	case "conflict":
		s.strategy = controlCentralized
	case "utility":
		s.strategy = controlUtility
	case "agentic":
		s.strategy = controlAgentic
	}
	s.wg.Add(1 + workers)
	if s.strategy == controlNone {
		go s.dispatch()
	} else {
		go s.conflictDispatch()
	}
	for range workers {
		go s.work()
	}
	after := readMem()
	s.initialization = InitializationMetrics{
		WallTimeUS: durationUS(time.Since(wallStarted)), SchedulerTimeUS: durationUS(time.Since(schedulerStarted)), MetadataTimeUS: durationUS(metadataTime),
		StatementCatalogTimeUS: durationUS(catalogTime), Allocations: after.Mallocs - before.Mallocs,
		AllocatedBytes: after.TotalAlloc - before.TotalAlloc, MetadataAllocations: afterMetadata.Mallocs - before.Mallocs,
		MetadataAllocatedBytes: afterMetadata.TotalAlloc - before.TotalAlloc, StatementCatalogCount: catalogCount,
	}
	return s
}

func durationUS(value time.Duration) float64 { return float64(value) / float64(time.Microsecond) }

// Submit decides admission before enqueueing. The CAS is the authoritative
// resource bound; the generated Oct flow is the authoritative policy decision.
func (s *Scheduler) Submit(ctx context.Context, op workload.Operation) Result {
	for {
		used := s.used.Load()
		if used >= s.capacity {
			return Result{Err: ErrRejected}
		}
		if !s.plainBatch {
			if _, ok := s.plan.command(op.Kind); !ok {
				return Result{Err: errors.New("operation absent from execution plan")}
			}
			started := time.Now()
			decision := policyDecision(int(used), int(s.capacity), int(op.Kind), 0, 0, s.maxBatch)
			s.policyNanos.Add(int64(time.Since(started)))
			if decision == 0 {
				return Result{Err: ErrRejected}
			}
		}
		if s.used.CompareAndSwap(used, used+1) {
			break
		}
	}
	r := &request{ctx: ctx, op: op, queuedAt: time.Now(), done: make(chan Result, 1)}
	select {
	case s.input <- r:
		for depth := int64(len(s.input)); ; {
			peak := s.peakQueue.Load()
			if depth <= peak || s.peakQueue.CompareAndSwap(peak, depth) {
				break
			}
		}
	case <-ctx.Done():
		s.used.Add(-1)
		return Result{Err: ctx.Err()}
	case <-s.closed:
		s.used.Add(-1)
		return Result{Err: context.Canceled}
	}
	select {
	case result := <-r.done:
		return result
	case <-ctx.Done():
		// The admitted operation remains owned by the scheduler and will release
		// capacity on completion; returning does not cause a duplicate retry.
		return Result{Err: ctx.Err()}
	}
}

func (s *Scheduler) InUse() int64 { return s.used.Load() }

func (s *Scheduler) Initialization() InitializationMetrics { return s.initialization }

func (s *Scheduler) PeakQueueDepth() int64 { return s.peakQueue.Load() }

func (s *Scheduler) PolicyCPUTime() time.Duration { return time.Duration(s.policyNanos.Load()) }

func (s *Scheduler) ConflictMetrics() ConflictMetrics { return s.conflicts.snapshot() }

func (s *Scheduler) EnableTrace() { s.traceEnabled.Store(true) }

func (s *Scheduler) Trace() []TraceEvent {
	s.traceMu.Lock()
	defer s.traceMu.Unlock()
	return append([]TraceEvent(nil), s.traceEvents...)
}

func (s *Scheduler) trace(r *request, event, reason string) {
	if !s.traceEnabled.Load() {
		return
	}
	s.traceMu.Lock()
	defer s.traceMu.Unlock()
	if len(s.traceEvents) < 256 {
		s.traceEvents = append(s.traceEvents, TraceEvent{RequestID: r.op.Sequence, Event: event, Reason: reason})
	}
}

func (s *Scheduler) Close() {
	s.closeOnce.Do(func() { close(s.closed); s.wg.Wait() })
}

func (s *Scheduler) dispatch() {
	defer s.wg.Done()
	defer close(s.jobs)
	var carry *request
	for {
		var first *request
		if carry != nil {
			first, carry = carry, nil
		} else {
			select {
			case first = <-s.input:
			case <-s.closed:
				return
			}
		}
		group := []*request{first}
		if first.op.Kind <= workload.RangeRead && s.maxBatch > 1 {
			deadline := time.NewTimer(s.batchWait)
		gather:
			for len(group) < s.maxBatch {
				select {
				case next := <-s.input:
					compatible := next.op.Kind == first.op.Kind && next.op.Kind <= workload.RangeRead
					decision := 2
					if !s.plainBatch {
						compatible = s.plan.compatible(first.op.Kind, next.op.Kind)
						started := time.Now()
						decision = policyDecision(int(s.used.Load()), int(s.capacity)+1, int(next.op.Kind), int(first.op.Kind), len(group), s.maxBatch)
						s.policyNanos.Add(int64(time.Since(started)))
					}
					if !compatible || decision == 1 {
						carry = next
						break gather
					}
					group = append(group, next)
					if decision == 3 {
						break gather
					}
				case <-deadline.C:
					break gather
				case <-s.closed:
					deadline.Stop()
					return
				}
			}
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}
		}
		select {
		case s.jobs <- batch{requests: group}:
		case <-s.closed:
			return
		}
	}
}

func (s *Scheduler) work() {
	defer s.wg.Done()
	for job := range s.jobs {
		started := time.Now()
		ops := make([]workload.Operation, len(job.requests))
		for i, r := range job.requests {
			ops[i] = r.op
		}
		var err error
		if len(ops) > 1 {
			err = s.executeBatch(job.requests[0].ctx, ops)
		} else {
			err = s.executeOne(job.requests[0].ctx, ops[0])
		}
		finished := time.Now()
		if s.strategy != controlNone {
			s.completions <- completion{job: job, err: err, started: started, finished: finished}
			continue
		}
		for _, r := range job.requests {
			r.done <- Result{Err: err, QueueTime: started.Sub(r.queuedAt), Service: finished.Sub(started), BatchSize: len(job.requests)}
			s.used.Add(-1)
		}
	}
}

func policyDecision(used, capacity, requestClass, batchClass, batchCount, maxBatch int) int {
	machine := fn_Scheduler_SchedulerDecision(used, capacity, requestClass, batchClass, batchCount, maxBatch)
	machine.__octStep()
	decision, complete := machine.__octResult()
	if !complete {
		panic("generated Oct scheduler did not complete")
	}
	return decision
}

const (
	utilityDispatch = iota
	utilityDefer
	utilityPromote
	utilityJoinBatch
)

func utilityDecision(legal bool, priority, ageMicros, batchCount, maxBatch int) int {
	machine := fn_Scheduler_UtilityDecision(legal, priority, ageMicros, batchCount, maxBatch)
	machine.__octStep()
	decision, complete := machine.__octResult()
	if !complete {
		panic("generated Oct utility policy did not complete")
	}
	return decision
}
