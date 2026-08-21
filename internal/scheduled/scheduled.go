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
	Err       error
	QueueTime time.Duration
	Service   time.Duration
	BatchSize int
}

type request struct {
	ctx      context.Context
	op       workload.Operation
	queuedAt time.Time
	done     chan Result
}

type batch struct{ requests []*request }

type Scheduler struct {
	store     *db.Store
	capacity  int64
	maxBatch  int
	batchWait time.Duration
	workers   int
	used      atomic.Int64
	input     chan *request
	jobs      chan batch
	closed    chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func New(store *db.Store, capacity, maxBatch, workers int, batchWait time.Duration) *Scheduler {
	if capacity < 1 {
		capacity = 1
	}
	if maxBatch < 1 {
		maxBatch = 1
	}
	if workers < 1 {
		workers = 1
	}
	s := &Scheduler{
		store: store, capacity: int64(capacity), maxBatch: maxBatch,
		batchWait: batchWait, workers: workers,
		input: make(chan *request, capacity), jobs: make(chan batch, workers), closed: make(chan struct{}),
	}
	s.wg.Add(1 + workers)
	go s.dispatch()
	for range workers {
		go s.work()
	}
	return s
}

// Submit decides admission before enqueueing. The CAS is the authoritative
// resource bound; the generated Oct flow is the authoritative policy decision.
func (s *Scheduler) Submit(ctx context.Context, op workload.Operation) Result {
	for {
		used := s.used.Load()
		if policyDecision(int(used), int(s.capacity), int(op.Kind), 0, 0, s.maxBatch) == 0 {
			return Result{Err: ErrRejected}
		}
		if s.used.CompareAndSwap(used, used+1) {
			break
		}
	}
	r := &request{ctx: ctx, op: op, queuedAt: time.Now(), done: make(chan Result, 1)}
	select {
	case s.input <- r:
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
					decision := policyDecision(int(s.used.Load()), int(s.capacity)+1, int(next.op.Kind), int(first.op.Kind), len(group), s.maxBatch)
					if decision == 1 {
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
			err = s.store.ExecuteReadBatch(job.requests[0].ctx, ops)
		} else {
			err = s.store.Execute(job.requests[0].ctx, ops[0])
		}
		finished := time.Now()
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
