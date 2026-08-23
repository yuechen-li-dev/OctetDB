package bench

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/yuechen-li-dev/octetdb/internal/baseline"
	"github.com/yuechen-li-dev/octetdb/internal/db"
	"github.com/yuechen-li-dev/octetdb/internal/metrics"
	"github.com/yuechen-li-dev/octetdb/internal/scheduled"
	"github.com/yuechen-li-dev/octetdb/internal/workload"
)

type Phase struct {
	Name     string                  `json:"name"`
	Duration time.Duration           `json:"duration"`
	Rate     int                     `json:"rate_per_second"`
	EndRate  int                     `json:"end_rate_per_second,omitempty"`
	Regime   workload.ConflictRegime `json:"conflict_regime"`
}

type Config struct {
	Seed              uint64        `json:"seed"`
	PoolSize          int32         `json:"pool_size"`
	SchedulerCapacity int           `json:"scheduler_capacity"`
	MaxBatch          int           `json:"max_batch"`
	BatchWait         time.Duration `json:"batch_wait"`
	RequestTimeout    time.Duration `json:"request_timeout"`
	WarmupOperations  int           `json:"warmup_operations"`
	DatasetCustomers  int           `json:"dataset_customers"`
	DatasetProducts   int           `json:"dataset_products"`
	Phases            []Phase       `json:"phases"`
}

func DefaultConfig() Config {
	return Config{
		Seed: 20260821, PoolSize: 8, SchedulerCapacity: 128, MaxBatch: 8, BatchWait: 2 * time.Millisecond,
		RequestTimeout: 30 * time.Second, WarmupOperations: 5000, DatasetCustomers: 100, DatasetProducts: 20,
		Phases: []Phase{{Name: "low", Duration: 10 * time.Second, Rate: 750, Regime: workload.LowContention}, {Name: "mixed", Duration: 10 * time.Second, Rate: 1500, Regime: workload.MixedContention}, {Name: "normal_before", Duration: 10 * time.Second, Rate: 750, Regime: workload.LowContention}, {Name: "overload", Duration: 8 * time.Second, Rate: 5000, Regime: workload.HotKeyContention}, {Name: "normal_after", Duration: 12 * time.Second, Rate: 750, Regime: workload.LowContention}},
	}
}

type Memory struct {
	AllocatedBytes   uint64  `json:"allocated_bytes"`
	Allocations      uint64  `json:"allocations"`
	BytesPerOp       float64 `json:"bytes_per_op"`
	AllocationsPerOp float64 `json:"allocations_per_op"`
	HeapHighBytes    uint64  `json:"heap_high_bytes"`
	GCCycles         uint32  `json:"gc_cycles"`
	GCPauseTotalMS   float64 `json:"gc_pause_total_ms"`
}
type Pool struct {
	MaxConns      int32   `json:"max_connections"`
	Acquires      int64   `json:"acquires"`
	AcquireWaitMS float64 `json:"acquire_wait_ms"`
	MaxInUse      int32   `json:"max_in_use"`
}
type Environment struct {
	OS                string `json:"os"`
	Arch              string `json:"arch"`
	GoVersion         string `json:"go_version"`
	PostgreSQLVersion string `json:"postgresql_version"`
	PgxVersion        string `json:"pgx_version"`
	CPUCount          int    `json:"cpu_count"`
}
type Result struct {
	Lane             string                          `json:"lane"`
	Started          time.Time                       `json:"started"`
	Elapsed          time.Duration                   `json:"elapsed"`
	Summary          metrics.Summary                 `json:"summary"`
	Memory           Memory                          `json:"memory"`
	Pool             Pool                            `json:"pool"`
	Environment      Environment                     `json:"environment"`
	Initialization   scheduled.InitializationMetrics `json:"initialization"`
	PeakQueueDepth   int64                           `json:"peak_queue_depth"`
	SchedulerCPUTime time.Duration                   `json:"scheduler_cpu_time"`
	Conflict         scheduled.ConflictMetrics       `json:"conflict"`
	Observer         scheduled.ObserverMetrics       `json:"observer"`
}

type laneResult struct {
	Err                              error
	QueueTime, Service, ConflictWait time.Duration
	BatchSize                        int
	Priority                         int
}
type lane interface {
	Submit(context.Context, workload.Operation) laneResult
}
type baselineAdapter struct{ baseline.Lane }

func (l baselineAdapter) Submit(ctx context.Context, op workload.Operation) laneResult {
	r := l.Lane.Submit(ctx, op)
	return laneResult{Err: r.Err, QueueTime: r.QueueTime, Service: r.Service, BatchSize: r.BatchSize}
}

type scheduledAdapter struct{ *scheduled.Scheduler }

func (l scheduledAdapter) Submit(ctx context.Context, op workload.Operation) laneResult {
	r := l.Scheduler.Submit(ctx, op)
	return laneResult{Err: r.Err, QueueTime: r.QueueTime, Service: r.Service, ConflictWait: r.ConflictWait, BatchSize: r.BatchSize, Priority: r.Priority}
}

func Run(ctx context.Context, name string, cfg Config, store *db.Store) (Result, error) {
	if err := store.Reset(ctx); err != nil {
		return Result{}, err
	}
	var impl lane
	var scheduler *scheduled.Scheduler
	startupStarted := time.Now()
	switch name {
	case "conventional", "baseline":
		impl = baselineAdapter{baseline.Lane{Store: store}}
	case "batch", "admission":
		scheduler = scheduled.NewFixedBatch(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "runtime":
		scheduler = scheduled.NewRuntime(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "static", "scheduled":
		scheduler = scheduled.NewStatic(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "conflict":
		scheduler = scheduled.NewConflictAware(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "f0":
		scheduler = scheduled.NewPersistentParity(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "priority", "h":
		scheduler = scheduled.NewPriority(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "shadow", "k0":
		scheduler = scheduled.NewObserverShadow(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "reactive", "j":
		scheduler = scheduled.NewReactiveObserver(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "utility", "f1":
		scheduler = scheduled.NewUtility(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "agentic":
		scheduler = scheduled.NewAgentic(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	default:
		return Result{}, fmt.Errorf("unknown lane %q", name)
	}
	if scheduler != nil {
		defer scheduler.Close()
	}
	initialization := scheduled.InitializationMetrics{WallTimeUS: float64(time.Since(startupStarted)) / float64(time.Microsecond)}
	if scheduler != nil {
		initialization = scheduler.Initialization()
	}
	warmup := workload.Generate(cfg.Seed^0xa5a5a5a5, cfg.WarmupOperations, time.Unix(1_750_000_000, 0).UTC())
	if err := runCorrectnessOps(ctx, warmup, cfg.SchedulerCapacity/2, impl); err != nil {
		return Result{}, fmt.Errorf("warmup: %w", err)
	}
	if err := store.Reset(ctx); err != nil {
		return Result{}, fmt.Errorf("reset after warmup: %w", err)
	}
	ops := generatePhasedOps(cfg)
	count := len(ops)
	debug.FreeOSMemory()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	poolBefore := store.Pool.Stat()
	started := time.Now()
	samples := make([]metrics.Sample, 0, count)
	var mu sync.Mutex
	var wg sync.WaitGroup
	opIndex := 0
	overloadEnd := time.Time{}
	maxInUse := int32(0)
	for _, phase := range cfg.Phases {
		phaseStart := time.Now()
		schedule := phaseSchedule(phase)
		for emitted := 0; emitted < len(schedule) && opIndex < len(ops); emitted++ {
			due := phaseStart.Add(schedule[emitted])
			if wait := time.Until(due); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return Result{}, ctx.Err()
				case <-timer.C:
				}
			}
			op := ops[opIndex]
			opIndex++
			phaseName := phase.Name
			wg.Add(1)
			go func() {
				defer wg.Done()
				reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
				defer cancel()
				begin := time.Now()
				r := impl.Submit(reqCtx, op)
				sample := metrics.Sample{CompletedAt: time.Now(), Phase: phaseName, Latency: time.Since(begin), Queue: r.QueueTime, Service: r.Service, ConflictWait: r.ConflictWait, BatchSize: r.BatchSize, Priority: r.Priority, Admitted: !errorsIsRejected(r.Err), Rejected: errorsIsRejected(r.Err), Failed: r.Err != nil && !errorsIsRejected(r.Err)}
				mu.Lock()
				samples = append(samples, sample)
				mu.Unlock()
			}()
			inUse := store.Pool.Stat().AcquiredConns()
			if inUse > maxInUse {
				maxInUse = inUse
			}
		}
		if phase.Name == "overload" || phase.Name == "sustained_overload" {
			overloadEnd = time.Now()
		}
	}
	wg.Wait()
	elapsed := time.Since(started)
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	poolAfter := store.Pool.Stat()
	var pgVersion string
	_ = store.Pool.QueryRow(ctx, "SHOW server_version").Scan(&pgVersion)
	summary := metrics.Summarize(samples, elapsed, batchMax(name, cfg), overloadEnd)
	allocated := after.TotalAlloc - before.TotalAlloc
	allocations := after.Mallocs - before.Mallocs
	perOp := float64(0)
	if summary.Completed > 0 {
		perOp = float64(summary.Completed)
	}
	result := Result{Lane: name, Started: started, Elapsed: elapsed, Summary: summary, Memory: Memory{AllocatedBytes: allocated, Allocations: allocations, BytesPerOp: divide(float64(allocated), perOp), AllocationsPerOp: divide(float64(allocations), perOp), HeapHighBytes: after.HeapSys, GCCycles: after.NumGC - before.NumGC, GCPauseTotalMS: float64(after.PauseTotalNs-before.PauseTotalNs) / float64(time.Millisecond)}, Pool: Pool{MaxConns: cfg.PoolSize, Acquires: poolAfter.AcquireCount() - poolBefore.AcquireCount(), AcquireWaitMS: float64(poolAfter.AcquireDuration()-poolBefore.AcquireDuration()) / float64(time.Millisecond), MaxInUse: maxInUse}, Environment: Environment{OS: runtime.GOOS, Arch: runtime.GOARCH, GoVersion: runtime.Version(), PostgreSQLVersion: pgVersion, PgxVersion: "github.com/jackc/pgx/v5 v5.7.6", CPUCount: runtime.NumCPU()}, Initialization: initialization}
	if scheduler != nil {
		result.PeakQueueDepth = scheduler.PeakQueueDepth()
		result.SchedulerCPUTime = scheduler.PolicyCPUTime()
		result.Conflict = scheduler.ConflictMetrics()
		result.Observer = scheduler.ObserverMetrics()
	}
	return result, nil
}

func divide(value, denominator float64) float64 {
	if denominator == 0 {
		return 0
	}
	return value / denominator
}

func errorsIsRejected(err error) bool { return err == scheduled.ErrRejected }
func batchMax(name string, cfg Config) int {
	if name == "batch" || name == "runtime" || name == "static" || name == "scheduled" || name == "conflict" || name == "f0" || name == "priority" || name == "h" || name == "shadow" || name == "k0" || name == "reactive" || name == "j" || name == "utility" || name == "f1" || name == "agentic" {
		return cfg.MaxBatch
	}
	return 1
}

func generatePhasedOps(cfg Config) []workload.Operation {
	total := 0
	for _, p := range cfg.Phases {
		total += len(phaseSchedule(p))
	}
	ops := make([]workload.Operation, 0, total)
	base := time.Unix(1_800_000_000, 0).UTC()
	for phaseIndex, p := range cfg.Phases {
		count := len(phaseSchedule(p))
		part := workload.GenerateRegime(cfg.Seed^uint64(phaseIndex+1)*0x9e3779b9, count, base.Add(time.Duration(len(ops))*time.Microsecond), p.Regime)
		for i := range part {
			part[i].Sequence = int64(len(ops) + i)
			part[i].OrderID = 1_000_000 + part[i].Sequence
		}
		ops = append(ops, part...)
	}
	return ops
}

// phaseSchedule uses deterministic 100 ms buckets for genuine linear ramps.
// Constant phases retain the original evenly spaced schedule exactly.
func phaseSchedule(phase Phase) []time.Duration {
	if phase.EndRate <= 0 || phase.EndRate == phase.Rate {
		count := int(phase.Duration.Seconds() * float64(phase.Rate))
		out := make([]time.Duration, count)
		for i := range out {
			out[i] = time.Duration(i) * time.Second / time.Duration(phase.Rate)
		}
		return out
	}
	bucket := 100 * time.Millisecond
	buckets := int((phase.Duration + bucket - 1) / bucket)
	out := make([]time.Duration, 0, int(phase.Duration.Seconds()*float64(phase.Rate+phase.EndRate)/2))
	for b := 0; b < buckets; b++ {
		start := time.Duration(b) * bucket
		width := bucket
		if start+width > phase.Duration {
			width = phase.Duration - start
		}
		fraction := (float64(b) + 0.5) / float64(buckets)
		rate := float64(phase.Rate) + fraction*float64(phase.EndRate-phase.Rate)
		count := int(rate*width.Seconds() + 0.5)
		for i := 0; i < count; i++ {
			out = append(out, start+time.Duration(i)*width/time.Duration(count))
		}
	}
	return out
}

type Correctness struct {
	Equal          bool                `json:"equal"`
	Operations     int                 `json:"operations"`
	BaselineState  db.State            `json:"baseline_state"`
	ScheduledState db.State            `json:"scheduled_state"`
	States         map[string]db.State `json:"states"`
}

func CheckCorrectness(ctx context.Context, cfg Config, store *db.Store) (Correctness, error) {
	ops := workload.Generate(cfg.Seed, 500, time.Unix(1_800_000_000, 0).UTC())
	if err := store.Reset(ctx); err != nil {
		return Correctness{}, err
	}
	b := baselineAdapter{baseline.Lane{Store: store}}
	if err := runCorrectnessOps(ctx, ops, cfg.SchedulerCapacity/2, b); err != nil {
		return Correctness{}, fmt.Errorf("baseline: %w", err)
	}
	bs, err := store.Snapshot(ctx)
	if err != nil {
		return Correctness{}, err
	}
	if err := store.Reset(ctx); err != nil {
		return Correctness{}, err
	}
	states := map[string]db.State{"conventional": bs}
	constructors := map[string]func() *scheduled.Scheduler{
		"batch": func() *scheduled.Scheduler {
			return scheduled.NewFixedBatch(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"runtime": func() *scheduled.Scheduler {
			return scheduled.NewRuntime(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"static": func() *scheduled.Scheduler {
			return scheduled.NewStatic(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"conflict": func() *scheduled.Scheduler {
			return scheduled.NewConflictAware(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"f0": func() *scheduled.Scheduler {
			return scheduled.NewPersistentParity(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"priority": func() *scheduled.Scheduler {
			return scheduled.NewPriority(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"shadow": func() *scheduled.Scheduler {
			return scheduled.NewObserverShadow(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"reactive": func() *scheduled.Scheduler {
			return scheduled.NewReactiveObserver(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"utility": func() *scheduled.Scheduler {
			return scheduled.NewUtility(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
		"agentic": func() *scheduled.Scheduler {
			return scheduled.NewAgentic(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		},
	}
	equal := true
	var ss db.State
	for _, name := range []string{"batch", "runtime", "static", "conflict", "f0", "priority", "shadow", "reactive", "utility", "agentic"} {
		if err := store.Reset(ctx); err != nil {
			return Correctness{}, err
		}
		s := constructors[name]()
		if err := runCorrectnessOps(ctx, ops, cfg.SchedulerCapacity/2, scheduledAdapter{s}); err != nil {
			s.Close()
			return Correctness{}, fmt.Errorf("%s: %w", name, err)
		}
		s.Close()
		state, err := store.Snapshot(ctx)
		if err != nil {
			return Correctness{}, err
		}
		states[name] = state
		if name == "static" {
			ss = state
		}
		equal = equal && reflect.DeepEqual(bs, state)
	}
	return Correctness{Equal: equal, Operations: len(ops), BaselineState: bs, ScheduledState: ss, States: states}, nil
}

type ObserverTrace struct {
	Phases  []PlantPhase              `json:"phases"`
	Events  []scheduled.ObserverEvent `json:"events"`
	Metrics scheduled.ObserverMetrics `json:"metrics"`
}

type ObserverSummary struct {
	Phases  []PlantPhase              `json:"phases"`
	Metrics scheduled.ObserverMetrics `json:"metrics"`
}

func WriteObserverTraceCSV(path string, trace ObserverTrace) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"tick", "at_ns", "raw_queue", "filtered_queue_milli", "arrivals", "completions", "mean_dispatch_delay_us", "persistence_queue_milli", "linear_queue_milli", "persistence_delay_us", "linear_delay_us", "actual_queue_milli", "actual_delay_us", "matured_persistence_queue_milli", "matured_linear_queue_milli", "matured_persistence_delay_us", "matured_linear_delay_us", "matured", "reactive_would_limit", "predictive_would_limit"}); err != nil {
		return err
	}
	for _, e := range trace.Events {
		row := []string{strconv.FormatInt(e.Tick, 10), strconv.FormatInt(e.AtNS, 10), strconv.Itoa(e.RawQueue), strconv.Itoa(e.FilteredQueueMilli), strconv.Itoa(e.Arrivals), strconv.Itoa(e.Completions), strconv.Itoa(e.MeanDispatchDelayMicros), strconv.Itoa(e.PersistenceQueueMilli), strconv.Itoa(e.LinearQueueMilli), strconv.Itoa(e.PersistenceDelayMicros), strconv.Itoa(e.LinearDelayMicros), strconv.Itoa(e.ActualQueueMilli), strconv.Itoa(e.ActualDelayMicros), strconv.Itoa(e.MaturedPersistenceQueueMilli), strconv.Itoa(e.MaturedLinearQueueMilli), strconv.Itoa(e.MaturedPersistenceDelayMicros), strconv.Itoa(e.MaturedLinearDelayMicros), strconv.FormatBool(e.Matured), strconv.FormatBool(e.ReactiveWouldLimit), strconv.FormatBool(e.PredictiveWouldLimit)}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// CaptureObserverTrace runs K0 with the same H eligibility and fairness path;
// only the observer is active, and its bounded trace is outside timed lanes.
func CaptureObserverTrace(ctx context.Context, cfg Config, store *db.Store) (ObserverTrace, error) {
	if err := store.Reset(ctx); err != nil {
		return ObserverTrace{}, err
	}
	s := scheduled.NewObserverShadow(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
	defer s.Close()
	if err := runCorrectnessOps(ctx, workload.Generate(cfg.Seed^0x4d35, 500, time.Unix(1_750_000_000, 0).UTC()), cfg.SchedulerCapacity/2, scheduledAdapter{s}); err != nil {
		return ObserverTrace{}, fmt.Errorf("observer warmup: %w", err)
	}
	if err := store.Reset(ctx); err != nil {
		return ObserverTrace{}, err
	}
	s.EnableObserverTrace()
	ops := generatePhasedOps(cfg)
	opIndex := 0
	var wg sync.WaitGroup
	out := ObserverTrace{Phases: make([]PlantPhase, 0, len(cfg.Phases))}
	for _, phase := range cfg.Phases {
		phaseStart := time.Now()
		schedule := phaseSchedule(phase)
		for emitted := 0; emitted < len(schedule) && opIndex < len(ops); emitted++ {
			due := phaseStart.Add(schedule[emitted])
			if wait := time.Until(due); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ObserverTrace{}, ctx.Err()
				case <-timer.C:
				}
			}
			op := ops[opIndex]
			opIndex++
			wg.Add(1)
			go func() {
				defer wg.Done()
				reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
				defer cancel()
				_ = s.Submit(reqCtx, op)
			}()
		}
		out.Phases = append(out.Phases, PlantPhase{Name: phase.Name, StartNS: phaseStart.UnixNano(), EndNS: time.Now().UnixNano(), Offered: len(schedule), ConflictRegime: fmt.Sprint(phase.Regime)})
	}
	wg.Wait()
	out.Events = s.ObserverTrace()
	out.Metrics = s.ObserverMetrics()
	return out, nil
}

type DiagnosticTrace struct {
	Utility []scheduled.TraceEvent `json:"utility"`
	Agentic []scheduled.TraceEvent `json:"agentic"`
}

type PlantPhase struct {
	Name           string `json:"name"`
	StartNS        int64  `json:"start_ns"`
	EndNS          int64  `json:"end_ns"`
	Offered        int    `json:"offered"`
	ConflictRegime string `json:"conflict_regime"`
}

type PlantTrace struct {
	Phases []PlantPhase           `json:"phases"`
	Events []scheduled.PlantEvent `json:"events"`
}

type PlantPhaseSummary struct {
	Name                    string  `json:"name"`
	Completions             int     `json:"completions"`
	DispatchCompletionP50MS float64 `json:"dispatch_completion_p50_ms"`
	DispatchCompletionP95MS float64 `json:"dispatch_completion_p95_ms"`
	DispatchCompletionP99MS float64 `json:"dispatch_completion_p99_ms"`
	QueueP95MS              float64 `json:"queue_p95_ms"`
	PeakPending             int     `json:"peak_pending"`
	PeakActive              int     `json:"peak_active"`
}

type PlantSummary struct {
	EventCount int                 `json:"event_count"`
	Phases     []PlantPhaseSummary `json:"phases"`
}

func SummarizePlantTrace(trace PlantTrace) PlantSummary {
	out := PlantSummary{EventCount: len(trace.Events), Phases: make([]PlantPhaseSummary, 0, len(trace.Phases))}
	for _, phase := range trace.Phases {
		var delays, queues []float64
		item := PlantPhaseSummary{Name: phase.Name}
		for _, event := range trace.Events {
			if event.AtNS < phase.StartNS || event.AtNS > phase.EndNS {
				continue
			}
			if event.Pending > item.PeakPending {
				item.PeakPending = event.Pending
			}
			if event.Active > item.PeakActive {
				item.PeakActive = event.Active
			}
			if event.Kind == "completion" {
				item.Completions++
				delays = append(delays, float64(event.DispatchDelayNS)/float64(time.Millisecond))
				queues = append(queues, float64(event.QueueNS)/float64(time.Millisecond))
			}
		}
		sort.Float64s(delays)
		sort.Float64s(queues)
		item.DispatchCompletionP50MS = percentileFloat(delays, .50)
		item.DispatchCompletionP95MS = percentileFloat(delays, .95)
		item.DispatchCompletionP99MS = percentileFloat(delays, .99)
		item.QueueP95MS = percentileFloat(queues, .95)
		out.Phases = append(out.Phases, item)
	}
	return out
}

func percentileFloat(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	return values[int(percentile*float64(len(values)-1))]
}

func WritePlantTraceCSV(path string, trace PlantTrace) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if err := w.Write([]string{"at_ns", "event", "request_id", "command", "pending", "active", "admitted", "batch_size", "queue_ns", "service_ns", "dispatch_completion_ns"}); err != nil {
		return err
	}
	for _, e := range trace.Events {
		row := []string{strconv.FormatInt(e.AtNS, 10), e.Kind, strconv.FormatInt(e.RequestID, 10), e.CommandKind, strconv.Itoa(e.Pending), strconv.Itoa(e.Active), strconv.FormatInt(e.Admitted, 10), strconv.Itoa(e.BatchSize), strconv.FormatInt(e.QueueNS, 10), strconv.FormatInt(e.ServiceNS, 10), strconv.FormatInt(e.DispatchDelayNS, 10)}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	w.Flush()
	return w.Error()
}

// CapturePlantTrace exercises the real H scheduler and PostgreSQL path with
// diagnostics enabled. It is deliberately separate from Run so lifecycle
// tracing cannot contaminate authoritative timed lane measurements.
func CapturePlantTrace(ctx context.Context, cfg Config, store *db.Store) (PlantTrace, error) {
	if err := store.Reset(ctx); err != nil {
		return PlantTrace{}, err
	}
	s := scheduled.NewPlantCharacterization(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
	defer s.Close()
	if err := runCorrectnessOps(ctx, workload.Generate(cfg.Seed^0x4d34, 500, time.Unix(1_750_000_000, 0).UTC()), cfg.SchedulerCapacity/2, scheduledAdapter{s}); err != nil {
		return PlantTrace{}, fmt.Errorf("plant warmup: %w", err)
	}
	if err := store.Reset(ctx); err != nil {
		return PlantTrace{}, err
	}
	ops := generatePhasedOps(cfg)
	opIndex := 0
	var wg sync.WaitGroup
	out := PlantTrace{Phases: make([]PlantPhase, 0, len(cfg.Phases))}
	for _, phase := range cfg.Phases {
		phaseStart := time.Now()
		schedule := phaseSchedule(phase)
		for emitted := 0; emitted < len(schedule) && opIndex < len(ops); emitted++ {
			due := phaseStart.Add(schedule[emitted])
			if wait := time.Until(due); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-ctx.Done():
					timer.Stop()
					return PlantTrace{}, ctx.Err()
				case <-timer.C:
				}
			}
			op := ops[opIndex]
			opIndex++
			wg.Add(1)
			go func() {
				defer wg.Done()
				reqCtx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
				defer cancel()
				_ = s.Submit(reqCtx, op)
			}()
		}
		out.Phases = append(out.Phases, PlantPhase{Name: phase.Name, StartNS: phaseStart.UnixNano(), EndNS: time.Now().UnixNano(), Offered: len(schedule), ConflictRegime: fmt.Sprint(phase.Regime)})
	}
	wg.Wait()
	out.Events = s.PlantTrace()
	return out, nil
}

// CaptureTrace runs a bounded diagnostic workload outside authoritative
// timing. Performance lanes leave tracing disabled.
func CaptureTrace(ctx context.Context, cfg Config, store *db.Store) (DiagnosticTrace, error) {
	var out DiagnosticTrace
	for _, item := range []struct {
		name string
		new  func() *scheduled.Scheduler
		dst  *[]scheduled.TraceEvent
	}{
		{"utility", func() *scheduled.Scheduler {
			return scheduled.NewUtility(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		}, &out.Utility},
		{"agentic", func() *scheduled.Scheduler {
			return scheduled.NewAgentic(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		}, &out.Agentic},
	} {
		if err := store.Reset(ctx); err != nil {
			return out, err
		}
		s := item.new()
		s.EnableTrace()
		ops := workload.GenerateRegime(cfg.Seed^0x4d32, 64, time.Unix(1_810_000_000, 0).UTC(), workload.HotKeyContention)
		if err := runCorrectnessOps(ctx, ops, 64, scheduledAdapter{s}); err != nil {
			s.Close()
			return out, fmt.Errorf("trace %s: %w", item.name, err)
		}
		*item.dst = s.Trace()
		s.Close()
	}
	return out, nil
}

func runCorrectnessOps(ctx context.Context, ops []workload.Operation, width int, impl lane) error {
	if width < 1 {
		width = 1
	}
	for start := 0; start < len(ops); start += width {
		end := start + width
		if end > len(ops) {
			end = len(ops)
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		var first error
		for _, op := range ops[start:end] {
			op := op
			wg.Add(1)
			go func() {
				defer wg.Done()
				if result := impl.Submit(ctx, op); result.Err != nil {
					mu.Lock()
					if first == nil {
						first = fmt.Errorf("op %d: %w", op.Sequence, result.Err)
					}
					mu.Unlock()
				}
			}()
		}
		wg.Wait()
		if first != nil {
			return first
		}
	}
	return nil
}

func WriteJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o644)
}
