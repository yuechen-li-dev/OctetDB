package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/yuechen-li-dev/database-scheduler/internal/baseline"
	"github.com/yuechen-li-dev/database-scheduler/internal/db"
	"github.com/yuechen-li-dev/database-scheduler/internal/metrics"
	"github.com/yuechen-li-dev/database-scheduler/internal/scheduled"
	"github.com/yuechen-li-dev/database-scheduler/internal/workload"
)

type Phase struct {
	Name     string        `json:"name"`
	Duration time.Duration `json:"duration"`
	Rate     int           `json:"rate_per_second"`
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
		Phases: []Phase{{"steady", 15 * time.Second, 500}, {"burst", 2 * time.Second, 5000}, {"normal_before", 15 * time.Second, 500}, {"overload", 10 * time.Second, 10000}, {"normal_after", 20 * time.Second, 500}},
	}
}

type Memory struct {
	AllocatedBytes uint64  `json:"allocated_bytes"`
	HeapHighBytes  uint64  `json:"heap_high_bytes"`
	GCCycles       uint32  `json:"gc_cycles"`
	GCPauseTotalMS float64 `json:"gc_pause_total_ms"`
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
	Lane        string          `json:"lane"`
	Started     time.Time       `json:"started"`
	Elapsed     time.Duration   `json:"elapsed"`
	Summary     metrics.Summary `json:"summary"`
	Memory      Memory          `json:"memory"`
	Pool        Pool            `json:"pool"`
	Environment Environment     `json:"environment"`
}

type laneResult struct {
	Err                error
	QueueTime, Service time.Duration
	BatchSize          int
}
type lane interface {
	Submit(context.Context, workload.Operation) laneResult
}
type baselineAdapter struct{ baseline.Lane }

func (l baselineAdapter) Submit(ctx context.Context, op workload.Operation) laneResult {
	r := l.Lane.Submit(ctx, op)
	return laneResult(r)
}

type scheduledAdapter struct{ *scheduled.Scheduler }

func (l scheduledAdapter) Submit(ctx context.Context, op workload.Operation) laneResult {
	r := l.Scheduler.Submit(ctx, op)
	return laneResult(r)
}

func Run(ctx context.Context, name string, cfg Config, store *db.Store) (Result, error) {
	if err := store.Reset(ctx); err != nil {
		return Result{}, err
	}
	var impl lane
	var scheduler *scheduled.Scheduler
	switch name {
	case "baseline":
		impl = baselineAdapter{baseline.Lane{Store: store}}
	case "admission":
		scheduler = scheduled.New(store, cfg.SchedulerCapacity, 1, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	case "scheduled":
		scheduler = scheduled.New(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
		impl = scheduledAdapter{scheduler}
	default:
		return Result{}, fmt.Errorf("unknown lane %q", name)
	}
	if scheduler != nil {
		defer scheduler.Close()
	}
	warmup := workload.Generate(cfg.Seed^0xa5a5a5a5, cfg.WarmupOperations, time.Unix(1_750_000_000, 0).UTC())
	if err := runCorrectnessOps(ctx, warmup, cfg.SchedulerCapacity/2, impl); err != nil {
		return Result{}, fmt.Errorf("warmup: %w", err)
	}
	if err := store.Reset(ctx); err != nil {
		return Result{}, fmt.Errorf("reset after warmup: %w", err)
	}
	count := 0
	for _, p := range cfg.Phases {
		count += int(p.Duration.Seconds() * float64(p.Rate))
	}
	ops := workload.Generate(cfg.Seed, count, time.Unix(1_800_000_000, 0).UTC())
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
		phaseCount := int(phase.Duration.Seconds() * float64(phase.Rate))
		for emitted := 0; emitted < phaseCount && opIndex < len(ops); emitted++ {
			due := phaseStart.Add(time.Duration(emitted) * time.Second / time.Duration(phase.Rate))
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
				sample := metrics.Sample{CompletedAt: time.Now(), Phase: phaseName, Latency: time.Since(begin), Queue: r.QueueTime, Service: r.Service, BatchSize: r.BatchSize, Admitted: !errorsIsRejected(r.Err), Rejected: errorsIsRejected(r.Err), Failed: r.Err != nil && !errorsIsRejected(r.Err)}
				mu.Lock()
				samples = append(samples, sample)
				mu.Unlock()
			}()
			inUse := store.Pool.Stat().AcquiredConns()
			if inUse > maxInUse {
				maxInUse = inUse
			}
		}
		if phase.Name == "overload" {
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
	result := Result{Lane: name, Started: started, Elapsed: elapsed, Summary: metrics.Summarize(samples, elapsed, batchMax(name, cfg), overloadEnd), Memory: Memory{AllocatedBytes: after.TotalAlloc - before.TotalAlloc, HeapHighBytes: after.HeapSys, GCCycles: after.NumGC - before.NumGC, GCPauseTotalMS: float64(after.PauseTotalNs-before.PauseTotalNs) / float64(time.Millisecond)}, Pool: Pool{MaxConns: cfg.PoolSize, Acquires: poolAfter.AcquireCount() - poolBefore.AcquireCount(), AcquireWaitMS: float64(poolAfter.AcquireDuration()-poolBefore.AcquireDuration()) / float64(time.Millisecond), MaxInUse: maxInUse}, Environment: Environment{OS: runtime.GOOS, Arch: runtime.GOARCH, GoVersion: runtime.Version(), PostgreSQLVersion: pgVersion, PgxVersion: "github.com/jackc/pgx/v5 v5.7.6", CPUCount: runtime.NumCPU()}}
	return result, nil
}

func errorsIsRejected(err error) bool { return err == scheduled.ErrRejected }
func batchMax(name string, cfg Config) int {
	if name == "scheduled" {
		return cfg.MaxBatch
	}
	return 1
}

type Correctness struct {
	Equal          bool     `json:"equal"`
	Operations     int      `json:"operations"`
	BaselineState  db.State `json:"baseline_state"`
	ScheduledState db.State `json:"scheduled_state"`
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
	s := scheduled.New(store, cfg.SchedulerCapacity, cfg.MaxBatch, int(cfg.PoolSize), cfg.BatchWait)
	defer s.Close()
	if err := runCorrectnessOps(ctx, ops, cfg.SchedulerCapacity/2, scheduledAdapter{s}); err != nil {
		return Correctness{}, fmt.Errorf("scheduled: %w", err)
	}
	ss, err := store.Snapshot(ctx)
	if err != nil {
		return Correctness{}, err
	}
	return Correctness{Equal: reflect.DeepEqual(bs, ss), Operations: len(ops), BaselineState: bs, ScheduledState: ss}, nil
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
