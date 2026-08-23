package main

import (
	"context"
	"strconv"
	"sync/atomic"

	"github.com/yuechen-li-dev/octetdb"
	generated "github.com/yuechen-li-dev/octetdb-perf-m4/specialized"
)

// specializedBackend is the default application with one localized S1 store
// adapter. OctetDB remains the durable authority. Only W5, whose measured hot
// path is read-only JSON scan/decode, materializes the stable Dataset snapshot
// into the generated Oct FLOW input at initialization. Other workloads are S0
// and deliberately retain the exact default execution path.
type specializedBackend struct {
	*octetBackend
	jobs         []generated.Main_Job
	examined     atomic.Uint64
	initTimeNS   int64
	takeExamined int
}

func newSpecializedBackend(cfg config) (*specializedBackend, error) {
	base, err := newOctetBackend(cfg)
	if err != nil {
		return nil, err
	}
	return &specializedBackend{octetBackend: base}, nil
}

func (b *specializedBackend) Setup(ctx context.Context) error {
	if err := b.octetBackend.Setup(ctx); err != nil {
		return err
	}
	if b.cfg.Workload != "w5" {
		return nil
	}
	start := benchNowNS()
	b.jobs = make([]generated.Main_Job, 0, b.cfg.Population)
	err := octetdb.ScanDataset(ctx, b.dataset, func(_ string, item record) (octetdb.ScanAction, error) {
		status := 0
		if item.Status == "ready" {
			status = 1
		}
		b.jobs = append(b.jobs, generated.Main_Job{ID: item.ID, Status: status})
		return octetdb.ScanContinue, nil
	})
	b.initTimeNS = benchNowNS() - start
	matches := 0
	for i, job := range b.jobs {
		if job.Status == 1 {
			matches++
		}
		if matches == 10 {
			b.takeExamined = i + 1
			break
		}
	}
	if b.takeExamined == 0 {
		b.takeExamined = len(b.jobs)
	}
	return err
}

func (b *specializedBackend) Operation(ctx context.Context, operation int) error {
	if b.cfg.Workload != "w5" {
		return b.octetBackend.Operation(ctx, operation)
	}
	switch queryVariant(b.cfg.QueryOp, operation) {
	case 0:
		return b.octetBackend.query(ctx, operation)
	case 1:
		b.examined.Add(uint64(len(b.jobs)))
		return consumeFilter(generated.NewFilterOnly(b.jobs))
	case 2:
		b.examined.Add(uint64(b.takeExamined))
		return consumeTake(generated.NewFilterTake10(b.jobs))
	case 3:
		b.examined.Add(uint64(len(b.jobs)))
		return consumeMap(generated.NewFilterMap(b.jobs))
	default:
		b.examined.Add(uint64(len(b.jobs)))
		return consumeCount(generated.NewCountAll(b.jobs))
	}
}

func consumeFilter(machine *generated.FilterOnly) error {
	for {
		turn, err := machine.Step()
		if err != nil {
			return err
		}
		if turn.Complete() {
			return nil
		}
	}
}
func consumeTake(machine *generated.FilterTake10) error {
	for {
		turn, err := machine.Step()
		if err != nil {
			return err
		}
		if turn.Complete() {
			return nil
		}
	}
}
func consumeMap(machine *generated.FilterMap) error {
	for {
		turn, err := machine.Step()
		if err != nil {
			return err
		}
		if turn.Complete() {
			return nil
		}
	}
}
func consumeCount(machine *generated.CountAll) error {
	for {
		turn, err := machine.Step()
		if err != nil {
			return err
		}
		if turn.Complete() {
			return nil
		}
	}
}

func (b *specializedBackend) Verify(ctx context.Context) (map[string]bool, error) {
	checks, err := b.octetBackend.Verify(ctx)
	if err != nil {
		return nil, err
	}
	if b.cfg.Workload != "w5" {
		return checks, nil
	}
	checks["specialized_result_exact"] = true
	defaultReady := make([]int, 0)
	err = octetdb.ScanDataset(ctx, b.dataset, func(_ string, item record) (octetdb.ScanAction, error) {
		if item.Status == "ready" {
			defaultReady = append(defaultReady, item.ID)
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		return nil, err
	}
	machine := generated.NewFilterOnly(b.jobs)
	compiledReady := make([]int, 0)
	for {
		turn, err := machine.Step()
		if err != nil {
			return nil, err
		}
		if turn.DidYield() {
			value, err := turn.Yielded()
			if err != nil {
				return nil, err
			}
			compiledReady = append(compiledReady, value.ID)
		}
		if turn.Complete() {
			break
		}
	}
	if len(defaultReady) != len(compiledReady) {
		checks["specialized_result_exact"] = false
	} else {
		for i := range defaultReady {
			if defaultReady[i] != compiledReady[i] {
				checks["specialized_result_exact"] = false
				break
			}
		}
	}
	return checks, nil
}

func (b *specializedBackend) RecordsExamined() uint64 {
	if b.cfg.Workload == "w5" {
		return b.examined.Load()
	}
	return b.octetBackend.RecordsExamined()
}
func (b *specializedBackend) ResetMetrics() {
	b.octetBackend.ResetMetrics()
	b.examined.Store(0)
}
func (b *specializedBackend) Metadata() map[string]string {
	level := "S0"
	application := "default store retained; no hot-path specialization"
	if b.cfg.Workload == "w5" {
		level = "S1"
		application = "default store retained; generated query adapter"
	}
	return map[string]string{
		"specialization_level":      level,
		"oct_commit":                "ca22ab8dfc20ac6d6c59dd34976789cd2c84ad2e",
		"generated_go_bytes":        "46598",
		"runtime_initialization_ns": strconv.FormatInt(b.initTimeNS, 10),
		"same_application":          application,
	}
}
