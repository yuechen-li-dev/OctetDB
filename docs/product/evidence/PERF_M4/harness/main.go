package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type config struct {
	Lane        string `json:"lane"`
	Workload    string `json:"workload"`
	Population  int    `json:"population"`
	Operations  int    `json:"operations"`
	Concurrency int    `json:"concurrency"`
	Contention  string `json:"contention"`
	Mix         string `json:"mix,omitempty"`
	QueryOp     string `json:"query_op,omitempty"`
	Selectivity string `json:"selectivity,omitempty"`
	DataPath    string `json:"data_path,omitempty"`
	PostgresURL string `json:"-"`
	Warmup      int    `json:"warmup"`
	SkipSeed    bool   `json:"skip_seed,omitempty"`
}

type result struct {
	SchemaVersion      int               `json:"schema_version"`
	Timestamp          time.Time         `json:"timestamp"`
	Config             config            `json:"config"`
	DurationNS         int64             `json:"duration_ns"`
	Throughput         float64           `json:"throughput_ops_s"`
	P50NS              int64             `json:"p50_ns"`
	P95NS              int64             `json:"p95_ns"`
	P99NS              int64             `json:"p99_ns"`
	MaxNS              int64             `json:"max_ns"`
	CPUUserNS          int64             `json:"cpu_user_ns"`
	CPUKernelNS        int64             `json:"cpu_kernel_ns"`
	RSSBytes           uint64            `json:"rss_bytes"`
	HeapAllocBytes     uint64            `json:"heap_alloc_bytes"`
	HeapObjects        uint64            `json:"heap_objects"`
	TotalAllocDelta    uint64            `json:"total_alloc_delta_bytes"`
	MallocDelta        uint64            `json:"malloc_delta"`
	GCCyclesDelta      uint32            `json:"gc_cycles_delta"`
	GCPauseDeltaNS     uint64            `json:"gc_pause_delta_ns"`
	StorageBeforeBytes int64             `json:"storage_before_bytes"`
	StorageAfterBytes  int64             `json:"storage_after_bytes"`
	StorageDeltaPerOp  float64           `json:"storage_delta_bytes_per_op"`
	WALBeforeBytes     int64             `json:"wal_before_bytes"`
	WALAfterBytes      int64             `json:"wal_after_bytes"`
	WALBytesPerOp      float64           `json:"wal_bytes_per_op"`
	RecordsExamined    uint64            `json:"records_examined,omitempty"`
	Correctness        map[string]bool   `json:"correctness"`
	Metadata           map[string]string `json:"metadata"`
}

type backend interface {
	Setup(context.Context) error
	Operation(context.Context, int) error
	Verify(context.Context) (map[string]bool, error)
	StorageBytes() (int64, error)
	WALBytes() (int64, error)
	RecordsExamined() uint64
	ResetMetrics()
	Metadata() map[string]string
	Close() error
}

func main() {
	var cfg config
	var output string
	var cpuProfile, heapProfile string
	flag.StringVar(&cfg.Lane, "lane", "default", "default, specialized, or postgres")
	flag.StringVar(&cfg.Workload, "workload", "w1", "w1 through w6")
	flag.IntVar(&cfg.Population, "population", 1000, "logical records")
	flag.IntVar(&cfg.Operations, "operations", 200, "measured logical operations")
	flag.IntVar(&cfg.Concurrency, "concurrency", 1, "concurrent clients")
	flag.IntVar(&cfg.Warmup, "warmup", 20, "unmeasured operations")
	flag.BoolVar(&cfg.SkipSeed, "skip-seed", false, "reuse an already populated OctetDB directory")
	flag.StringVar(&cfg.Contention, "contention", "uniform", "uniform, hotset, or hotkey")
	flag.StringVar(&cfg.Mix, "mix", "70r20w10q", "W6 mix: 70r20w10q or 50r40w10q")
	flag.StringVar(&cfg.QueryOp, "query-op", "mixed", "W5: mixed, point, filter, take, map, or count")
	flag.StringVar(&cfg.Selectivity, "selectivity", "25", "W5: early, 1, 10, 25, 50, 100, or none")
	flag.StringVar(&cfg.DataPath, "data", "", "OctetDB directory")
	flag.StringVar(&cfg.PostgresURL, "postgres", os.Getenv("PERF_M4_POSTGRES_URL"), "PostgreSQL URL")
	flag.StringVar(&output, "output", "", "JSON output file; stdout if empty")
	flag.StringVar(&cpuProfile, "cpu-profile", "", "write a CPU profile for the measured interval")
	flag.StringVar(&heapProfile, "heap-profile", "", "write an in-use heap profile after measurement")
	flag.Parse()

	if err := validate(cfg); err != nil {
		fatal(err)
	}
	if cfg.DataPath == "" {
		cfg.DataPath = filepath.Join(os.TempDir(), "octetdb-perf-m4", fmt.Sprintf("%s-%s-%d", cfg.Lane, cfg.Workload, time.Now().UnixNano()))
	}
	ctx := context.Background()
	var b backend
	var err error
	switch cfg.Lane {
	case "default":
		b, err = newOctetBackend(cfg)
	case "specialized":
		b, err = newSpecializedBackend(cfg)
	case "postgres":
		b, err = newPostgresBackend(ctx, cfg)
	default:
		err = fmt.Errorf("unsupported lane %q", cfg.Lane)
	}
	if err != nil {
		fatal(err)
	}
	defer b.Close()
	if err := b.Setup(ctx); err != nil {
		fatal(fmt.Errorf("setup: %w", err))
	}
	for i := 0; i < cfg.Warmup; i++ {
		if err := b.Operation(ctx, -cfg.Warmup+i); err != nil {
			fatal(fmt.Errorf("warmup: %w", err))
		}
	}
	correct, err := b.Verify(ctx)
	if err != nil {
		fatal(fmt.Errorf("pre-run correctness: %w", err))
	}
	if !allTrue(correct) {
		fatal(fmt.Errorf("pre-run correctness failed: %v", correct))
	}
	b.ResetMetrics()
	var cpuFile *os.File
	if cpuProfile != "" {
		if err := os.MkdirAll(filepath.Dir(cpuProfile), 0o755); err != nil {
			fatal(err)
		}
		cpuFile, err = os.Create(cpuProfile)
		if err != nil {
			fatal(err)
		}
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			fatal(err)
		}
	}

	storageBefore, _ := b.StorageBytes()
	walBefore, _ := b.WALBytes()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	processBefore := readProcessMetrics()
	latencies := make([]int64, cfg.Operations)
	startNS := benchNowNS()
	var next atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	for worker := 0; worker < cfg.Concurrency; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= cfg.Operations {
					return
				}
				t0 := benchNowNS()
				if err := b.Operation(ctx, i); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				latencies[i] = benchNowNS() - t0
			}
		}()
	}
	wg.Wait()
	duration := time.Duration(benchNowNS() - startNS)
	if cpuFile != nil {
		pprof.StopCPUProfile()
		_ = cpuFile.Close()
	}
	if value := firstErr.Load(); value != nil {
		fatal(fmt.Errorf("measurement: %w", value.(error)))
	}
	processAfter := readProcessMetrics()
	runtime.ReadMemStats(&after)
	if heapProfile != "" {
		if err := os.MkdirAll(filepath.Dir(heapProfile), 0o755); err != nil {
			fatal(err)
		}
		file, err := os.Create(heapProfile)
		if err != nil {
			fatal(err)
		}
		if err := pprof.WriteHeapProfile(file); err != nil {
			_ = file.Close()
			fatal(err)
		}
		_ = file.Close()
	}
	storageAfter, _ := b.StorageBytes()
	walAfter, _ := b.WALBytes()
	correct, err = b.Verify(ctx)
	if err != nil {
		fatal(fmt.Errorf("post-run correctness: %w", err))
	}
	if !allTrue(correct) {
		fatal(fmt.Errorf("post-run correctness failed: %v", correct))
	}

	sorted := append([]int64(nil), latencies...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	r := result{
		SchemaVersion: 1, Timestamp: time.Now().UTC(), Config: cfg,
		DurationNS: duration.Nanoseconds(), Throughput: float64(cfg.Operations) / duration.Seconds(),
		P50NS: percentile(sorted, .50), P95NS: percentile(sorted, .95), P99NS: percentile(sorted, .99), MaxNS: sorted[len(sorted)-1],
		CPUUserNS: processAfter.UserNS - processBefore.UserNS, CPUKernelNS: processAfter.KernelNS - processBefore.KernelNS,
		RSSBytes: processAfter.RSSBytes, HeapAllocBytes: after.HeapAlloc, HeapObjects: after.HeapObjects,
		TotalAllocDelta: after.TotalAlloc - before.TotalAlloc, MallocDelta: after.Mallocs - before.Mallocs,
		GCCyclesDelta: after.NumGC - before.NumGC, GCPauseDeltaNS: after.PauseTotalNs - before.PauseTotalNs,
		StorageBeforeBytes: storageBefore, StorageAfterBytes: storageAfter, RecordsExamined: b.RecordsExamined(), Correctness: correct,
		WALBeforeBytes: walBefore, WALAfterBytes: walAfter,
		Metadata: map[string]string{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "octetdb": "github.com/yuechen-li-dev/octetdb@v0.2.0"},
	}
	for key, value := range b.Metadata() {
		r.Metadata[key] = value
	}
	if cfg.Operations > 0 {
		r.StorageDeltaPerOp = float64(storageAfter-storageBefore) / float64(cfg.Operations)
		r.WALBytesPerOp = float64(walAfter-walBefore) / float64(cfg.Operations)
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fatal(err)
	}
	encoded = append(encoded, '\n')
	if output == "" {
		_, err = os.Stdout.Write(encoded)
	} else {
		err = os.MkdirAll(filepath.Dir(output), 0o755)
		if err == nil {
			err = os.WriteFile(output, encoded, 0o644)
		}
	}
	if err != nil {
		fatal(err)
	}
}

func validate(c config) error {
	if c.Population < 2 || c.Operations < 1 || c.Concurrency < 1 || c.Warmup < 0 {
		return errors.New("population >= 2, operations/concurrency >= 1, and warmup >= 0 are required")
	}
	if c.Workload < "w1" || c.Workload > "w6" {
		return fmt.Errorf("invalid workload %q", c.Workload)
	}
	if c.Contention != "uniform" && c.Contention != "hotset" && c.Contention != "hotkey" {
		return fmt.Errorf("invalid contention %q", c.Contention)
	}
	validQuery := map[string]bool{"mixed": true, "point": true, "filter": true, "take": true, "map": true, "count": true}
	validSelectivity := map[string]bool{"early": true, "1": true, "10": true, "25": true, "50": true, "100": true, "none": true}
	if !validQuery[c.QueryOp] || !validSelectivity[c.Selectivity] {
		return errors.New("invalid W5 query-op or selectivity")
	}
	if c.Lane == "postgres" && c.PostgresURL == "" {
		return errors.New("-postgres or PERF_M4_POSTGRES_URL is required")
	}
	return nil
}

func percentile(sorted []int64, q float64) int64 { return sorted[int(q*float64(len(sorted)-1))] }
func allTrue(values map[string]bool) bool {
	for _, value := range values {
		if !value {
			return false
		}
	}
	return true
}
func fatal(err error) { fmt.Fprintln(os.Stderr, "perfm4:", err); os.Exit(1) }

func choosePair(c config, operation int) (int, int) {
	r := rand.New(rand.NewSource(int64(operation) + 0x4d34))
	switch c.Contention {
	case "hotkey":
		return 0, 1 + r.Intn(c.Population-1)
	case "hotset":
		n := min(c.Population, 16)
		a := r.Intn(n)
		b := r.Intn(n - 1)
		if b >= a {
			b++
		}
		return a, b
	default:
		a := r.Intn(c.Population)
		b := r.Intn(c.Population - 1)
		if b >= a {
			b++
		}
		return a, b
	}
}
