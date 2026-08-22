package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	rmetrics "runtime/metrics"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	tb "github.com/tigerbeetle/tigerbeetle-go"
	"github.com/yuechen-li-dev/database-scheduler/internal/m7write"
)

const initialBalance = uint64(1_000_000_000_000)

type config struct {
	Lane        string
	Durability  string
	Workload    string
	Topology    string
	Address     string
	DSN         string
	StorageDir  string
	Output      string
	CPUProfile  string
	HeapProfile string
	ServerPID   int
	SkipSetup   bool
	Warmup      int
	Operations  int
	Accounts    int
	Batch       int
	Seed        int64
	Duration    time.Duration
	GroupWait   time.Duration
}

type report struct {
	GeneratedAt string       `json:"generated_at"`
	GoVersion   string       `json:"go_version"`
	GOOS        string       `json:"goos"`
	GOARCH      string       `json:"goarch"`
	GOMAXPROCS  int          `json:"gomaxprocs"`
	Config      configReport `json:"config"`
	Result      result       `json:"result"`
}

type configReport struct {
	Lane           string `json:"lane"`
	Durability     string `json:"durability"`
	Workload       string `json:"workload"`
	BatchSemantics string `json:"batch_semantics"`
	Topology       string `json:"topology"`
	Operations     int    `json:"operations"`
	Accounts       int    `json:"accounts"`
	OfferedBatch   int    `json:"offered_batch"`
	Seed           int64  `json:"seed"`
	Duration       string `json:"duration,omitempty"`
	Warmup         int    `json:"warmup_operations"`
}

type result struct {
	Operations          int               `json:"operations"`
	Batches             int               `json:"batches"`
	ElapsedSeconds      float64           `json:"elapsed_seconds"`
	OperationsPerSecond float64           `json:"operations_per_second"`
	BatchesPerSecond    float64           `json:"batches_per_second"`
	P50Micros           float64           `json:"p50_us"`
	P95Micros           float64           `json:"p95_us"`
	P99Micros           float64           `json:"p99_us"`
	P999Micros          float64           `json:"p99_9_us"`
	MaxMicros           float64           `json:"max_us"`
	CPUSeconds          float64           `json:"go_runtime_cpu_seconds"`
	ProcessCPUSeconds   float64           `json:"process_cpu_seconds"`
	ProcessCPUCores     float64           `json:"process_cpu_cores"`
	ExternalProcess     processResult     `json:"external_process"`
	GC                  gcResult          `json:"gc"`
	Storage             storageResult     `json:"storage"`
	Correctness         correctnessResult `json:"correctness"`
	TimeSeries          []timePoint       `json:"time_series,omitempty"`
}

type timePoint struct {
	Seconds             float64 `json:"seconds"`
	Operations          int     `json:"operations"`
	OperationsPerSecond float64 `json:"operations_per_second"`
	P99Micros           float64 `json:"p99_us"`
	GCCycles            uint32  `json:"gc_cycles"`
	LiveHeapBytes       uint64  `json:"live_heap_bytes"`
	RSSBytes            uint64  `json:"rss_bytes"`
	WALBytes            uint64  `json:"wal_bytes"`
}

type gcResult struct {
	AllocatedBytes   uint64  `json:"allocated_bytes"`
	Allocations      uint64  `json:"allocations"`
	BytesPerOp       float64 `json:"bytes_per_op"`
	AllocsPerOp      float64 `json:"allocs_per_op"`
	GCCycles         uint32  `json:"gc_cycles"`
	GCCPUSeconds     float64 `json:"gc_cpu_seconds"`
	GCCPUPercent     float64 `json:"gc_cpu_percent_of_go_capacity"`
	PauseTotalMicros float64 `json:"pause_total_us"`
	MaxPauseMicros   float64 `json:"max_pause_us"`
	LiveHeapBytes    uint64  `json:"live_heap_bytes"`
	HeapGoalBytes    uint64  `json:"heap_goal_bytes"`
	RSSBytes         uint64  `json:"rss_bytes"`
}

type storageResult struct {
	WALBytes        uint64  `json:"wal_bytes"`
	WALBytesPerOp   float64 `json:"wal_bytes_per_op"`
	Syncs           uint64  `json:"syncs"`
	CommandsPerSync float64 `json:"commands_per_sync"`
}

type processResult struct {
	PID           int     `json:"pid,omitempty"`
	CPUSeconds    float64 `json:"cpu_seconds"`
	UtilizedCores float64 `json:"utilized_cores"`
	RSSBytes      uint64  `json:"rss_bytes"`
}

type correctnessResult struct {
	Conserved           bool   `json:"conserved"`
	DuplicateSuppressed bool   `json:"duplicate_suppressed"`
	Accepted            int    `json:"accepted"`
	Rejected            int    `json:"rejected"`
	StateDigest         string `json:"state_digest"`
	Detail              string `json:"detail"`
}

type logicalTransfer struct {
	id                  uint64
	source, destination int
	amount              uint64
}

type executor interface {
	setup(context.Context) error
	executeBatch(context.Context, []logicalTransfer) (int, int, error)
	verify(context.Context, logicalTransfer) (correctnessResult, error)
	storage() storageResult
	close() error
}

func main() {
	var c config
	flag.StringVar(&c.Lane, "lane", "oct", "oct, go, tiger, or postgres")
	flag.StringVar(&c.Durability, "durability", "group", "memory, sync_each, or group")
	flag.StringVar(&c.Workload, "workload", "independent", "independent, hot_source, hot_destination, or hotset")
	flag.StringVar(&c.Topology, "topology", "in_process", "reported process/network topology")
	flag.StringVar(&c.Address, "tiger-address", "127.0.0.1:3000", "TigerBeetle address")
	flag.StringVar(&c.DSN, "postgres-dsn", "", "PostgreSQL DSN")
	flag.StringVar(&c.StorageDir, "storage-dir", "", "OctetDB storage directory (temporary when empty)")
	flag.StringVar(&c.Output, "out", "", "JSON output path")
	flag.StringVar(&c.CPUProfile, "cpu-profile", "", "CPU profile path")
	flag.StringVar(&c.HeapProfile, "heap-profile", "", "heap profile path")
	flag.IntVar(&c.ServerPID, "server-pid", 0, "external server PID to sample on Linux")
	flag.BoolVar(&c.SkipSetup, "skip-setup", false, "use an existing populated lane (recovery probes)")
	flag.IntVar(&c.Warmup, "warmup", 1000, "excluded logical transfer warm-up")
	flag.IntVar(&c.Operations, "operations", 10000, "logical transfers")
	flag.IntVar(&c.Accounts, "accounts", 1000, "account population")
	flag.IntVar(&c.Batch, "batch", 1, "offered batch/burst size")
	flag.Int64Var(&c.Seed, "seed", 8128, "deterministic seed and ID namespace")
	flag.DurationVar(&c.Duration, "duration", 0, "run for at least this duration instead of a fixed operation count")
	flag.DurationVar(&c.GroupWait, "group-wait", 200*time.Microsecond, "OctetDB internal group wait")
	flag.Parse()
	if err := run(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(c config) error {
	if c.Accounts < 2 || c.Batch < 1 || c.Operations < 1 {
		return errors.New("accounts >= 2, batch >= 1, and operations >= 1 are required")
	}
	ctx := context.Background()
	exec, cleanup, batchSemantics, err := openExecutor(ctx, c)
	if err != nil {
		return err
	}
	defer cleanup()
	defer exec.close()
	if !c.SkipSetup {
		if err := exec.setup(ctx); err != nil {
			return fmt.Errorf("setup: %w", err)
		}
	}
	if c.Warmup > 0 {
		warm := c
		warm.Seed += 1_000_000
		for offset := 0; offset < c.Warmup; offset += c.Batch {
			n := min(c.Batch, c.Warmup-offset)
			accepted, rejected, err := exec.executeBatch(ctx, makeTransfers(warm, offset, n))
			if err != nil || accepted != n || rejected != 0 {
				return fmt.Errorf("warm-up at %d: accepted=%d rejected=%d err=%v", offset, accepted, rejected, err)
			}
		}
	}
	if c.CPUProfile != "" {
		f, err := createFile(c.CPUProfile)
		if err != nil {
			return err
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			f.Close()
			return err
		}
		defer func() { pprof.StopCPUProfile(); f.Close() }()
	}
	runtime.GC()
	before := readRuntimeSample()
	beforeProcess := readProcessSample(os.Getpid())
	beforeExternal := readProcessSample(c.ServerPID)
	beforeStorage := exec.storage()
	latencies := make([]int64, 0, c.Operations)
	accepted, rejected, operations, batches := 0, 0, 0, 0
	start := time.Now()
	lastPointAt, lastPointOps := start, 0
	intervalLatencies := make([]int64, 0, 4096)
	var timeSeries []timePoint
	var first logicalTransfer
	for operations < c.Operations || (c.Duration > 0 && time.Since(start) < c.Duration) {
		n := c.Batch
		if c.Duration == 0 && operations+n > c.Operations {
			n = c.Operations - operations
		}
		transfers := makeTransfers(c, operations, n)
		if operations == 0 {
			first = transfers[0]
		}
		at := time.Now()
		a, r, err := exec.executeBatch(ctx, transfers)
		elapsed := time.Since(at).Nanoseconds()
		if err != nil {
			return fmt.Errorf("execute batch %d: %w", batches, err)
		}
		for range transfers {
			latencies = append(latencies, elapsed)
			intervalLatencies = append(intervalLatencies, elapsed)
		}
		accepted += a
		rejected += r
		operations += len(transfers)
		batches++
		if c.Duration > 0 && time.Since(lastPointAt) >= time.Second {
			now := time.Now()
			sample := readRuntimeSample()
			pointStorage := exec.storage()
			sort.Slice(intervalLatencies, func(i, j int) bool { return intervalLatencies[i] < intervalLatencies[j] })
			timeSeries = append(timeSeries, timePoint{Seconds: now.Sub(start).Seconds(), Operations: operations, OperationsPerSecond: float64(operations-lastPointOps) / now.Sub(lastPointAt).Seconds(), P99Micros: percentile(intervalLatencies, .99), GCCycles: sample.numGC - before.numGC, LiveHeapBytes: sample.liveHeap, RSSBytes: sample.rss, WALBytes: pointStorage.WALBytes - beforeStorage.WALBytes})
			lastPointAt, lastPointOps = now, operations
			intervalLatencies = intervalLatencies[:0]
		}
	}
	elapsed := time.Since(start)
	after := readRuntimeSample()
	afterProcess := readProcessSample(os.Getpid())
	afterExternal := readProcessSample(c.ServerPID)
	afterStorage := exec.storage()
	correct, err := exec.verify(ctx, first)
	if err != nil {
		return fmt.Errorf("verify: %w", err)
	}
	correct.Accepted, correct.Rejected = accepted, rejected
	if rejected != 0 {
		correct.Conserved = false
		correct.Detail += fmt.Sprintf("; %d workload transfers rejected", rejected)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	storage := storageResult{WALBytes: afterStorage.WALBytes - beforeStorage.WALBytes, Syncs: afterStorage.Syncs - beforeStorage.Syncs}
	storage.WALBytesPerOp = float64(storage.WALBytes) / float64(operations)
	if storage.Syncs > 0 {
		storage.CommandsPerSync = float64(operations) / float64(storage.Syncs)
	}
	totalCPU := after.totalCPU - before.totalCPU
	gcCPU := after.gcCPU - before.gcCPU
	gcr := gcResult{AllocatedBytes: after.totalAlloc - before.totalAlloc, Allocations: after.mallocs - before.mallocs, GCCycles: after.numGC - before.numGC, GCCPUSeconds: gcCPU, PauseTotalMicros: float64(after.pauseTotal-before.pauseTotal) / 1000, MaxPauseMicros: maxPauseSince(after.pauses, before.numGC, after.numGC) / 1000, LiveHeapBytes: after.liveHeap, HeapGoalBytes: after.heapGoal, RSSBytes: after.rss}
	gcr.BytesPerOp = float64(gcr.AllocatedBytes) / float64(operations)
	gcr.AllocsPerOp = float64(gcr.Allocations) / float64(operations)
	if totalCPU > 0 {
		gcr.GCCPUPercent = 100 * gcCPU / totalCPU
	}
	processCPU := afterProcess.cpuSeconds - beforeProcess.cpuSeconds
	externalCPU := afterExternal.cpuSeconds - beforeExternal.cpuSeconds
	r := result{Operations: operations, Batches: batches, ElapsedSeconds: elapsed.Seconds(), OperationsPerSecond: float64(operations) / elapsed.Seconds(), BatchesPerSecond: float64(batches) / elapsed.Seconds(), P50Micros: percentile(latencies, .5), P95Micros: percentile(latencies, .95), P99Micros: percentile(latencies, .99), P999Micros: percentile(latencies, .999), MaxMicros: float64(latencies[len(latencies)-1]) / 1000, CPUSeconds: totalCPU, ProcessCPUSeconds: processCPU, ProcessCPUCores: processCPU / elapsed.Seconds(), ExternalProcess: processResult{PID: c.ServerPID, CPUSeconds: externalCPU, UtilizedCores: externalCPU / elapsed.Seconds(), RSSBytes: afterExternal.rss}, GC: gcr, Storage: storage, Correctness: correct, TimeSeries: timeSeries}
	rep := report{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), Config: configReport{Lane: c.Lane, Durability: c.Durability, Workload: c.Workload, BatchSemantics: batchSemantics, Topology: c.Topology, Operations: operations, Accounts: c.Accounts, OfferedBatch: c.Batch, Seed: c.Seed, Duration: c.Duration.String(), Warmup: c.Warmup}, Result: r}
	data, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	if c.Output != "" {
		if err := os.MkdirAll(filepath.Dir(c.Output), 0755); err != nil {
			return err
		}
		if err := os.WriteFile(c.Output, append(data, '\n'), 0644); err != nil {
			return err
		}
	}
	fmt.Println(string(data))
	if c.HeapProfile != "" {
		f, err := createFile(c.HeapProfile)
		if err != nil {
			return err
		}
		defer f.Close()
		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			return err
		}
	}
	return nil
}

type runtimeSample struct {
	totalAlloc, mallocs, pauseTotal, liveHeap, heapGoal, rss uint64
	numGC                                                    uint32
	gcCPU, totalCPU                                          float64
	pauses                                                   []uint64
}

func readRuntimeSample() runtimeSample {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	names := []string{"/cpu/classes/gc/total:cpu-seconds", "/cpu/classes/total:cpu-seconds", "/gc/heap/live:bytes", "/gc/heap/goal:bytes"}
	samples := make([]rmetrics.Sample, len(names))
	for i, n := range names {
		samples[i].Name = n
	}
	rmetrics.Read(samples)
	s := runtimeSample{totalAlloc: m.TotalAlloc, mallocs: m.Mallocs, pauseTotal: m.PauseTotalNs, numGC: m.NumGC, pauses: append([]uint64(nil), m.PauseNs[:]...), rss: readRSS()}
	for _, x := range samples {
		switch x.Name {
		case names[0]:
			s.gcCPU = x.Value.Float64()
		case names[1]:
			s.totalCPU = x.Value.Float64()
		case names[2]:
			s.liveHeap = x.Value.Uint64()
		case names[3]:
			s.heapGoal = x.Value.Uint64()
		}
	}
	return s
}
func maxPauseSince(pauses []uint64, before, after uint32) float64 {
	var max uint64
	for i := before; i < after; i++ {
		v := pauses[i%uint32(len(pauses))]
		if v > max {
			max = v
		}
	}
	return float64(max)
}
func readRSS() uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			f := strings.Fields(line)
			if len(f) >= 2 {
				v, _ := strconv.ParseUint(f[1], 10, 64)
				return v * 1024
			}
		}
	}
	return 0
}

type processSample struct {
	cpuSeconds float64
	rss        uint64
}

func readProcessSample(pid int) processSample {
	if pid <= 0 {
		return processSample{}
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return processSample{}
	}
	// Fields after the final ')' begin with field 3 (state). utime/stime are
	// therefore indexes 11/12. Linux uses 100 clock ticks/s on this benchmark
	// host; the environment evidence records getconf CLK_TCK explicitly.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return processSample{}
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 13 {
		return processSample{}
	}
	userTicks, _ := strconv.ParseUint(fields[11], 10, 64)
	systemTicks, _ := strconv.ParseUint(fields[12], 10, 64)
	return processSample{cpuSeconds: float64(userTicks+systemTicks) / 100, rss: readProcessRSS(pid)}
}

func readProcessRSS(pid int) uint64 {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				value, _ := strconv.ParseUint(fields[1], 10, 64)
				return value * 1024
			}
		}
	}
	return 0
}
func percentile(v []int64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(v)))) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(v) {
		i = len(v) - 1
	}
	return float64(v[i]) / 1000
}
func createFile(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func makeTransfers(c config, offset, n int) []logicalTransfer {
	out := make([]logicalTransfer, n)
	hot := 16
	if c.Accounts < hot {
		hot = c.Accounts
	}
	for i := 0; i < n; i++ {
		k := offset + i
		a := 1 + (2*k)%c.Accounts
		b := 1 + (2*k+1)%c.Accounts
		switch c.Workload {
		case "independent":
		case "hot_source":
			a = 1
			b = 2 + k%(c.Accounts-1)
		case "hot_destination":
			a = 2 + k%(c.Accounts-1)
			b = 1
		case "hotset":
			a = 1 + k%hot
			b = 1 + (k*7+3)%hot
			if a == b {
				b = 1 + b%hot
			}
		default:
			panic("unknown workload " + c.Workload)
		}
		out[i] = logicalTransfer{id: uint64(c.Seed)*1_000_000_000 + 10_000_000 + uint64(k+1), source: a, destination: b, amount: 1}
	}
	return out
}

func openExecutor(ctx context.Context, c config) (executor, func(), string, error) {
	switch c.Lane {
	case "oct", "go":
		dir := c.StorageDir
		cleanup := func() {}
		if dir == "" {
			var err error
			dir, err = os.MkdirTemp("", "tigercompare-oct-")
			if err != nil {
				return nil, nil, "", err
			}
			cleanup = func() { os.RemoveAll(dir) }
		}
		mode := m7write.BatchSync
		if c.Durability == "memory" {
			mode = m7write.MemoryOnly
		} else if c.Durability == "sync_each" {
			mode = m7write.SyncEach
		} else if c.Durability != "group" {
			cleanup()
			return nil, nil, "", fmt.Errorf("unknown durability %q", c.Durability)
		}
		cfg := m7write.Config{StorageDir: dir, Durability: mode, SegmentRecords: 4096, GroupMax: c.Batch, GroupWait: c.GroupWait, DedupeWindow: max(c.Operations+c.Accounts+100, 100000), MailboxCapacity: max(c.Batch*2, 256)}
		var e *m7write.Engine
		var err error
		if c.Lane == "go" {
			e, err = m7write.OpenGoM1Baseline(cfg)
		} else {
			e, err = m7write.Open(cfg)
		}
		if err != nil {
			cleanup()
			return nil, nil, "", err
		}
		return &octExecutor{e: e, accounts: c.Accounts}, cleanup, "harness concurrent burst of per-command Submit calls; internal group commit bounded by offered batch", nil
	case "tiger":
		client, err := tb.NewClient(tb.ToUint128(0), []string{c.Address})
		if err != nil {
			return nil, nil, "", err
		}
		return &tigerExecutor{client: client, accounts: c.Accounts, seed: c.Seed}, func() {}, "one homogeneous CreateTransfers client request", nil
	case "postgres":
		if c.DSN == "" {
			return nil, nil, "", errors.New("postgres-dsn is required")
		}
		p, err := m7write.OpenPostgreSQL(ctx, c.DSN)
		if err != nil {
			return nil, nil, "", err
		}
		if err := p.Reset(ctx); err != nil {
			p.Close()
			return nil, nil, "", err
		}
		return &postgresExecutor{p: p, accounts: c.Accounts}, func() {}, "harness concurrent burst; each logical transfer is a separate SQL transaction", nil
	default:
		return nil, nil, "", fmt.Errorf("unknown lane %q", c.Lane)
	}
}

type octExecutor struct {
	e        *m7write.Engine
	accounts int
}

func (o *octExecutor) setup(ctx context.Context) error {
	const setupBurst = 256
	for start := 1; start <= o.accounts; start += setupBurst {
		end := min(start+setupBurst, o.accounts+1)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var first error
		for i := start; i < end; i++ {
			i := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, err := o.e.Submit(ctx, m7write.Command{ID: fmt.Sprintf("setup-%d", i), Kind: m7write.Create, Account: m7write.AccountID(i), Amount: int(initialBalance)})
				if err != nil || !r.Accepted {
					mu.Lock()
					if first == nil {
						first = fmt.Errorf("create %d: result=%+v err=%v", i, r, err)
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
func (o *octExecutor) executeBatch(ctx context.Context, x []logicalTransfer) (int, int, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	a, r := 0, 0
	var first error
	for _, t := range x {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := o.e.Submit(ctx, m7write.Command{ID: fmt.Sprintf("x-%d", t.id), Kind: m7write.Transfer, Account: m7write.AccountID(t.source), Other: m7write.AccountID(t.destination), Amount: int(t.amount)})
			mu.Lock()
			defer mu.Unlock()
			if err != nil && first == nil {
				first = err
			}
			if res.Accepted {
				a++
			} else {
				r++
			}
		}()
	}
	wg.Wait()
	return a, r, first
}
func (o *octExecutor) verify(ctx context.Context, t logicalTransfer) (correctnessResult, error) {
	res, err := o.e.Submit(ctx, m7write.Command{ID: fmt.Sprintf("x-%d", t.id), Kind: m7write.Transfer, Account: m7write.AccountID(t.source), Other: m7write.AccountID(t.destination), Amount: int(t.amount)})
	if err != nil {
		return correctnessResult{}, err
	}
	h := sha256.New()
	var total uint64
	for i := 1; i <= o.accounts; i++ {
		a, ok := o.e.Store().Account(m7write.AccountID(i))
		if !ok {
			return correctnessResult{}, fmt.Errorf("account %d missing", i)
		}
		total += uint64(a.Balance)
		fmt.Fprintf(h, "%d:%d;", i, a.Balance)
	}
	expected := uint64(o.accounts) * initialBalance
	return correctnessResult{Conserved: total == expected, DuplicateSuppressed: res.Duplicate, StateDigest: hex.EncodeToString(h.Sum(nil)), Detail: fmt.Sprintf("sum=%d expected=%d", total, expected)}, nil
}
func (o *octExecutor) storage() storageResult {
	s := o.e.StorageMetrics()
	return storageResult{WALBytes: s.WALBytesWritten, Syncs: s.Syncs}
}
func (o *octExecutor) close() error { return o.e.Close() }

type tigerExecutor struct {
	client   tb.Client
	accounts int
	seed     int64
}

func (t *tigerExecutor) setup(ctx context.Context) error {
	_ = ctx
	// Leave room below the protocol's nominal event maximum for release-specific
	// request framing; 8,000 is accepted by the 0.17.9 Go client and server.
	const maxBatch = 8000
	all := make([]tb.Account, 0, t.accounts+1)
	all = append(all, tb.Account{ID: tb.ToUint128(1), Ledger: 1, Code: 1})
	flags := tb.AccountFlags{DebitsMustNotExceedCredits: true}.ToUint16()
	for i := 1; i <= t.accounts; i++ {
		all = append(all, tb.Account{ID: tb.ToUint128(uint64(i + 1)), Ledger: 1, Code: 1, Flags: flags})
	}
	for start := 0; start < len(all); start += maxBatch {
		end := min(start+maxBatch, len(all))
		rs, err := t.client.CreateAccounts(all[start:end])
		if err != nil {
			return err
		}
		for _, r := range rs {
			if r.Status != tb.AccountCreated {
				return fmt.Errorf("create account: %s", r.Status)
			}
		}
	}
	fund := make([]tb.Transfer, t.accounts)
	for i := range fund {
		fund[i] = tb.Transfer{ID: tb.ToUint128(uint64(t.seed)*1_000_000_000 + uint64(i+1)), DebitAccountID: tb.ToUint128(1), CreditAccountID: tb.ToUint128(uint64(i + 2)), Amount: tb.ToUint128(initialBalance), Ledger: 1, Code: 1}
	}
	for start := 0; start < len(fund); start += maxBatch {
		end := min(start+maxBatch, len(fund))
		rs, err := t.client.CreateTransfers(fund[start:end])
		if err != nil {
			return err
		}
		for _, r := range rs {
			if r.Status != tb.TransferCreated {
				return fmt.Errorf("fund: %s", r.Status)
			}
		}
	}
	return nil
}
func (t *tigerExecutor) executeBatch(ctx context.Context, x []logicalTransfer) (int, int, error) {
	_ = ctx
	batch := make([]tb.Transfer, len(x))
	for i, v := range x {
		batch[i] = tb.Transfer{ID: tb.ToUint128(v.id), DebitAccountID: tb.ToUint128(uint64(v.source + 1)), CreditAccountID: tb.ToUint128(uint64(v.destination + 1)), Amount: tb.ToUint128(v.amount), Ledger: 1, Code: 1}
	}
	rs, err := t.client.CreateTransfers(batch)
	if err != nil {
		return 0, 0, err
	}
	a, r := 0, 0
	for _, x := range rs {
		if x.Status == tb.TransferCreated {
			a++
		} else {
			r++
		}
	}
	return a, r, nil
}
func (t *tigerExecutor) verify(ctx context.Context, x logicalTransfer) (correctnessResult, error) {
	_ = ctx
	dup := tb.Transfer{ID: tb.ToUint128(x.id), DebitAccountID: tb.ToUint128(uint64(x.source + 1)), CreditAccountID: tb.ToUint128(uint64(x.destination + 1)), Amount: tb.ToUint128(x.amount), Ledger: 1, Code: 1}
	rs, err := t.client.CreateTransfers([]tb.Transfer{dup})
	if err != nil {
		return correctnessResult{}, err
	}
	suppressed := len(rs) == 1 && rs[0].Status == tb.TransferExists
	h := sha256.New()
	var netSum uint64
	const maxBatch = 8000
	for start := 0; start < t.accounts; start += maxBatch {
		end := min(start+maxBatch, t.accounts)
		ids := make([]tb.Uint128, end-start)
		for i := range ids {
			ids[i] = tb.ToUint128(uint64(start + i + 2))
		}
		accounts, err := t.client.LookupAccounts(ids)
		if err != nil {
			return correctnessResult{}, err
		}
		if len(accounts) != len(ids) {
			return correctnessResult{}, fmt.Errorf("lookup returned %d/%d", len(accounts), len(ids))
		}
		for i, a := range accounts {
			debit, _ := a.DebitsPosted.Uint64()
			credit, _ := a.CreditsPosted.Uint64()
			net := credit - debit
			netSum += net
			fmt.Fprintf(h, "%d:%d;", start+i+1, net)
		}
	}
	expected := uint64(t.accounts) * initialBalance
	return correctnessResult{Conserved: netSum == expected, DuplicateSuppressed: suppressed, StateDigest: hex.EncodeToString(h.Sum(nil)), Detail: fmt.Sprintf("user account net sum=%d expected=%d", netSum, expected)}, nil
}
func (t *tigerExecutor) storage() storageResult { return storageResult{} }
func (t *tigerExecutor) close() error           { t.client.Close(); return nil }

type postgresExecutor struct {
	p        *m7write.PostgreSQLBaseline
	accounts int
}

func (p *postgresExecutor) setup(ctx context.Context) error {
	for i := 1; i <= p.accounts; i++ {
		r, err := p.p.Execute(ctx, m7write.Command{ID: fmt.Sprintf("setup-%d", i), Kind: m7write.Create, Account: m7write.AccountID(i), Amount: int(initialBalance)})
		if err != nil || !r.Accepted {
			return fmt.Errorf("create %d: result=%+v err=%v", i, r, err)
		}
	}
	return nil
}
func (p *postgresExecutor) executeBatch(ctx context.Context, x []logicalTransfer) (int, int, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	a, r := 0, 0
	var first error
	for _, t := range x {
		t := t
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := p.p.Execute(ctx, m7write.Command{ID: fmt.Sprintf("x-%d", t.id), Kind: m7write.Transfer, Account: m7write.AccountID(t.source), Other: m7write.AccountID(t.destination), Amount: int(t.amount)})
			mu.Lock()
			defer mu.Unlock()
			if err != nil && first == nil {
				first = err
			}
			if res.Accepted {
				a++
			} else {
				r++
			}
		}()
	}
	wg.Wait()
	return a, r, first
}
func (p *postgresExecutor) verify(ctx context.Context, t logicalTransfer) (correctnessResult, error) {
	res, err := p.p.Execute(ctx, m7write.Command{ID: fmt.Sprintf("x-%d", t.id), Kind: m7write.Transfer, Account: m7write.AccountID(t.source), Other: m7write.AccountID(t.destination), Amount: int(t.amount)})
	if err != nil {
		return correctnessResult{}, err
	}
	h := sha256.New()
	var total uint64
	for i := 1; i <= p.accounts; i++ {
		a, err := p.p.Account(ctx, m7write.AccountID(i))
		if err != nil {
			return correctnessResult{}, err
		}
		total += uint64(a.Balance)
		fmt.Fprintf(h, "%d:%d;", i, a.Balance)
	}
	expected := uint64(p.accounts) * initialBalance
	return correctnessResult{Conserved: total == expected, DuplicateSuppressed: res.Duplicate, StateDigest: hex.EncodeToString(h.Sum(nil)), Detail: fmt.Sprintf("sum=%d expected=%d", total, expected)}, nil
}
func (p *postgresExecutor) storage() storageResult { return storageResult{} }
func (p *postgresExecutor) close() error           { p.p.Close(); return nil }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
