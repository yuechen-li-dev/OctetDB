package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	m7write "github.com/yuechen-li-dev/octetdb/internal/researchengine"
)

type report struct {
	GeneratedAt string   `json:"generated_at"`
	GoVersion   string   `json:"go_version"`
	OS          string   `json:"os"`
	Arch        string   `json:"arch"`
	CPUs        int      `json:"cpus"`
	Seed        int64    `json:"seed"`
	Operations  int      `json:"operations"`
	Results     []result `json:"results"`
}

type result struct {
	Lane              string  `json:"lane"`
	Workload          string  `json:"workload"`
	Durability        string  `json:"durability"`
	Operations        int     `json:"operations"`
	Concurrency       int     `json:"concurrency"`
	CommandsPerSecond float64 `json:"commands_per_second"`
	P50Micros         float64 `json:"p50_us"`
	P95Micros         float64 `json:"p95_us"`
	P99Micros         float64 `json:"p99_us"`
	SyncsPerSecond    float64 `json:"syncs_per_second"`
	CommandsPerSync   float64 `json:"commands_per_sync"`
	WALBytesPerOp     float64 `json:"wal_bytes_per_op"`
	AllocsPerOp       float64 `json:"allocs_per_op"`
	BytesPerOp        float64 `json:"bytes_per_op"`
	FinalBalance      int     `json:"final_total_balance"`
}

func main() {
	out := flag.String("out", "", "JSON output path")
	operations := flag.Int("operations", 2000, "operations per workload; fsync each is capped at 500")
	accounts := flag.Int("accounts", 128, "accounts")
	workers := flag.Int("workers", 16, "concurrent submitters")
	batchMatrix := flag.Bool("batch-matrix", false, "run only the bounded group-commit matrix")
	flag.Parse()
	r := report{GeneratedAt: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Seed: 8128, Operations: *operations}
	modes := []struct {
		name  string
		mode  m7write.DurabilityMode
		group int
		wait  time.Duration
	}{
		{"memory", m7write.MemoryOnly, 1, 0},
		{"fsync_each", m7write.SyncEach, 1, 0},
		{"group_16_0us", m7write.BatchSync, 16, 0},
	}
	if *batchMatrix {
		modes = []struct {
			name  string
			mode  m7write.DurabilityMode
			group int
			wait  time.Duration
		}{
			{"group_4_0us", m7write.BatchSync, 4, 0}, {"group_16_0us", m7write.BatchSync, 16, 0}, {"group_64_0us", m7write.BatchSync, 64, 0},
			{"group_16_50us", m7write.BatchSync, 16, 50 * time.Microsecond}, {"group_16_200us", m7write.BatchSync, 16, 200 * time.Microsecond}, {"group_16_1ms", m7write.BatchSync, 16, time.Millisecond},
		}
	}
	for _, mode := range modes {
		n := *operations
		if mode.mode == m7write.SyncEach && n > 500 {
			n = 500
		}
		for _, workload := range []string{"independent", "hot_key", "transfer"} {
			r.Results = append(r.Results, runEngine(false, mode.name, mode.mode, mode.group, mode.wait, workload, n, *accounts, *workers, r.Seed))
			r.Results = append(r.Results, runEngine(true, mode.name, mode.mode, mode.group, mode.wait, workload, n, *accounts, *workers, r.Seed))
		}
	}
	if dsn := os.Getenv("DBSCHED_POSTGRES_DSN"); dsn != "" && !*batchMatrix {
		for _, workload := range []string{"independent", "hot_key", "transfer"} {
			r.Results = append(r.Results, runPostgres(dsn, workload, min(*operations, 500), *accounts, *workers, r.Seed))
		}
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		panic(err)
	}
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(*out, append(data, '\n'), 0o644); err != nil {
			panic(err)
		}
	}
	fmt.Println(string(data))
}

func runEngine(goControl bool, modeName string, mode m7write.DurabilityMode, group int, wait time.Duration, workload string, n, accounts, workers int, seed int64) result {
	dir, _ := os.MkdirTemp("", "octetdb-m8-")
	defer os.RemoveAll(dir)
	cfg := m7write.Config{StorageDir: dir, Durability: mode, SegmentRecords: 4096, GroupMax: group, GroupWait: wait, DedupeWindow: n + accounts + 1, MailboxCapacity: 256}
	var e *m7write.Engine
	var err error
	if goControl {
		e, err = m7write.OpenGoM1Baseline(cfg)
	} else {
		e, err = m7write.Open(cfg)
	}
	if err != nil {
		panic(err)
	}
	defer e.Close()
	setup(func(c m7write.Command) error { _, err := e.Submit(context.Background(), c); return err }, accounts)
	commands := makeCommands(workload, n, accounts, seed)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	storageBefore := e.StorageMetrics()
	latencies, elapsed := executeConcurrent(commands, workers, func(c m7write.Command) error { _, err := e.Submit(context.Background(), c); return err })
	runtime.ReadMemStats(&after)
	storageAfter := e.StorageMetrics()
	syncs := storageAfter.Syncs - storageBefore.Syncs
	wal := storageAfter.WALBytesWritten - storageBefore.WALBytesWritten
	lane := "oct_flow"
	if goControl {
		lane = "go_control"
	}
	return makeResult(lane, workload, modeName, n, workers, latencies, elapsed, syncs, wal, after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc, totalBalance(e.Store(), accounts))
}

func runPostgres(dsn, workload string, n, accounts, workers int, seed int64) result {
	ctx := context.Background()
	p, err := m7write.OpenPostgreSQL(ctx, dsn)
	if err != nil {
		panic(err)
	}
	defer p.Close()
	if err := p.Reset(ctx); err != nil {
		panic(err)
	}
	setup(func(c m7write.Command) error { _, err := p.Execute(ctx, c); return err }, accounts)
	commands := makeCommands(workload, n, accounts, seed)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	latencies, elapsed := executeConcurrent(commands, workers, func(c m7write.Command) error { _, err := p.Execute(ctx, c); return err })
	runtime.ReadMemStats(&after)
	total := 0
	for i := 1; i <= accounts; i++ {
		a, err := p.Account(ctx, m7write.AccountID(i))
		if err != nil {
			panic(err)
		}
		total += a.Balance
	}
	return makeResult("postgresql_17", workload, "postgres_transaction_commit", n, workers, latencies, elapsed, uint64(n), 0, after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc, total)
}

func executeConcurrent(commands []m7write.Command, workers int, execute func(m7write.Command) error) ([]int64, time.Duration) {
	latencies := make([]int64, len(commands))
	jobs := make(chan int)
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	started := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				at := time.Now()
				err := execute(commands[i])
				latencies[i] = time.Since(at).Nanoseconds()
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
				}
			}
		}()
	}
	for i := range commands {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	elapsed := time.Since(started)
	if firstErr != nil {
		panic(firstErr)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	return latencies, elapsed
}

func makeResult(lane, workload, durability string, n, workers int, latencies []int64, elapsed time.Duration, syncs, wal, allocs, bytes uint64, total int) result {
	commandsPerSync := 0.0
	if syncs > 0 {
		commandsPerSync = float64(n) / float64(syncs)
	}
	return result{Lane: lane, Workload: workload, Durability: durability, Operations: n, Concurrency: workers, CommandsPerSecond: float64(n) / elapsed.Seconds(), P50Micros: p(latencies, .5), P95Micros: p(latencies, .95), P99Micros: p(latencies, .99), SyncsPerSecond: float64(syncs) / elapsed.Seconds(), CommandsPerSync: commandsPerSync, WALBytesPerOp: float64(wal) / float64(n), AllocsPerOp: float64(allocs) / float64(n), BytesPerOp: float64(bytes) / float64(n), FinalBalance: total}
}

func setup(execute func(m7write.Command) error, accounts int) {
	for i := 1; i <= accounts; i++ {
		if err := execute(m7write.Command{ID: fmt.Sprintf("setup-%d", i), Kind: m7write.Create, Account: m7write.AccountID(i), Amount: 1000}); err != nil {
			panic(err)
		}
	}
}
func makeCommands(workload string, n, accounts int, seed int64) []m7write.Command {
	rng := rand.New(rand.NewSource(seed))
	out := make([]m7write.Command, n)
	for i := range out {
		a := 1 + i%accounts
		b := 1 + rng.Intn(accounts)
		if b == a {
			b = 1 + b%accounts
		}
		kind := m7write.Deposit
		if workload == "hot_key" {
			a = 1
		}
		if workload == "transfer" {
			kind = m7write.Transfer
		}
		out[i] = m7write.Command{ID: fmt.Sprintf("%s-%d", workload, i), Kind: kind, Account: m7write.AccountID(a), Other: m7write.AccountID(b), Amount: 1}
	}
	return out
}
func totalBalance(store *m7write.Store, accounts int) int {
	total := 0
	for i := 1; i <= accounts; i++ {
		a, _ := store.Account(m7write.AccountID(i))
		total += a.Balance
	}
	return total
}
func p(v []int64, q float64) float64 { return float64(v[int(float64(len(v)-1)*q)]) / 1000 }
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
