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

	"github.com/yuechen-li-dev/database-scheduler/internal/m7write"
)

type report struct {
	GeneratedAt string   `json:"generated_at"`
	OctCommit   string   `json:"oct_commit"`
	GoVersion   string   `json:"go_version"`
	OS          string   `json:"os"`
	Arch        string   `json:"arch"`
	CPUs        int      `json:"cpus"`
	Seed        int64    `json:"seed"`
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
	FinalBalance      int     `json:"final_total_balance"`
}

func main() {
	out := flag.String("out", "", "optional JSON output")
	operations := flag.Int("operations", 2000, "commands per workload (fsync-each is capped at one quarter)")
	accounts := flag.Int("accounts", 128, "resident accounts")
	workers := flag.Int("workers", 8, "concurrent submitters")
	flag.Parse()
	r := report{GeneratedAt: time.Now().UTC().Format(time.RFC3339), OctCommit: "309da01b60ec0f7917d4fd5efd1707bd71d2d40f", GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Seed: 7737}
	for _, mode := range []m7write.DurabilityMode{m7write.MemoryOnly, m7write.BatchSync, m7write.SyncEach} {
		n := *operations
		if mode == m7write.SyncEach && n > 500 {
			n = 500
		}
		for _, workload := range []string{"independent_deposits", "hot_account", "random_transfers", "many_to_one"} {
			r.Results = append(r.Results, runOct(mode, workload, n, *accounts, *workers, r.Seed))
			r.Results = append(r.Results, runGo(mode, workload, n, *accounts, *workers, r.Seed))
		}
	}
	if dsn := os.Getenv("DBSCHED_POSTGRES_DSN"); dsn != "" {
		for _, workload := range []string{"independent_deposits", "hot_account", "random_transfers", "many_to_one"} {
			r.Results = append(r.Results, runPostgres(dsn, workload, min(*operations, 500), *accounts, *workers, r.Seed))
		}
	}
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		panic(err)
	}
	if *out != "" {
		if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
			panic(err)
		}
		if err := os.WriteFile(*out, append(encoded, '\n'), 0o644); err != nil {
			panic(err)
		}
	}
	fmt.Println(string(encoded))
}

type executor func(m7write.Command) error

func runOct(mode m7write.DurabilityMode, workload string, n, accounts, workers int, seed int64) result {
	dir, _ := os.MkdirTemp("", "m7-oct-")
	defer os.RemoveAll(dir)
	e, err := m7write.Open(m7write.Config{MailboxCapacity: 256, Durability: mode, BatchSize: 64, LogPath: filepath.Join(dir, "wal")})
	if err != nil {
		panic(err)
	}
	defer e.Close()
	setup(func(c m7write.Command) error { _, err := e.Submit(context.Background(), c); return err }, accounts)
	return measure("oct_flow", mode, workload, n, accounts, workers, seed, func(c m7write.Command) error { _, err := e.Submit(context.Background(), c); return err }, e.Store())
}

func runGo(mode m7write.DurabilityMode, workload string, n, accounts, workers int, seed int64) result {
	dir, _ := os.MkdirTemp("", "m7-go-")
	defer os.RemoveAll(dir)
	e, err := m7write.OpenGoBaseline(m7write.Config{Durability: mode, BatchSize: 64, LogPath: filepath.Join(dir, "wal")})
	if err != nil {
		panic(err)
	}
	defer e.Close()
	setup(func(c m7write.Command) error { _, err := e.Execute(c); return err }, accounts)
	return measure("go_control", mode, workload, n, accounts, workers, seed, func(c m7write.Command) error { _, err := e.Execute(c); return err }, e.Store())
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
	latencies := make([]int64, n)
	jobs := make(chan int)
	var failMu sync.Mutex
	var failErr error
	var wg sync.WaitGroup
	started := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				at := time.Now()
				_, err := p.Execute(ctx, commands[index])
				latencies[index] = time.Since(at).Nanoseconds()
				if err != nil {
					failMu.Lock()
					if failErr == nil {
						failErr = err
					}
					failMu.Unlock()
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
	if failErr != nil {
		panic(failErr)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	total := 0
	for i := 1; i <= accounts; i++ {
		a, err := p.Account(ctx, m7write.AccountID(i))
		if err != nil {
			panic(err)
		}
		total += a.Balance
	}
	return result{Lane: "postgresql_17", Workload: workload, Durability: "postgres_transaction_commit", Operations: n, Concurrency: workers, CommandsPerSecond: float64(n) / elapsed.Seconds(), P50Micros: percentile(latencies, .50), P95Micros: percentile(latencies, .95), P99Micros: percentile(latencies, .99), FinalBalance: total}
}

func setup(execute executor, accounts int) {
	for i := 1; i <= accounts; i++ {
		if err := execute(m7write.Command{ID: fmt.Sprintf("setup-%d", i), Kind: m7write.Create, Account: m7write.AccountID(i), Amount: 1000}); err != nil {
			panic(err)
		}
	}
}

func measure(lane string, mode m7write.DurabilityMode, workload string, n, accounts, workers int, seed int64, execute executor, store *m7write.Store) result {
	commands := makeCommands(workload, n, accounts, seed)
	latencies := make([]int64, n)
	jobs := make(chan int)
	var failMu sync.Mutex
	var failErr error
	var wg sync.WaitGroup
	started := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				at := time.Now()
				err := execute(commands[index])
				latencies[index] = time.Since(at).Nanoseconds()
				if err != nil {
					failMu.Lock()
					if failErr == nil {
						failErr = err
					}
					failMu.Unlock()
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
	if failErr != nil {
		panic(failErr)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	total := 0
	for i := 1; i <= accounts; i++ {
		a, _ := store.Account(m7write.AccountID(i))
		total += a.Balance
	}
	return result{Lane: lane, Workload: workload, Durability: modeName(mode), Operations: n, Concurrency: workers, CommandsPerSecond: float64(n) / elapsed.Seconds(), P50Micros: percentile(latencies, .50), P95Micros: percentile(latencies, .95), P99Micros: percentile(latencies, .99), FinalBalance: total}
}

func makeCommands(workload string, n, accounts int, seed int64) []m7write.Command {
	rng := rand.New(rand.NewSource(seed))
	out := make([]m7write.Command, n)
	for i := range out {
		a := 1 + (i % accounts)
		b := 1 + rng.Intn(accounts)
		if b == a {
			b = 1 + (b % accounts)
		}
		kind := m7write.Deposit
		amount := 1
		switch workload {
		case "hot_account":
			a = 1
		case "random_transfers":
			kind = m7write.Transfer
		case "many_to_one":
			kind = m7write.Transfer
			b = 1
			a = 2 + (i % (accounts - 1))
		}
		out[i] = m7write.Command{ID: fmt.Sprintf("%s-%d", workload, i), Kind: kind, Account: m7write.AccountID(a), Other: m7write.AccountID(b), Amount: amount}
	}
	return out
}

func percentile(values []int64, p float64) float64 {
	index := int(float64(len(values)-1) * p)
	return float64(values[index]) / 1000
}
func modeName(mode m7write.DurabilityMode) string {
	if mode == m7write.MemoryOnly {
		return "memory_only"
	}
	if mode == m7write.BatchSync {
		return "batch_sync_64"
	}
	return "fsync_each"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
