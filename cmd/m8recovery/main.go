package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	m7write "github.com/yuechen-li-dev/octetdb/internal/researchengine"
)

type report struct {
	GeneratedAt string   `json:"generated_at"`
	GoVersion   string   `json:"go_version"`
	Results     []result `json:"results"`
}
type result struct {
	Control              string  `json:"control"`
	Commits              int     `json:"commits"`
	Tail                 string  `json:"tail"`
	TimeToReadyMillis    float64 `json:"time_to_ready_ms"`
	RecordsReplayed      int     `json:"records_replayed"`
	AgentsRestored       int     `json:"agents_restored"`
	SnapshotBytes        int64   `json:"snapshot_bytes"`
	WALBytesScanned      int64   `json:"wal_bytes_scanned"`
	RecoveryAllocs       uint64  `json:"recovery_allocs"`
	RecoveryBytes        uint64  `json:"recovery_bytes"`
	CanonicalHash        string  `json:"canonical_hash"`
	LogicalStateBytes    uint64  `json:"logical_state_bytes"`
	FlowCheckpointBytes  uint64  `json:"flow_checkpoint_bytes"`
	DedupeBytes          uint64  `json:"dedupe_bytes"`
	PublicationBytes     uint64  `json:"publication_bytes"`
	SnapshotPauseMillis  float64 `json:"snapshot_pause_ms"`
	DedupeDecodeMillis   float64 `json:"dedupe_decode_ms"`
	AgentRestoreMillis   float64 `json:"agent_restore_ms"`
	FlowDeltaApplyMillis float64 `json:"flow_delta_apply_ms"`
}

func main() {
	out := flag.String("out", "", "JSON output")
	sizesText := flag.String("sizes", "10000,100000", "comma-separated commit counts")
	flag.Parse()
	r := report{GeneratedAt: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version()}
	for _, part := range strings.Split(*sizesText, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			panic(err)
		}
		r.Results = append(r.Results, runLogOnly(n))
		r.Results = append(r.Results, runSnapshot(n, 0))
		tail := n / 10
		if tail > 10000 {
			tail = 10000
		}
		r.Results = append(r.Results, runSnapshot(n, tail))
	}
	data, _ := json.MarshalIndent(r, "", "  ")
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

func runLogOnly(n int) result {
	dir, _ := os.MkdirTemp("", "m8-m0-")
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "m0.wal")
	cfg := m7write.Config{Durability: m7write.BatchSync, BatchSize: 64, LogPath: path, MailboxCapacity: 256}
	e, err := m7write.Open(cfg)
	if err != nil {
		panic(err)
	}
	populate(e, n, 128, "m0")
	if err := e.Close(); err != nil {
		panic(err)
	}
	info, _ := os.Stat(path)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started := time.Now()
	recovered, err := m7write.Open(cfg)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	if err != nil {
		panic(err)
	}
	_, hash := recovered.Store().CanonicalOctagon()
	agents := recovered.AgentCount()
	recovered.Close()
	return result{Control: "m0_log_only", Commits: n, Tail: "full_history", TimeToReadyMillis: float64(elapsed.Microseconds()) / 1000, RecordsReplayed: n, AgentsRestored: agents, WALBytesScanned: info.Size(), RecoveryAllocs: after.Mallocs - before.Mallocs, RecoveryBytes: after.TotalAlloc - before.TotalAlloc, CanonicalHash: hash}
}

func runSnapshot(n, tail int) result {
	dir, _ := os.MkdirTemp("", "m8-m1-")
	defer os.RemoveAll(dir)
	base := m7write.Config{StorageDir: dir, Durability: m7write.MemoryOnly, GroupMax: 16, SegmentRecords: 4096, DedupeWindow: min(n+tail, 100000), MailboxCapacity: 256}
	e, err := m7write.Open(base)
	if err != nil {
		panic(err)
	}
	populate(e, n, 128, "snap")
	snapshotStarted := time.Now()
	_, snapshotBytes, err := e.Snapshot()
	snapshotPause := time.Since(snapshotStarted)
	if err != nil {
		panic(err)
	}
	storageBreakdown := e.StorageMetrics()
	e.Close()
	if tail > 0 {
		tailCfg := base
		tailCfg.Durability = m7write.BatchSync
		e, err = m7write.Open(tailCfg)
		if err != nil {
			panic(err)
		}
		populateRange(e, n, tail, 128, "tail")
		e.Close()
	}
	recoveryCfg := base
	recoveryCfg.Durability = m7write.BatchSync
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started := time.Now()
	recovered, err := m7write.Open(recoveryCfg)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	if err != nil {
		panic(err)
	}
	metrics := recovered.RecoveryMetrics()
	_, hash := recovered.Store().CanonicalOctagon()
	recovered.Close()
	label := "none"
	if tail > 0 {
		label = "medium"
	}
	return result{Control: "m2_snapshot", Commits: n + tail, Tail: label, TimeToReadyMillis: float64(elapsed.Microseconds()) / 1000, RecordsReplayed: metrics.RecordsReplayed, AgentsRestored: metrics.AgentsRestored, SnapshotBytes: snapshotBytes, WALBytesScanned: metrics.WALBytesScanned, RecoveryAllocs: after.Mallocs - before.Mallocs, RecoveryBytes: after.TotalAlloc - before.TotalAlloc, CanonicalHash: hash, LogicalStateBytes: storageBreakdown.LogicalStateBytes, FlowCheckpointBytes: storageBreakdown.FlowCheckpointBytes, DedupeBytes: storageBreakdown.DedupeBytes, PublicationBytes: storageBreakdown.PublicationBytes, SnapshotPauseMillis: float64(snapshotPause.Microseconds()) / 1000, DedupeDecodeMillis: float64(metrics.DedupeDecode.Microseconds()) / 1000, AgentRestoreMillis: float64(metrics.AgentRestore.Microseconds()) / 1000, FlowDeltaApplyMillis: float64(metrics.FlowDeltaApply.Microseconds()) / 1000}
}

func populate(e *m7write.Engine, n, accounts int, prefix string) {
	creates := min(n, accounts)
	commands := make([]m7write.Command, 0, n)
	for i := 0; i < creates; i++ {
		commands = append(commands, m7write.Command{ID: fmt.Sprintf("%s-create-%d", prefix, i), Kind: m7write.Create, Account: m7write.AccountID(i + 1), Amount: 1000})
	}
	for i := creates; i < n; i++ {
		commands = append(commands, m7write.Command{ID: fmt.Sprintf("%s-deposit-%d", prefix, i), Kind: m7write.Deposit, Account: m7write.AccountID(1 + i%creates), Amount: 1})
	}
	execute(e, commands, 16)
}
func populateRange(e *m7write.Engine, start, n, accounts int, prefix string) {
	commands := make([]m7write.Command, n)
	for i := range commands {
		commands[i] = m7write.Command{ID: fmt.Sprintf("%s-%d", prefix, start+i), Kind: m7write.Deposit, Account: m7write.AccountID(1 + i%accounts), Amount: 1}
	}
	execute(e, commands, 16)
}
func execute(e *m7write.Engine, commands []m7write.Command, workers int) {
	jobs := make(chan m7write.Command)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for command := range jobs {
				_, err := e.Submit(context.Background(), command)
				if err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		}()
	}
	for _, command := range commands {
		jobs <- command
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		panic(first)
	}
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
