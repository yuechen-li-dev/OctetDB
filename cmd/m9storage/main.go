package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	generated "github.com/yuechen-li-dev/database-scheduler/internal/m7generated"
	"github.com/yuechen-li-dev/database-scheduler/internal/m7write"
)

type evidence struct {
	GeneratedAt     string         `json:"generated_at"`
	GoVersion       string         `json:"go_version"`
	Operations      int            `json:"operations"`
	FlowScenarios   []flowScenario `json:"flow_scenarios"`
	FlowWALAblation []storageRun   `json:"flow_wal_ablation"`
	DedupeAblation  []storageRun   `json:"dedupe_snapshot_ablation"`
}
type flowScenario struct {
	Name                string  `json:"name"`
	FullCheckpointBytes int     `json:"full_checkpoint_bytes"`
	DeltaBytes          int     `json:"delta_bytes"`
	Ratio               float64 `json:"delta_to_full_ratio"`
}
type storageRun struct {
	Name                string  `json:"name"`
	Operations          int     `json:"operations"`
	WALBytes            uint64  `json:"wal_bytes"`
	WALBytesPerOp       float64 `json:"wal_bytes_per_op"`
	FlowDeltaBytes      uint64  `json:"flow_delta_bytes"`
	FlowBytesPerOp      float64 `json:"flow_bytes_per_op"`
	DedupeBytes         uint64  `json:"dedupe_bytes"`
	SnapshotBytes       int64   `json:"snapshot_bytes"`
	SnapshotPauseMillis float64 `json:"snapshot_pause_ms"`
	RecoveryMillis      float64 `json:"recovery_ms"`
	RecoveryAllocs      uint64  `json:"recovery_allocs"`
	RecoveryBytes       uint64  `json:"recovery_bytes"`
	DedupeDecodeMillis  float64 `json:"dedupe_decode_ms"`
	Agents              int     `json:"agents"`
	CheckpointBytes     uint64  `json:"checkpoint_bytes"`
	Hash                string  `json:"hash"`
}

func main() {
	out := flag.String("out", "", "output JSON")
	n := flag.Int("operations", 100000, "commands per ablation")
	flag.Parse()
	e := evidence{GeneratedAt: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version(), Operations: *n, FlowScenarios: measureFlowScenarios()}
	e.FlowWALAblation = []storageRun{run("full_checkpoint_wal", *n, true, false, true), run("semantic_delta_wal", *n, false, false, true)}
	e.DedupeAblation = []storageRun{run("json_dedupe_snapshot", *n, false, true, false), run("compact_dedupe_snapshot", *n, false, false, false)}
	data, _ := json.MarshalIndent(e, "", "  ")
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

func run(name string, n int, full, jsonDedupe, writeWAL bool) storageRun {
	dir, _ := os.MkdirTemp("", "m9-storage-")
	defer os.RemoveAll(dir)
	mode := m7write.MemoryOnly
	if writeWAL {
		mode = m7write.BatchSync
	}
	cfg := m7write.Config{StorageDir: dir, Durability: mode, GroupMax: 16, GroupWait: 0, SegmentRecords: 4096, DedupeWindow: min(n, 100000), MailboxCapacity: 256, FullCheckpointWAL: full, JSONDedupeSnapshot: jsonDedupe}
	e, err := m7write.Open(cfg)
	if err != nil {
		panic(err)
	}
	populate(e, n)
	started := time.Now()
	_, snapshotBytes, err := e.Snapshot()
	pause := time.Since(started)
	if err != nil {
		panic(err)
	}
	stats := e.StorageMetrics()
	_, hash := e.Store().CanonicalOctagon()
	e.Close()
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	started = time.Now()
	recovered, err := m7write.Open(cfg)
	elapsed := time.Since(started)
	runtime.ReadMemStats(&after)
	if err != nil {
		panic(err)
	}
	metrics := recovered.RecoveryMetrics()
	agents := recovered.AgentCount()
	recovered.Close()
	return storageRun{Name: name, Operations: n, WALBytes: stats.WALBytesWritten, WALBytesPerOp: float64(stats.WALBytesWritten) / float64(n), FlowDeltaBytes: stats.FlowDeltaBytes, FlowBytesPerOp: float64(stats.FlowDeltaBytes) / float64(n), DedupeBytes: stats.DedupeBytes, SnapshotBytes: snapshotBytes, SnapshotPauseMillis: float64(pause.Microseconds()) / 1000, RecoveryMillis: float64(elapsed.Microseconds()) / 1000, RecoveryAllocs: after.Mallocs - before.Mallocs, RecoveryBytes: after.TotalAlloc - before.TotalAlloc, DedupeDecodeMillis: float64(metrics.DedupeDecode.Microseconds()) / 1000, Agents: agents, CheckpointBytes: stats.FlowCheckpointBytes, Hash: hash}
}

func populate(e *m7write.Engine, n int) {
	creates := min(n, 128)
	for i := 0; i < creates; i++ {
		if _, err := e.Submit(context.Background(), m7write.Command{ID: fmt.Sprintf("m9-%09d", i), Kind: m7write.Create, Account: m7write.AccountID(i + 1), Amount: 1000}); err != nil {
			panic(err)
		}
	}
	commands := make([]m7write.Command, n-creates)
	for i := creates; i < n; i++ {
		account := m7write.AccountID(i%128 + 1)
		commands[i-creates] = m7write.Command{ID: fmt.Sprintf("m9-%09d", i), Kind: m7write.Deposit, Account: account, Amount: 1}
	}
	jobs := make(chan m7write.Command)
	var wg sync.WaitGroup
	var first error
	var mu sync.Mutex
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				_, err := e.Submit(context.Background(), c)
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
	for _, c := range commands {
		jobs <- c
	}
	close(jobs)
	wg.Wait()
	if first != nil {
		panic(first)
	}
}

func measureFlowScenarios() []flowScenario {
	m := generated.NewAccountAgent(1)
	var previous *generated.AccountAgentCheckpoint
	cases := []struct {
		name string
		ctx  generated.Main_CommandContext
	}{{"accepted_create", generated.Main_CommandContext{Kind: generated.NewCommandKindCreate(), AccountA: 1, Amount: 100, StatusA: generated.NewAccountStatusMissing()}}, {"one_shot_turn_counter", generated.Main_CommandContext{Kind: generated.NewCommandKindDeposit(), AccountA: 1, Amount: 1, ExistsA: true, BalanceA: 100, StatusA: generated.NewAccountStatusOpen(), VersionA: 1}}, {"rejected_command", generated.Main_CommandContext{Kind: generated.NewCommandKindWithdraw(), AccountA: 1, Amount: 1000, ExistsA: true, BalanceA: 101, StatusA: generated.NewAccountStatusOpen(), VersionA: 2}}, {"begin_transfer", generated.Main_CommandContext{Kind: generated.NewCommandKindBeginTransfer(), AccountA: 1, AccountB: 2, Amount: 25, ExistsA: true, ExistsB: true, BalanceA: 101, BalanceB: 0, StatusA: generated.NewAccountStatusOpen(), StatusB: generated.NewAccountStatusOpen(), VersionA: 2, VersionB: 1}}, {"confirm_board_change", generated.Main_CommandContext{Kind: generated.NewCommandKindConfirm(), AccountA: 1, AccountB: 2, Amount: 25, ExistsA: true, ExistsB: true, BalanceA: 101, BalanceB: 0, StatusA: generated.NewAccountStatusOpen(), StatusB: generated.NewAccountStatusOpen(), VersionA: 2, VersionB: 1}}}
	out := make([]flowScenario, 0, len(cases))
	for _, c := range cases {
		if _, err := m.Step(c.ctx); err != nil {
			panic(err)
		}
		cp, err := m.Checkpoint()
		if err != nil {
			panic(err)
		}
		delta, err := generated.ExportAccountAgentDelta(previous, cp)
		if err != nil {
			panic(err)
		}
		out = append(out, flowScenario{Name: c.name, FullCheckpointBytes: len(cp.Bytes()), DeltaBytes: len(delta.Bytes()), Ratio: float64(len(delta.Bytes())) / float64(len(cp.Bytes()))})
		copy := cp
		previous = &copy
	}
	return out
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
