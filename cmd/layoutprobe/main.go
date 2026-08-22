package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yuechen-li-dev/database-scheduler/internal/m7write"
)

type report struct {
	Accounts          int                   `json:"accounts"`
	BeforeSnapshotOps int                   `json:"before_snapshot_ops"`
	TailOps           int                   `json:"tail_ops"`
	SnapshotPath      string                `json:"snapshot_path"`
	SnapshotBytes     int64                 `json:"snapshot_bytes"`
	SnapshotDuration  time.Duration         `json:"snapshot_duration"`
	WALTailBytes      int64                 `json:"wal_tail_bytes"`
	Recovery          m7write.RecoveryStats `json:"recovery"`
	ReadyWall         time.Duration         `json:"ready_wall"`
	Duplicate         bool                  `json:"duplicate_suppressed"`
	Conserved         bool                  `json:"conserved"`
	Digest            string                `json:"digest"`
}

func main() {
	var dir, out string
	var accounts, operations, tail int
	flag.StringVar(&dir, "dir", "", "durable C2 directory")
	flag.StringVar(&out, "out", "", "JSON output path")
	flag.IntVar(&accounts, "accounts", 1000, "account population")
	flag.IntVar(&operations, "operations", 100000, "operations before snapshot")
	flag.IntVar(&tail, "tail", 10000, "operations after snapshot")
	flag.Parse()
	if dir == "" {
		fmt.Fprintln(os.Stderr, "-dir is required")
		os.Exit(2)
	}
	cfg := m7write.SpecializedConfig{StorageDir: dir, Durability: m7write.BatchSync, AccountHint: accounts, RecordHint: accounts + operations + tail, DedupeWindow: accounts + operations + tail + 10}
	e, err := m7write.OpenSpecialized(cfg)
	must(err)
	batch := make([]m7write.Command, 0, 512)
	for start := 1; start <= accounts; start += 512 {
		batch = batch[:0]
		for id := start; id < min(start+512, accounts+1); id++ {
			batch = append(batch, m7write.Command{ID: fmt.Sprintf("setup-%d", id), Kind: m7write.Create, Account: m7write.AccountID(id), Amount: 1_000_000_000})
		}
		_, err = e.SubmitBatch(batch)
		must(err)
	}
	runTransfers(e, batch, accounts, 0, operations)
	snapshotStarted := time.Now()
	path, snapshotBytes, err := e.Snapshot()
	must(err)
	snapshotDuration := time.Since(snapshotStarted)
	runTransfers(e, batch, accounts, operations, tail)
	must(e.Close())
	walInfo, err := os.Stat(filepath.Join(dir, "c2.wal"))
	must(err)
	readyStarted := time.Now()
	recovered, err := m7write.OpenSpecialized(cfg)
	must(err)
	readyWall := time.Since(readyStarted)
	duplicate, err := recovered.Submit(m7write.Command{ID: fmt.Sprintf("x-%d", operations+1), Kind: m7write.Transfer, Account: 1, Other: 2, Amount: 1})
	must(err)
	h := sha256.New()
	var total int64
	for id := 1; id <= accounts; id++ {
		a, ok := recovered.Account(m7write.AccountID(id))
		if !ok {
			panic(fmt.Sprintf("missing account %d", id))
		}
		total += int64(a.Balance)
		fmt.Fprintf(h, "%d:%d:%d:%d;", a.ID, a.Balance, a.Status, a.Version)
	}
	rep := report{Accounts: accounts, BeforeSnapshotOps: operations, TailOps: tail, SnapshotPath: path, SnapshotBytes: snapshotBytes, SnapshotDuration: snapshotDuration, WALTailBytes: walInfo.Size(), Recovery: recovered.RecoveryMetrics(), ReadyWall: readyWall, Duplicate: duplicate.Duplicate, Conserved: total == int64(accounts)*1_000_000_000, Digest: hex.EncodeToString(h.Sum(nil))}
	must(recovered.Close())
	data, err := json.MarshalIndent(rep, "", "  ")
	must(err)
	data = append(data, '\n')
	if out != "" {
		must(os.MkdirAll(filepath.Dir(out), 0o755))
		must(os.WriteFile(out, data, 0o644))
	}
	fmt.Print(string(data))
}

func runTransfers(e *m7write.SpecializedEngine, batch []m7write.Command, accounts, offset, count int) {
	for start := offset; start < offset+count; start += 512 {
		batch = batch[:0]
		for i := start; i < min(start+512, offset+count); i++ {
			a := 1 + (2*i)%accounts
			b := 1 + (2*i+1)%accounts
			batch = append(batch, m7write.Command{ID: fmt.Sprintf("x-%d", i+1), Kind: m7write.Transfer, Account: m7write.AccountID(a), Other: m7write.AccountID(b), Amount: 1})
		}
		_, err := e.SubmitBatch(batch)
		must(err)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
