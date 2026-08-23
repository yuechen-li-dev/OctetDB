package main

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"

	m7write "github.com/yuechen-li-dev/octetdb/internal/researchengine"
)

func main() {
	out := flag.String("out", "experiments/M7/traces/transfer.json", "trace output")
	publication := flag.String("publication", "experiments/M7/publication/accounts.octagon", "canonical publication output")
	flag.Parse()
	var events []m7write.TraceEvent
	dir, err := os.MkdirTemp("", "m7-trace-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	e, err := m7write.Open(m7write.Config{MailboxCapacity: 4, Durability: m7write.SyncEach, LogPath: filepath.Join(dir, "transitions.wal"), Trace: func(event m7write.TraceEvent) { events = append(events, event) }})
	if err != nil {
		panic(err)
	}
	commands := []m7write.Command{{ID: "create-1", Kind: m7write.Create, Account: 1, Amount: 100}, {ID: "create-2", Kind: m7write.Create, Account: 2, Amount: 10}, {ID: "transfer-1-2", Kind: m7write.Transfer, Account: 1, Other: 2, Amount: 25}}
	for _, command := range commands {
		if _, err := e.Submit(context.Background(), command); err != nil {
			panic(err)
		}
	}
	if err := e.Close(); err != nil {
		panic(err)
	}
	encoded, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*out, append(encoded, '\n'), 0o644); err != nil {
		panic(err)
	}
	bytes, _ := e.Store().CanonicalOctagon()
	if err := os.MkdirAll(filepath.Dir(*publication), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*publication, bytes, 0o644); err != nil {
		panic(err)
	}
}
