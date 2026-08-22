package m7write

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func m1Config(dir string, mode DurabilityMode) Config {
	return Config{StorageDir: dir, Durability: mode, SegmentRecords: 3, GroupMax: 16, GroupWait: time.Millisecond, DedupeWindow: 100}
}

func TestM1SegmentSnapshotTailRecoveryAndDedupe(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, m1Config(dir, SyncEach))
	mustSubmit(t, e, Command{ID: "create-a", Kind: Create, Account: 1, Amount: 100})
	mustSubmit(t, e, Command{ID: "create-b", Kind: Create, Account: 2, Amount: 0})
	mustSubmit(t, e, Command{ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 30})
	path, size, err := e.Snapshot()
	if err != nil || size == 0 {
		t.Fatalf("snapshot path=%q size=%d err=%v", path, size, err)
	}
	if entries, _ := os.ReadDir(filepath.Join(dir, "wal")); len(entries) != 0 {
		t.Fatalf("covered WAL was not retired: %v", entries)
	}
	mustSubmit(t, e, Command{ID: "confirm", Kind: Confirm, Account: 1, Other: 2, Amount: 30})
	mustSubmit(t, e, Command{ID: "tail", Kind: Deposit, Account: 2, Amount: 5})
	wantBytes, wantHash := e.Store().CanonicalOctagon()
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	recovered := mustOpen(t, m1Config(dir, SyncEach))
	defer recovered.Close()
	stats := recovered.RecoveryMetrics()
	if stats.SnapshotSequence != 3 || stats.RecordsReplayed != 2 || stats.AgentsRestored != 2 {
		t.Fatalf("recovery=%+v", stats)
	}
	gotBytes, gotHash := recovered.Store().CanonicalOctagon()
	if string(gotBytes) != string(wantBytes) || gotHash != wantHash {
		t.Fatal("snapshot+tail state differs from uninterrupted frontier")
	}
	duplicate := mustSubmit(t, recovered, Command{ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 30})
	if !duplicate.Duplicate || duplicate.Sequence != 3 {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	a, _ := recovered.Store().Account(1)
	b, _ := recovered.Store().Account(2)
	if a.Balance != 70 || b.Balance != 35 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
}

func TestM1BoundedGroupCommitAcknowledgesAfterSharedSync(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, m1Config(dir, BatchSync))
	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 1; i <= n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := e.Submit(context.Background(), Command{ID: "create-" + string(rune(64+i)), Kind: Create, Account: AccountID(i), Amount: 1})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	stats := e.StorageMetrics()
	if stats.Committed != n || stats.Syncs >= n || stats.Syncs == 0 {
		t.Fatalf("group stats=%+v", stats)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := mustOpen(t, m1Config(dir, BatchSync))
	defer recovered.Close()
	for i := 1; i <= n; i++ {
		if account, ok := recovered.Store().Account(AccountID(i)); !ok || account.Balance != 1 {
			t.Fatalf("missing durable account %d", i)
		}
	}
}

func TestM1TruncatedActiveTailAndCorruptionFailClosed(t *testing.T) {
	t.Run("truncated active tail", func(t *testing.T) {
		dir := t.TempDir()
		e := mustOpen(t, m1Config(dir, SyncEach))
		mustSubmit(t, e, Command{ID: "one", Kind: Create, Account: 1, Amount: 1})
		e.Close()
		path := onlySegment(t, dir)
		data, _ := os.ReadFile(path)
		data = append(data[:len(data)-segmentFooterSize], 0, 0, 0, 100, '{')
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		recovered := mustOpen(t, m1Config(dir, SyncEach))
		defer recovered.Close()
		if account, ok := recovered.Store().Account(1); !ok || account.Balance != 1 {
			t.Fatalf("account=%+v ok=%v", account, ok)
		}
	})

	tests := []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"header", func(data []byte) []byte { data[0] ^= 0xff; return data }},
		{"record checksum", func(data []byte) []byte {
			length := int(binary.BigEndian.Uint32(data[segmentHeaderSize : segmentHeaderSize+4]))
			data[segmentHeaderSize+4+length] ^= 0xff
			return data
		}},
		{"footer", func(data []byte) []byte { data[len(data)-1] ^= 0xff; return data }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			e := mustOpen(t, m1Config(dir, SyncEach))
			mustSubmit(t, e, Command{ID: "one", Kind: Create, Account: 1, Amount: 1})
			e.Close()
			path := onlySegment(t, dir)
			data, _ := os.ReadFile(path)
			data = test.mutate(data)
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := Open(m1Config(dir, SyncEach))
			var runtimeErr *RuntimeError
			if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestM1CorruptedSnapshotFailsClosed(t *testing.T) {
	dir := t.TempDir()
	e := mustOpen(t, m1Config(dir, SyncEach))
	mustSubmit(t, e, Command{ID: "one", Kind: Create, Account: 1, Amount: 1})
	path, _, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	data, _ := os.ReadFile(path)
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(m1Config(dir, SyncEach))
	var runtimeErr *RuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
		t.Fatalf("err=%v", err)
	}
}

func TestM1MissingSegmentOutOfOrderAndCheckpointMismatch(t *testing.T) {
	t.Run("missing middle segment", func(t *testing.T) {
		dir := t.TempDir()
		cfg := m1Config(dir, SyncEach)
		cfg.SegmentRecords = 2
		e := mustOpen(t, cfg)
		for i := 1; i <= 5; i++ {
			mustSubmit(t, e, Command{ID: string(rune('a' + i)), Kind: Create, Account: AccountID(i), Amount: 1})
		}
		e.Close()
		entries, _ := os.ReadDir(filepath.Join(dir, "wal"))
		var segments []string
		for _, entry := range entries {
			if _, ok := parseSegmentID(entry.Name()); ok {
				segments = append(segments, filepath.Join(dir, "wal", entry.Name()))
			}
		}
		sort.Strings(segments)
		if len(segments) != 3 {
			t.Fatalf("segments=%v", segments)
		}
		if err := os.Remove(segments[1]); err != nil {
			t.Fatal(err)
		}
		_, err := Open(cfg)
		var runtimeErr *RuntimeError
		if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
			t.Fatalf("err=%v", err)
		}
	})
	for _, test := range []struct {
		name   string
		mutate func(*logRecord)
	}{
		{"out of order", func(record *logRecord) { record.Sequence = 2; record.Result.Sequence = 2 }},
		{"flow checkpoint mismatch", func(record *logRecord) { record.Checkpoint = []byte("not-a-flow-checkpoint") }},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := m1Config(dir, SyncEach)
			e := mustOpen(t, cfg)
			mustSubmit(t, e, Command{ID: "one", Kind: Create, Account: 1, Amount: 1})
			e.Close()
			rewriteOnlyRecord(t, onlySegment(t, dir), test.mutate)
			_, err := Open(cfg)
			var runtimeErr *RuntimeError
			if !errors.As(err, &runtimeErr) {
				t.Fatalf("err=%v", err)
			}
			if test.name == "out of order" && runtimeErr.Kind != RecoveryCorrupt {
				t.Fatalf("err=%v", err)
			}
			if test.name == "flow checkpoint mismatch" && runtimeErr.Kind != RecoveryIncompatible {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestM1SnapshotPublicationHashMismatchFailsClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := m1Config(dir, SyncEach)
	e := mustOpen(t, cfg)
	mustSubmit(t, e, Command{ID: "one", Kind: Create, Account: 1, Amount: 1})
	path, _, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	e.Close()
	data, _ := os.ReadFile(path)
	var snapshot durableSnapshot
	if err := json.Unmarshal(data[52:], &snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.OctHash = "00"
	payload, _ := json.Marshal(snapshot)
	digest := sha256.Sum256(payload)
	framed := make([]byte, 52+len(payload))
	copy(framed[:20], data[:20])
	binary.BigEndian.PutUint64(framed[12:20], uint64(len(payload)))
	copy(framed[20:52], digest[:])
	copy(framed[52:], payload)
	if err := os.WriteFile(path, framed, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(cfg)
	var runtimeErr *RuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
		t.Fatalf("err=%v", err)
	}
}

func rewriteOnlyRecord(t *testing.T, path string, mutate func(*logRecord)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	length := int(binary.BigEndian.Uint32(data[segmentHeaderSize : segmentHeaderSize+4]))
	var record logRecord
	if err := json.Unmarshal(data[segmentHeaderSize+4:segmentHeaderSize+4+length], &record); err != nil {
		t.Fatal(err)
	}
	mutate(&record)
	framed, err := frame(record)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt := append([]byte(nil), data[:segmentHeaderSize]...)
	rebuilt = append(rebuilt, framed...)
	rebuilt = append(rebuilt, encodeSegmentFooter(record.Sequence, 1)...)
	if err := os.WriteFile(path, rebuilt, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestM1DedupeWindowIsExplicitlyBounded(t *testing.T) {
	dir := t.TempDir()
	cfg := m1Config(dir, SyncEach)
	cfg.DedupeWindow = 2
	e := mustOpen(t, cfg)
	for i, id := range []string{"a", "b", "c"} {
		mustSubmit(t, e, Command{ID: id, Kind: Create, Account: AccountID(i + 1), Amount: 1})
	}
	if len(e.results) != 2 {
		t.Fatalf("dedupe entries=%d", len(e.results))
	}
	if _, ok := e.results["a"]; ok {
		t.Fatal("oldest command was not evicted")
	}
	e.Close()
}

func TestM1GoControlUsesSameStorageAndRecoversWorkflow(t *testing.T) {
	dir := t.TempDir()
	cfg := m1Config(dir, BatchSync)
	e, err := OpenGoM1Baseline(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []Command{{ID: "a", Kind: Create, Account: 1, Amount: 20}, {ID: "b", Kind: Create, Account: 2}, {ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 7}} {
		mustSubmit(t, e, command)
	}
	if _, _, err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	e.Close()
	recovered, err := OpenGoM1Baseline(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	confirm := mustSubmit(t, recovered, Command{ID: "confirm", Kind: Confirm, Account: 1, Other: 2, Amount: 7})
	if !confirm.Accepted || confirm.TransitionCount != 3 {
		t.Fatalf("confirm=%+v", confirm)
	}
	a, _ := recovered.Store().Account(1)
	b, _ := recovered.Store().Account(2)
	if a.Balance != 13 || b.Balance != 7 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
}

func TestM1CrashWindowRecoveryMatrix(t *testing.T) {
	tests := []struct {
		point       FailurePoint
		wantApplied bool
		setup       int
	}{
		{BeforeWALAppend, false, 0},
		{DuringWALAppend, false, 0},
		{AfterWALAppendBeforeSync, true, 0},
		{AfterWALSyncBeforeApply, true, 0},
		{AfterStepBeforeDeltaExport, false, 0},
		{AfterDeltaExportBeforeWALAppend, false, 0},
		{AfterSyncBeforeDirtyClear, true, 0},
		{AfterStateApplyBeforeDirtyClear, true, 0},
		{AfterStateApplyBeforeAck, true, 0},
		{DuringSegmentRotation, false, 3},
	}
	for _, test := range tests {
		t.Run(string(test.point), func(t *testing.T) {
			dir := t.TempDir()
			base := m1Config(dir, SyncEach)
			e := mustOpen(t, base)
			for i := 0; i < test.setup; i++ {
				mustSubmit(t, e, Command{ID: string(rune('a' + i)), Kind: Create, Account: AccountID(i + 1), Amount: 1})
			}
			var fired atomic.Bool
			e.m1.failpoint = func(point FailurePoint) error {
				if point == test.point && fired.CompareAndSwap(false, true) {
					return errors.New("injected process stop")
				}
				return nil
			}
			_, err := e.Submit(context.Background(), Command{ID: "target", Kind: Create, Account: 99, Amount: 7})
			if err == nil || !fired.Load() {
				t.Fatalf("point did not fire: err=%v", err)
			}
			if e.m1.file != nil {
				_ = e.m1.file.Close()
				e.m1.file = nil
			}
			// Opening the same durable files models restart without running the old
			// process's graceful-close path.
			recovered := mustOpen(t, base)
			account, applied := recovered.Store().Account(99)
			if applied != test.wantApplied || (applied && account.Balance != 7) {
				t.Fatalf("applied=%v account=%+v", applied, account)
			}
			recovered.Close()
		})
	}
}

func TestM1SnapshotInstallationCrashMatrix(t *testing.T) {
	for _, point := range []FailurePoint{DuringSnapshotFlowCheckpoint, DuringCompactDedupeEncoding, DuringSnapshotWrite, AfterSnapshotSyncBeforeInstall, AfterSnapshotInstallBeforeCleanup} {
		t.Run(string(point), func(t *testing.T) {
			dir := t.TempDir()
			cfg := m1Config(dir, SyncEach)
			e := mustOpen(t, cfg)
			mustSubmit(t, e, Command{ID: "one", Kind: Create, Account: 1, Amount: 9})
			var fired atomic.Bool
			e.m1.failpoint = func(got FailurePoint) error {
				if got == point && fired.CompareAndSwap(false, true) {
					return errors.New("injected snapshot stop")
				}
				return nil
			}
			_, _, err := e.Snapshot()
			if err == nil || !fired.Load() {
				t.Fatalf("err=%v fired=%v", err, fired.Load())
			}
			e.m1.failpoint = nil
			e.Close()
			recovered := mustOpen(t, cfg)
			defer recovered.Close()
			account, ok := recovered.Store().Account(1)
			if !ok || account.Balance != 9 {
				t.Fatalf("account=%+v ok=%v", account, ok)
			}
			if point == AfterSnapshotInstallBeforeCleanup && recovered.RecoveryMetrics().SnapshotSequence != 1 {
				t.Fatalf("installed snapshot not selected: %+v", recovered.RecoveryMetrics())
			}
		})
	}
}

func onlySegment(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, entry := range entries {
		if _, ok := parseSegmentID(entry.Name()); ok {
			paths = append(paths, filepath.Join(dir, "wal", entry.Name()))
		}
	}
	sort.Strings(paths)
	if len(paths) != 1 {
		t.Fatalf("segments=%v", paths)
	}
	return paths[0]
}
