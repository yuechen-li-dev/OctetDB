package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestM2DeltaRecoveryMatchesExactUninterruptedCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := m1Config(dir, SyncEach)
	e := mustOpen(t, cfg)
	for _, c := range []Command{{ID: "a", Kind: Create, Account: 1, Amount: 100}, {ID: "b", Kind: Create, Account: 2}, {ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 25}} {
		mustSubmit(t, e, c)
	}
	if _, _, err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	for _, c := range []Command{{ID: "confirm", Kind: Confirm, Account: 1, Other: 2, Amount: 25}, {ID: "deposit", Kind: Deposit, Account: 1, Amount: 3}} {
		mustSubmit(t, e, c)
	}
	want := append([]byte(nil), e.entry(1).checkpoint...)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := mustOpen(t, cfg)
	defer recovered.Close()
	if !bytes.Equal(want, recovered.entry(1).checkpoint) {
		t.Fatal("delta replay did not reconstruct byte-identical full checkpoint")
	}
	if recovered.RecoveryMetrics().RecordsReplayed != 2 {
		t.Fatalf("metrics=%+v", recovered.RecoveryMetrics())
	}
}

func TestM2FlowCheckpointVersusDeltaWALAblation(t *testing.T) {
	run := func(full bool) ([]byte, []logRecord) {
		dir := t.TempDir()
		cfg := m1Config(dir, SyncEach)
		cfg.FullCheckpointWAL = full
		cfg.SegmentRecords = 100
		e := mustOpen(t, cfg)
		for i, c := range []Command{{Kind: Create, Account: 1, Amount: 100}, {Kind: Deposit, Account: 1, Amount: 1}, {Kind: Withdraw, Account: 1, Amount: 2}, {Kind: Freeze, Account: 1}} {
			c.ID = string(rune('a' + i))
			mustSubmit(t, e, c)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		path := onlySegment(t, dir)
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		records, _, _, _, err := scanSegments(filepath.Join(dir, "wal"), 0, m1ProgramID)
		if err != nil {
			t.Fatal(err)
		}
		return data, records
	}
	fullBytes, fullRecords := run(true)
	deltaBytes, deltaRecords := run(false)
	deltaBytesAgain, _ := run(false)
	if !bytes.Equal(deltaBytes, deltaBytesAgain) {
		t.Fatal("equivalent WAL bytes are not deterministic")
	}
	if len(deltaBytes) >= len(fullBytes)*2/3 {
		t.Fatalf("full=%d delta=%d", len(fullBytes), len(deltaBytes))
	}
	for _, r := range deltaRecords {
		if len(r.FlowDelta) == 0 || len(r.Checkpoint) != 0 {
			t.Fatalf("not a delta record: %+v", r)
		}
	}
	for _, r := range fullRecords {
		if len(r.Checkpoint) == 0 || len(r.FlowDelta) != 0 {
			t.Fatalf("not a checkpoint control: %+v", r)
		}
	}
}

func TestM2CompactDedupeSnapshotAblationDeterminismAndHorizon(t *testing.T) {
	run := func(jsonControl bool) ([]byte, int64) {
		dir := t.TempDir()
		cfg := m1Config(dir, MemoryOnly)
		cfg.DedupeWindow = 1200
		cfg.JSONDedupeSnapshot = jsonControl
		e := mustOpen(t, cfg)
		for i := 1; i <= 1000; i++ {
			account := AccountID((i-1)%16 + 1)
			kind := Deposit
			if i <= 16 {
				kind = Create
			}
			mustSubmit(t, e, Command{ID: fmtID(i), Kind: kind, Account: account, Amount: i})
		}
		path, size, err := e.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		e.Close()
		return data, size
	}
	jsonBytes, jsonSize := run(true)
	compactA, compactSize := run(false)
	compactB, _ := run(false)
	if compactSize >= jsonSize*2/3 {
		t.Fatalf("JSON=%d compact=%d", jsonSize, compactSize)
	}
	if !bytes.Equal(compactA, compactB) {
		t.Fatal("equivalent compact snapshots are not byte-identical")
	}
	if bytes.Equal(jsonBytes, compactA) {
		t.Fatal("ablation formats unexpectedly identical")
	}

	dir := t.TempDir()
	cfg := m1Config(dir, SyncEach)
	cfg.DedupeWindow = 2
	e := mustOpen(t, cfg)
	for i, id := range []string{"a", "b", "c"} {
		mustSubmit(t, e, Command{ID: id, Kind: Create, Account: AccountID(i + 1), Amount: 1})
	}
	if _, _, err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	e.Close()
	recovered := mustOpen(t, cfg)
	defer recovered.Close()
	if got := mustSubmit(t, recovered, Command{ID: "b", Kind: Create, Account: 2, Amount: 1}); !got.Duplicate {
		t.Fatalf("inside horizon=%+v", got)
	}
	got, err := recovered.Submit(context.Background(), Command{ID: "a", Kind: Deposit, Account: 1, Amount: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.Duplicate {
		t.Fatalf("retired ID remained duplicate: %+v", got)
	}
}

func TestM2FlowDeltaCorruptionFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*logRecord)
	}{
		{"bad version", func(r *logRecord) { r.FlowDelta[4] = 99 }},
		{"unknown field", func(r *logRecord) { r.FlowDelta[38] |= 0x80 }},
		{"unknown state", func(r *logRecord) { r.FlowDelta[40] = 9 }},
		{"impossible continuation", func(r *logRecord) { r.FlowDelta[41] = 99 }},
		{"truncated", func(r *logRecord) { r.FlowDelta = r.FlowDelta[:41] }},
		{"wrong prior", func(r *logRecord) { r.FlowDelta[6] = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := m1Config(dir, SyncEach)
			cfg.SegmentRecords = 100
			e := mustOpen(t, cfg)
			mustSubmit(t, e, Command{ID: "a", Kind: Create, Account: 1, Amount: 1})
			if tc.name == "wrong prior" {
				mustSubmit(t, e, Command{ID: "b", Kind: Deposit, Account: 1, Amount: 1})
			}
			e.Close()
			path := onlySegment(t, dir)
			if tc.name == "wrong prior" {
				rewriteRecordAt(t, path, 1, tc.mutate)
			} else {
				rewriteOnlyRecord(t, path, tc.mutate)
			}
			_, err := Open(cfg)
			var runtimeErr *RuntimeError
			if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryIncompatible {
				t.Fatalf("err=%v", err)
			}
		})
	}
	t.Run("invalid board value type", func(t *testing.T) {
		dir := t.TempDir()
		cfg := m1Config(dir, SyncEach)
		cfg.SegmentRecords = 100
		e := mustOpen(t, cfg)
		mustSubmit(t, e, Command{ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 25})
		e.Close()
		rewriteOnlyRecord(t, onlySegment(t, dir), func(r *logRecord) { r.FlowDelta[43] = 2 })
		_, err := Open(cfg)
		var runtimeErr *RuntimeError
		if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryIncompatible {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("wrong program fingerprint", func(t *testing.T) {
		dir := t.TempDir()
		cfg := m1Config(dir, SyncEach)
		e := mustOpen(t, cfg)
		mustSubmit(t, e, Command{ID: "a", Kind: Create, Account: 1, Amount: 1})
		e.Close()
		rewriteOnlyRecord(t, onlySegment(t, dir), func(r *logRecord) { r.ProgramID = "wrong" })
		_, err := Open(cfg)
		var runtimeErr *RuntimeError
		if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestM2CompactDedupeCorruptionFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*durableSnapshot)
	}{
		{"corrupt section", func(s *durableSnapshot) { s.DedupeCompact[len(s.DedupeCompact)-1] ^= 0xff }},
		{"count horizon mismatch", func(s *durableSnapshot) { binary.BigEndian.PutUint32(s.DedupeCompact[9:13], uint32(s.DedupeHorizon+1)) }},
		{"duplicate IDs", func(s *durableSnapshot) {
			s.DedupeCompact = bytes.Replace(s.DedupeCompact, []byte("bb"), []byte("aa"), 1)
		}},
		{"out of order", func(s *durableSnapshot) {
			at := bytes.Index(s.DedupeCompact, []byte("bb"))
			if at >= 0 {
				s.DedupeCompact[at+2] = 1
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := m1Config(dir, SyncEach)
			cfg.DedupeWindow = 10
			e := mustOpen(t, cfg)
			mustSubmit(t, e, Command{ID: "aa", Kind: Create, Account: 1, Amount: 1})
			mustSubmit(t, e, Command{ID: "bb", Kind: Create, Account: 2, Amount: 1})
			path, _, err := e.Snapshot()
			if err != nil {
				t.Fatal(err)
			}
			e.Close()
			rewriteSnapshot(t, path, tc.mutate)
			_, err = Open(cfg)
			var runtimeErr *RuntimeError
			if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func rewriteRecordAt(t *testing.T, path string, index int, mutate func(*logRecord)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pos := segmentHeaderSize
	var frames [][]byte
	var records []logRecord
	for pos < len(data)-segmentFooterSize {
		n := int(binary.BigEndian.Uint32(data[pos : pos+4]))
		var r logRecord
		if err := json.Unmarshal(data[pos+4:pos+4+n], &r); err != nil {
			t.Fatal(err)
		}
		records = append(records, r)
		frames = append(frames, append([]byte(nil), data[pos:pos+8+n]...))
		pos += 8 + n
	}
	mutate(&records[index])
	framed, err := frame(records[index])
	if err != nil {
		t.Fatal(err)
	}
	frames[index] = framed
	out := append([]byte(nil), data[:segmentHeaderSize]...)
	for _, f := range frames {
		out = append(out, f...)
	}
	out = append(out, encodeSegmentFooter(records[len(records)-1].Sequence, len(records))...)
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

func rewriteSnapshot(t *testing.T, path string, mutate func(*durableSnapshot)) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var s durableSnapshot
	if err := json.Unmarshal(data[52:], &s); err != nil {
		t.Fatal(err)
	}
	mutate(&s)
	payload, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(payload)
	out := make([]byte, 52+len(payload))
	copy(out[:8], []byte(m1SnapshotMagic))
	binary.BigEndian.PutUint32(out[8:12], m1SnapshotVersion)
	binary.BigEndian.PutUint64(out[12:20], uint64(len(payload)))
	copy(out[20:52], digest[:])
	copy(out[52:], payload)
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatal(err)
	}
}

func fmtID(i int) string { return "command-" + strings.Repeat("0", 8-len(stringInt(i))) + stringInt(i) }
func stringInt(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	return string(b[p:])
}
