package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func openTestC2(t *testing.T, dir string) *SpecializedEngine {
	t.Helper()
	e, err := OpenSpecialized(SpecializedConfig{StorageDir: dir, Durability: BatchSync, DedupeWindow: 32, AccountHint: 4})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func seedC2(t *testing.T, e *SpecializedEngine) {
	t.Helper()
	results, err := e.SubmitBatch([]Command{
		{ID: "create-9001", Kind: Create, Account: 9001, Amount: 100},
		{ID: "create-42", Kind: Create, Account: 42, Amount: 50},
	})
	if err != nil || len(results) != 2 || !results[0].Accepted || !results[1].Accepted {
		t.Fatalf("seed results=%+v err=%v", results, err)
	}
}

func TestSpecializedBatchSemanticsAndDenseArbitraryIDs(t *testing.T) {
	e := openTestC2(t, t.TempDir())
	defer e.Close()
	seedC2(t, e)
	results, err := e.SubmitBatch([]Command{
		{ID: "x-1", Kind: Transfer, Account: 9001, Other: 42, Amount: 10},
		{ID: "x-2", Kind: Transfer, Account: 9001, Other: 42, Amount: 15},
		{ID: "x-1", Kind: Transfer, Account: 9001, Other: 42, Amount: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].Accepted || !results[1].Accepted || !results[2].Duplicate || results[2].Sequence != results[0].Sequence {
		t.Fatalf("unexpected results: %+v", results)
	}
	a, _ := e.Account(9001)
	b, _ := e.Account(42)
	if a.Balance != 75 || b.Balance != 75 || a.Version != 3 || b.Version != 3 {
		t.Fatalf("state a=%+v b=%+v", a, b)
	}
	if e.LedgerLen() != 4 {
		t.Fatalf("ledger entries=%d, want 4 accepted unique effects", e.LedgerLen())
	}
}

func TestSpecializedSnapshotTailRecoveryAndDuplicate(t *testing.T) {
	dir := t.TempDir()
	e := openTestC2(t, dir)
	seedC2(t, e)
	if _, _, err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(Command{ID: "tail", Kind: Transfer, Account: 9001, Other: 42, Amount: 7}); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := openTestC2(t, dir)
	defer recovered.Close()
	a, _ := recovered.Account(9001)
	b, _ := recovered.Account(42)
	if a.Balance != 93 || b.Balance != 57 {
		t.Fatalf("recovered state a=%+v b=%+v", a, b)
	}
	if recovered.LedgerLen() != 3 {
		t.Fatalf("recovered ledger entries=%d, want 3", recovered.LedgerLen())
	}
	duplicate, err := recovered.Submit(Command{ID: "tail", Kind: Transfer, Account: 9001, Other: 42, Amount: 7})
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	stats := recovered.RecoveryMetrics()
	if stats.SnapshotSequence != 2 || stats.RecordsReplayed != 1 {
		t.Fatalf("recovery stats=%+v", stats)
	}
}

func TestSpecializedIncompleteTailIsIgnored(t *testing.T) {
	dir := t.TempDir()
	e := openTestC2(t, dir)
	seedC2(t, e)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "c2.wal")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{100, 0, 0, 0, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	recovered := openTestC2(t, dir)
	defer recovered.Close()
	if a, ok := recovered.Account(9001); !ok || a.Balance != 100 {
		t.Fatalf("account=%+v ok=%v", a, ok)
	}
	info, _ := os.Stat(path)
	if info.Size() <= c2HeaderSize || info.Size() >= c2HeaderSize+300 {
		t.Fatalf("unexpected truncated size %d", info.Size())
	}
}

func TestSpecializedCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	e := openTestC2(t, dir)
	seedC2(t, e)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "c2.wal")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSpecialized(SpecializedConfig{StorageDir: dir, Durability: BatchSync, DedupeWindow: 32}); err == nil {
		t.Fatal("corrupt WAL was accepted")
	}
}

func TestSpecializedSnapshotCorruptionFailsClosed(t *testing.T) {
	dir := t.TempDir()
	e := openTestC2(t, dir)
	seedC2(t, e)
	path, _, err := e.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSpecialized(SpecializedConfig{StorageDir: dir, Durability: BatchSync, DedupeWindow: 32}); err == nil {
		t.Fatal("corrupt snapshot was accepted")
	}
}

func TestSpecializedMatchesDirectGoAcceptedRejectedAndWorkflow(t *testing.T) {
	baseline, err := OpenGoM1Baseline(Config{StorageDir: filepath.Join(t.TempDir(), "go"), Durability: BatchSync, GroupMax: 1, DedupeWindow: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer baseline.Close()
	c2, err := OpenSpecialized(SpecializedConfig{StorageDir: filepath.Join(t.TempDir(), "c2"), Durability: BatchSync, DedupeWindow: 64, AccountHint: 2, RecordHint: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	commands := []Command{
		{ID: "create-a", Kind: Create, Account: 100, Amount: 10},
		{ID: "create-b", Kind: Create, Account: 200, Amount: 5},
		{ID: "too-much", Kind: Withdraw, Account: 100, Amount: 99},
		{ID: "freeze", Kind: Freeze, Account: 100},
		{ID: "frozen-transfer", Kind: Transfer, Account: 100, Other: 200, Amount: 1},
		{ID: "unfreeze", Kind: Unfreeze, Account: 100},
		{ID: "begin", Kind: BeginTransfer, Account: 100, Other: 200, Amount: 2},
		{ID: "confirm", Kind: Confirm, Account: 100, Other: 200, Amount: 2},
		{ID: "cancel", Kind: Cancel, Account: 100},
		{ID: "deposit", Kind: Deposit, Account: 200, Amount: 3},
	}
	for _, command := range commands {
		want, err := baseline.Submit(context.Background(), command)
		if err != nil {
			t.Fatalf("baseline %s: %v", command.ID, err)
		}
		got, err := c2.Submit(command)
		if err != nil {
			t.Fatalf("C2 %s: %v", command.ID, err)
		}
		if got.Sequence != want.Sequence || got.Accepted != want.Accepted || got.ReasonTag != want.ReasonTag || got.EffectTag != want.EffectTag || got.TransitionCount != want.TransitionCount {
			t.Fatalf("%s mismatch\nwant=%+v\n got=%+v", command.ID, want, got)
		}
	}
	for _, id := range []AccountID{100, 200} {
		want, wantOK := baseline.Store().Account(id)
		got, gotOK := c2.Account(id)
		if gotOK != wantOK || got != want {
			t.Fatalf("account %d mismatch want=%+v/%v got=%+v/%v", id, want, wantOK, got, gotOK)
		}
	}
	if c2.LedgerLen() != baseline.Store().LedgerLen() {
		t.Fatalf("ledger length mismatch want=%d got=%d", baseline.Store().LedgerLen(), c2.LedgerLen())
	}
}

func BenchmarkAccountLookupMap(b *testing.B) {
	accounts := make(map[AccountID]Account, 100_000)
	for i := 1; i <= 100_000; i++ {
		id := AccountID(i*17 + 3)
		accounts[id] = Account{ID: id, Balance: i, Status: StatusOpen, Version: 1}
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sum int
	for i := 0; i < b.N; i++ {
		sum += accounts[AccountID((i%100_000+1)*17+3)].Balance
	}
	_ = sum
}

func BenchmarkAccountLookupDense(b *testing.B) {
	index := make(map[AccountID]uint32, 100_000)
	accounts := make([]Account, 100_000)
	for i := 0; i < 100_000; i++ {
		id := AccountID((i+1)*17 + 3)
		index[id] = uint32(i)
		accounts[i] = Account{ID: id, Balance: i + 1, Status: StatusOpen, Version: 1}
	}
	b.ReportAllocs()
	b.ResetTimer()
	var sum int
	for i := 0; i < b.N; i++ {
		sum += accounts[index[AccountID((i%100_000+1)*17+3)]].Balance
	}
	_ = sum
}

func BenchmarkWALRecordJSON(b *testing.B) {
	record := logRecord{Version: m1WALVersion, SchemaID: m1SchemaID, ProgramID: m1GoProgramID, Sequence: 1, AgentID: 1, CommandID: "x-21000000001", CommandKind: Transfer.Tag, AccountA: 1, AccountB: 2, Amount: 1, Result: Result{Sequence: 1, CommandID: "x-21000000001", Accepted: true, EffectTag: 3, TransitionCount: 2}, EffectTag: 3, NewBalanceA: 999, NewBalanceB: 1001, ExpectedA: 1, ExpectedB: 1, Checkpoint: []byte(`{"pending":{"active":false,"target":0,"amount":0},"turns":2}`)}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALRecordC2Reusable(b *testing.B) {
	record := c2Record{sequence: 1, commandID: "x-21000000001", kind: Transfer.Tag, a: 1, b: 2, amount: 1, accepted: true, effect: 3, turns: 2, newBalanceA: 999, newBalanceB: 1001, expectedA: 1, expectedB: 1}
	buffer := make([]byte, 0, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buffer = appendC2Record(buffer[:0], record)
	}
	_ = buffer
}
