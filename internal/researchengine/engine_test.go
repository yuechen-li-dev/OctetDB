package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	generated "github.com/yuechen-li-dev/octetdb/internal/model"
)

func TestGeneratedSeamStepCheckpointRestore(t *testing.T) {
	machine := generated.NewAccountAgent(1)
	ctx := generated.Main_CommandContext{Kind: generated.NewCommandKindCreate(), AccountA: 1, Amount: 100, StatusA: generated.NewAccountStatusMissing()}
	turn, err := machine.Step(ctx)
	if err != nil {
		t.Fatal(err)
	}
	decision, err := turn.Yielded()
	if err != nil || !decision.Accepted || decision.NewBalanceA != 100 {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	cp, err := machine.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	restored, err := generated.RestoreAccountAgent(cp)
	if err != nil {
		t.Fatal(err)
	}
	ctx.Kind = generated.NewCommandKindDeposit()
	ctx.ExistsA = true
	ctx.BalanceA = 100
	ctx.StatusA = generated.NewAccountStatusOpen()
	ctx.VersionA = 1
	ctx.Amount = 5
	left, _ := machine.Step(ctx)
	right, _ := restored.Step(ctx)
	ld, _ := left.Yielded()
	rd, _ := right.Yielded()
	if ld != rd || rd.NewBalanceA != 105 || rd.TransitionCount != 2 {
		t.Fatalf("baseline=%+v restored=%+v", ld, rd)
	}
	if _, err := AdmitPositiveAmount(-1); err == nil {
		t.Fatal("refinement admitted negative amount")
	}
}

func TestDurableTransferDuplicateRecoveryAndPublication(t *testing.T) {
	path := filepath.Join(t.TempDir(), "transitions.wal")
	e := mustOpen(t, Config{MailboxCapacity: 8, Durability: SyncEach, LogPath: path})
	mustSubmit(t, e, Command{ID: "create-a", Kind: Create, Account: 1, Amount: 100})
	mustSubmit(t, e, Command{ID: "create-b", Kind: Create, Account: 2, Amount: 40})
	result := mustSubmit(t, e, Command{ID: "transfer", Kind: Transfer, Account: 1, Other: 2, Amount: 25})
	if !result.Accepted {
		t.Fatalf("transfer rejected: %+v", result)
	}
	beforeBytes, beforeHash := e.Store().CanonicalOctagon()
	ledgerBefore := e.Store().LedgerLen()
	duplicate := mustSubmit(t, e, Command{ID: "transfer", Kind: Transfer, Account: 1, Other: 2, Amount: 25})
	if !duplicate.Duplicate || e.Store().LedgerLen() != ledgerBefore {
		t.Fatalf("duplicate=%+v ledger=%d", duplicate, e.Store().LedgerLen())
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	recovered := mustOpen(t, Config{MailboxCapacity: 8, Durability: SyncEach, LogPath: path})
	defer recovered.Close()
	a, _ := recovered.Store().Account(1)
	b, _ := recovered.Store().Account(2)
	if a.Balance != 75 || b.Balance != 65 || a.Version != 2 || b.Version != 2 {
		t.Fatalf("recovered a=%+v b=%+v", a, b)
	}
	afterBytes, afterHash := recovered.Store().CanonicalOctagon()
	if string(beforeBytes) != string(afterBytes) || beforeHash != afterHash {
		t.Fatal("canonical publication changed across restart")
	}
	mustSubmit(t, recovered, Command{ID: "deposit", Kind: Deposit, Account: 1, Amount: 1})
	_, changedHash := recovered.Store().CanonicalOctagon()
	if changedHash == afterHash {
		t.Fatal("logical hash did not change after commit")
	}
}

func TestWorkflowBoardCheckpointSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "workflow.wal")
	e := mustOpen(t, Config{Durability: SyncEach, LogPath: path})
	mustSubmit(t, e, Command{ID: "a", Kind: Create, Account: 1, Amount: 100})
	mustSubmit(t, e, Command{ID: "b", Kind: Create, Account: 2, Amount: 0})
	begin := mustSubmit(t, e, Command{ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 30})
	if !begin.Accepted {
		t.Fatalf("begin=%+v", begin)
	}
	e.Close()
	e = mustOpen(t, Config{Durability: SyncEach, LogPath: path})
	defer e.Close()
	confirm := mustSubmit(t, e, Command{ID: "confirm", Kind: Confirm, Account: 1, Other: 2, Amount: 30})
	if !confirm.Accepted || confirm.TransitionCount != 3 {
		t.Fatalf("confirm=%+v", confirm)
	}
	a, _ := e.Store().Account(1)
	b, _ := e.Store().Account(2)
	if a.Balance != 70 || b.Balance != 30 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
}

func TestUtilityDecisionsMatchConventionalGoControl(t *testing.T) {
	oct := mustOpen(t, Config{Durability: MemoryOnly})
	defer oct.Close()
	control, err := OpenGoBaseline(Config{Durability: MemoryOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	commands := []Command{
		{ID: "a", Kind: Create, Account: 1, Amount: 100},
		{ID: "b", Kind: Create, Account: 2, Amount: 10},
		{ID: "bad-amount", Kind: Deposit, Account: 1, Amount: -2},
		{ID: "missing", Kind: Withdraw, Account: 3, Amount: 1},
		{ID: "too-much", Kind: Withdraw, Account: 1, Amount: 1000},
		{ID: "freeze", Kind: Freeze, Account: 1},
		{ID: "frozen", Kind: Withdraw, Account: 1, Amount: 1},
		{ID: "unfreeze", Kind: Unfreeze, Account: 1},
		{ID: "transfer", Kind: Transfer, Account: 1, Other: 2, Amount: 20},
		{ID: "begin", Kind: BeginTransfer, Account: 1, Other: 2, Amount: 15},
		{ID: "confirm", Kind: Confirm, Account: 1, Other: 2, Amount: 15},
		{ID: "cancel", Kind: Cancel, Account: 1},
	}
	for _, command := range commands {
		got := mustSubmit(t, oct, command)
		want, err := control.Execute(command)
		if err != nil {
			t.Fatal(err)
		}
		if got.Accepted != want.Accepted || got.ReasonTag != want.ReasonTag || got.EffectTag != want.EffectTag {
			t.Fatalf("%s: oct=%+v go=%+v", command.ID, got, want)
		}
	}
	octBytes, _ := oct.Store().CanonicalOctagon()
	goBytes, _ := control.Store().CanonicalOctagon()
	if string(octBytes) != string(goBytes) {
		t.Fatalf("state differs\noct:\n%s\ngo:\n%s", octBytes, goBytes)
	}
}

func TestRejectedFreezeAndDurabilityFailureDoNotMutateAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "failure.wal")
	e := mustOpen(t, Config{Durability: SyncEach, LogPath: path})
	defer e.Close()
	mustSubmit(t, e, Command{ID: "create", Kind: Create, Account: 1, Amount: 50})
	mustSubmit(t, e, Command{ID: "freeze", Kind: Freeze, Account: 1})
	rejected := mustSubmit(t, e, Command{ID: "withdraw", Kind: Withdraw, Account: 1, Amount: 10})
	if rejected.Accepted {
		t.Fatal("frozen withdrawal accepted")
	}
	a, _ := e.Store().Account(1)
	if a.Balance != 50 || a.Version != 2 {
		t.Fatalf("rejection mutated account: %+v", a)
	}
	e.InjectDurabilityFailure(errors.New("disk unavailable"))
	_, err := e.Submit(context.Background(), Command{ID: "failed", Kind: Unfreeze, Account: 1})
	var runtimeErr *RuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Kind != DurabilityWriteFailed {
		t.Fatalf("err=%v", err)
	}
	a, _ = e.Store().Account(1)
	if a.Status != StatusFrozen || a.Version != 2 {
		t.Fatalf("failed durability published state: %+v", a)
	}
	success := mustSubmit(t, e, Command{ID: "unfreeze", Kind: Unfreeze, Account: 1})
	if success.TransitionCount != 4 {
		t.Fatalf("failed staged turn leaked into board: %+v", success)
	}
}

func TestTruncatedTailIsRemovedAndChecksumCorruptionStops(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tail.wal")
	e := mustOpen(t, Config{Durability: SyncEach, LogPath: path})
	mustSubmit(t, e, Command{ID: "create", Kind: Create, Account: 1, Amount: 5})
	e.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 20, '{'})
	f.Close()
	recovered := mustOpen(t, Config{Durability: SyncEach, LogPath: path})
	if !recovered.RecoveryTruncatedTail() {
		t.Fatal("truncated tail was not reported")
	}
	recovered.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	corrupt := filepath.Join(dir, "corrupt.wal")
	if err := os.WriteFile(corrupt, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(Config{Durability: SyncEach, LogPath: corrupt})
	var runtimeErr *RuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Kind != RecoveryCorrupt {
		t.Fatalf("corruption err=%v", err)
	}
}

func TestBoundedMailboxRejectsAndIndependentAgentsReachStateView(t *testing.T) {
	entered := make(chan AccountID, 4)
	release := make(chan struct{})
	var once sync.Once
	e := mustOpen(t, Config{MailboxCapacity: 1, Durability: MemoryOnly, Trace: func(event TraceEvent) {
		if event.Phase == "mailbox_dequeue" && event.CommandID == "block" {
			once.Do(func() { close(entered) })
			<-release
		}
	}})
	defer e.Close()
	firstDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), Command{ID: "block", Kind: Create, Account: 1, Amount: 1})
		firstDone <- err
	}()
	<-entered
	secondDone := make(chan error, 1)
	go func() {
		_, err := e.Submit(context.Background(), Command{ID: "queued", Kind: Deposit, Account: 1, Amount: 1})
		secondDone <- err
	}()
	time.Sleep(10 * time.Millisecond)
	_, err := e.Submit(context.Background(), Command{ID: "overflow", Kind: Deposit, Account: 1, Amount: 1})
	var runtimeErr *RuntimeError
	if !errors.As(err, &runtimeErr) || runtimeErr.Kind != MailboxFull {
		t.Fatalf("overflow err=%v", err)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}

	views := make(chan AccountID, 2)
	gate := make(chan struct{})
	e.cfg.Trace = func(event TraceEvent) {
		if event.Phase == "state_view" && (event.CommandID == "i1" || event.CommandID == "i2") {
			views <- event.Agent
			<-gate
		}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		e.Submit(context.Background(), Command{ID: "i1", Kind: Create, Account: 10, Amount: 1})
	}()
	go func() {
		defer wg.Done()
		e.Submit(context.Background(), Command{ID: "i2", Kind: Create, Account: 20, Amount: 1})
	}()
	select {
	case <-views:
	case <-time.After(time.Second):
		t.Fatal("first independent command did not reach view")
	}
	select {
	case <-views:
	case <-time.After(time.Second):
		t.Fatal("independent command was serialized before state view")
	}
	close(gate)
	wg.Wait()
}

func TestPostgreSQLBaseline(t *testing.T) {
	dsn := os.Getenv("DBSCHED_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set DBSCHED_POSTGRES_DSN for the real PostgreSQL control")
	}
	ctx := context.Background()
	p, err := OpenPostgreSQL(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	commands := []Command{{ID: "pg-a", Kind: Create, Account: 1, Amount: 100}, {ID: "pg-b", Kind: Create, Account: 2, Amount: 0}, {ID: "pg-transfer", Kind: Transfer, Account: 1, Other: 2, Amount: 25}, {ID: "pg-freeze", Kind: Freeze, Account: 1}, {ID: "pg-rejected", Kind: Withdraw, Account: 1, Amount: 1}}
	for _, command := range commands {
		if _, err := p.Execute(ctx, command); err != nil {
			t.Fatal(err)
		}
	}
	a, err := p.Account(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.Account(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if a.Balance != 75 || a.Status != StatusFrozen || b.Balance != 25 {
		t.Fatalf("a=%+v b=%+v", a, b)
	}
	duplicate, err := p.Execute(ctx, commands[2])
	if err != nil || !duplicate.Duplicate {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
}

func mustOpen(t *testing.T, cfg Config) *Engine {
	t.Helper()
	e, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return e
}
func mustSubmit(t *testing.T, e *Engine, command Command) Result {
	t.Helper()
	r, err := e.Submit(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
