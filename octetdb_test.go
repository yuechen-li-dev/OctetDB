package octetdb_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/yuechen-li-dev/octetdb"
)

func openDB(t *testing.T, path string, mutate ...func(*octetdb.Options)) *octetdb.DB {
	t.Helper()
	options := octetdb.Options{Path: path, MaxAccounts: 32, DedupeHorizon: 128, BatchMax: 32}
	for _, fn := range mutate {
		fn(&options)
	}
	db, err := octetdb.Open(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPublicLifecycleSingleAndBatch(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	created, err := db.Submit(context.Background(), octetdb.Command{ID: "create-a", Kind: octetdb.Create, AccountID: 1, Amount: 100})
	if err != nil || !created.Accepted || created.Duplicate {
		t.Fatalf("create=%+v err=%v", created, err)
	}
	results, err := db.SubmitBatch(context.Background(), []octetdb.Command{
		{ID: "create-b", Kind: octetdb.Create, AccountID: 2, Amount: 10},
		{ID: "transfer", Kind: octetdb.Transfer, AccountID: 1, OtherAccountID: 2, Amount: 25},
	})
	if err != nil || len(results) != 2 || !results[0].Accepted || !results[1].Accepted {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	account, ok := db.Get(1)
	if !ok || account.Balance != 75 || account.Version != 2 {
		t.Fatalf("account=%+v ok=%v", account, ok)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openDB(t, dir)
	defer reopened.Close()
	account, ok = reopened.Get(2)
	if !ok || account.Balance != 35 || account.Version != 2 {
		t.Fatalf("recovered account=%+v ok=%v", account, ok)
	}
}

func TestBatchIdempotencyAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	commands := []octetdb.Command{
		{ID: "a", Kind: octetdb.Create, AccountID: 1, Amount: 10},
		{ID: "b", Kind: octetdb.Deposit, AccountID: 1, Amount: 5},
	}
	if _, err := db.SubmitBatch(context.Background(), commands); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir)
	defer db.Close()
	results, err := db.SubmitBatch(context.Background(), commands)
	if err != nil || !results[0].Duplicate || !results[1].Duplicate {
		t.Fatalf("results=%+v err=%v", results, err)
	}
	account, _ := db.Get(1)
	if account.Balance != 15 {
		t.Fatalf("duplicate retry changed balance: %+v", account)
	}
}

func TestHotKeySerialCorrectness(t *testing.T) {
	db := openDB(t, t.TempDir())
	defer db.Close()
	if _, err := db.Submit(context.Background(), octetdb.Command{ID: "create", Kind: octetdb.Create, AccountID: 1}); err != nil {
		t.Fatal(err)
	}
	const writers = 24
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := db.Submit(context.Background(), octetdb.Command{ID: string(rune('a' + i)), Kind: octetdb.Deposit, AccountID: 1, Amount: 1})
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
	account, _ := db.Get(1)
	if account.Balance != writers || account.Version != writers+1 {
		t.Fatalf("account=%+v", account)
	}
}

func TestSnapshotAndWALTailRecovery(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	if _, err := db.Submit(context.Background(), octetdb.Command{ID: "create", Kind: octetdb.Create, AccountID: 1, Amount: 10}); err != nil {
		t.Fatal(err)
	}
	if err := db.Snapshot(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.Stats().SnapshotSequence != 1 {
		t.Fatalf("stats=%+v", db.Stats())
	}
	if _, err := db.Submit(context.Background(), octetdb.Command{ID: "tail", Kind: octetdb.Deposit, AccountID: 1, Amount: 2}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = openDB(t, dir)
	defer db.Close()
	account, _ := db.Get(1)
	if account.Balance != 12 || db.Stats().CommittedSequence != 2 {
		t.Fatalf("account=%+v stats=%+v", account, db.Stats())
	}
}

func TestIncompleteWALTailIsDetectedAndDiscarded(t *testing.T) {
	dir := t.TempDir()
	db := openDB(t, dir)
	if _, err := db.Submit(context.Background(), octetdb.Command{ID: "create", Kind: octetdb.Create, AccountID: 1, Amount: 10}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	walPath := filepath.Join(dir, "wal.oct")
	before, _ := os.Stat(walPath)
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{20, 0, 0, 0, 1, 2, 3})
	_ = f.Close()
	db = openDB(t, dir)
	defer db.Close()
	after, _ := os.Stat(walPath)
	if after.Size() != before.Size() {
		t.Fatalf("incomplete tail was not truncated: before=%d after=%d", before.Size(), after.Size())
	}
}

func TestCorruptionAndFormatIncompatibilityFailClosed(t *testing.T) {
	t.Run("wal corruption", func(t *testing.T) {
		dir := t.TempDir()
		db := openDB(t, dir)
		_, _ = db.Submit(context.Background(), octetdb.Command{ID: "create", Kind: octetdb.Create, AccountID: 1})
		_ = db.Close()
		path := filepath.Join(dir, "wal.oct")
		data, _ := os.ReadFile(path)
		data[len(data)-1] ^= 0xff
		_ = os.WriteFile(path, data, 0o600)
		_, err := octetdb.Open(context.Background(), octetdb.Options{Path: dir, MaxAccounts: 32, DedupeHorizon: 128, BatchMax: 32})
		assertKind(t, err, octetdb.ErrorCorruption)
	})
	t.Run("snapshot corruption", func(t *testing.T) {
		dir := t.TempDir()
		db := openDB(t, dir)
		_, _ = db.Submit(context.Background(), octetdb.Command{ID: "create", Kind: octetdb.Create, AccountID: 1})
		if err := db.Snapshot(context.Background()); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
		path := filepath.Join(dir, "snapshot.oct")
		data, _ := os.ReadFile(path)
		data[len(data)-1] ^= 0xff
		_ = os.WriteFile(path, data, 0o600)
		_, err := octetdb.Open(context.Background(), octetdb.Options{Path: dir, MaxAccounts: 32, DedupeHorizon: 128, BatchMax: 32})
		assertKind(t, err, octetdb.ErrorCorruption)
	})
	t.Run("format marker", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, "FORMAT"), []byte("OCTETDB\nformat=999\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := octetdb.Open(context.Background(), octetdb.Options{Path: dir})
		assertKind(t, err, octetdb.ErrorIncompatible)
	})
}

func TestCapacityCancellationAndClose(t *testing.T) {
	db := openDB(t, t.TempDir(), func(options *octetdb.Options) { options.MaxAccounts = 1; options.BatchMax = 1 })
	if _, err := db.Submit(context.Background(), octetdb.Command{ID: "one", Kind: octetdb.Create, AccountID: 1}); err != nil {
		t.Fatal(err)
	}
	_, err := db.Submit(context.Background(), octetdb.Command{ID: "two", Kind: octetdb.Create, AccountID: 2})
	assertKind(t, err, octetdb.ErrorCapacity)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := db.Submit(ctx, octetdb.Command{ID: "cancelled", Kind: octetdb.Deposit, AccountID: 1, Amount: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = db.Submit(context.Background(), octetdb.Command{ID: "closed", Kind: octetdb.Deposit, AccountID: 1, Amount: 1})
	assertKind(t, err, octetdb.ErrorClosed)
}

func TestProductionDependencyDirection(t *testing.T) {
	command := exec.Command("go", "list", "-deps", ".")
	command.Dir = "."
	output, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.Contains(dependency, "/experiments/") || strings.Contains(dependency, "/cmd/") || strings.Contains(dependency, "/internal/bench") {
			t.Fatalf("production package depends on research/tooling package %q", dependency)
		}
	}
}

func assertKind(t *testing.T, err error, want octetdb.ErrorKind) {
	t.Helper()
	var dbErr *octetdb.Error
	if !errors.As(err, &dbErr) || dbErr.Kind != want {
		t.Fatalf("err=%v kind=%v want=%v", err, dbErr, want)
	}
}
