package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"github.com/yuechen-li-dev/octetdb"
)

func main() {
	ctx := context.Background()
	path := filepath.Join(".", "octetdb-data")
	options := octetdb.Options{Path: path}
	commands := []octetdb.Command{
		{ID: "create-a", Kind: octetdb.Create, AccountID: 1, Amount: 100},
		{ID: "create-b", Kind: octetdb.Create, AccountID: 2, Amount: 50},
		{ID: "transfer-1", Kind: octetdb.Transfer, AccountID: 1, OtherAccountID: 2, Amount: 25},
	}

	db, err := octetdb.Open(ctx, options)
	must(err)
	_, err = db.SubmitBatch(ctx, commands)
	must(err)
	must(db.Close())

	db, err = octetdb.Open(ctx, options)
	must(err)
	a, aOK := db.Get(1)
	b, bOK := db.Get(2)
	retry, err := db.Submit(ctx, commands[2])
	must(err)
	if !aOK || !bOK || a.Balance != 75 || b.Balance != 75 || !retry.Duplicate {
		log.Fatalf("restart proof failed: a=%+v b=%+v retry=%+v", a, b, retry)
	}

	_, err = db.Submit(ctx, octetdb.Command{Kind: octetdb.Deposit, AccountID: 1, Amount: 1})
	mustKind(err, octetdb.ErrorInvalidInput)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = db.Submit(cancelled, octetdb.Command{ID: "cancelled", Kind: octetdb.Deposit, AccountID: 1, Amount: 1})
	if !errors.Is(err, context.Canceled) {
		log.Fatalf("cancelled submit: %v", err)
	}
	afterCancel, _ := db.Get(1)
	if afterCancel.Balance != 75 {
		log.Fatalf("cancelled command changed state: %+v", afterCancel)
	}
	must(db.Close())
	db, err = octetdb.Open(ctx, options)
	must(err)
	afterCancel, _ = db.Get(1)
	if afterCancel.Balance != 75 {
		log.Fatalf("cancelled command changed durable state: %+v", afterCancel)
	}
	must(db.Close())
	_, err = db.Submit(ctx, octetdb.Command{ID: "closed", Kind: octetdb.Deposit, AccountID: 1, Amount: 1})
	mustKind(err, octetdb.ErrorClosed)

	capacityPath := filepath.Join(".", "capacity-data")
	capacityDB, err := octetdb.Open(ctx, octetdb.Options{Path: capacityPath, MaxAccounts: 1})
	must(err)
	_, err = capacityDB.Submit(ctx, octetdb.Command{ID: "one", Kind: octetdb.Create, AccountID: 1})
	must(err)
	_, err = capacityDB.Submit(ctx, octetdb.Command{ID: "two", Kind: octetdb.Create, AccountID: 2})
	mustKind(err, octetdb.ErrorCapacity)
	must(capacityDB.Close())

	corruptPath := filepath.Join(".", "corrupt-data")
	corruptDB, err := octetdb.Open(ctx, octetdb.Options{Path: corruptPath})
	must(err)
	_, err = corruptDB.Submit(ctx, octetdb.Command{ID: "create", Kind: octetdb.Create, AccountID: 1})
	must(err)
	must(corruptDB.Close())
	wal := filepath.Join(corruptPath, "wal.oct")
	data, err := os.ReadFile(wal)
	must(err)
	data[len(data)-1] ^= 0xff
	must(os.WriteFile(wal, data, 0o600))
	_, err = octetdb.Open(ctx, octetdb.Options{Path: corruptPath})
	mustKind(err, octetdb.ErrorCorruption)

	entries, err := os.ReadDir(path)
	must(err)
	names := make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}
	sort.Strings(names)
	if fmt.Sprint(names) != "[FORMAT wal.oct]" {
		log.Fatalf("unexpected product files: %v", names)
	}
	fmt.Printf("external-ok balances=%d,%d duplicate=%v files=%v format=%d\n", a.Balance, b.Balance, retry.Duplicate, names, octetdb.FormatVersion)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func mustKind(err error, want octetdb.ErrorKind) {
	var dbErr *octetdb.Error
	if !errors.As(err, &dbErr) || dbErr.Kind != want {
		log.Fatalf("error=%v kind=%v want=%v", err, dbErr, want)
	}
}
