package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/yuechen-li-dev/octetdb"
)

func main() {
	dir, err := os.MkdirTemp("", "octetdb-restart-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()
	options := octetdb.Options{Path: dir}
	batch := []octetdb.Command{
		{ID: "create-a", Kind: octetdb.Create, AccountID: 1, Amount: 100},
		{ID: "create-b", Kind: octetdb.Create, AccountID: 2, Amount: 10},
		{ID: "transfer-1", Kind: octetdb.Transfer, AccountID: 1, OtherAccountID: 2, Amount: 25},
	}

	db, err := octetdb.Open(ctx, options)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.SubmitBatch(ctx, batch); err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, err = octetdb.Open(ctx, options)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	retried, err := db.SubmitBatch(ctx, batch)
	if err != nil {
		log.Fatal(err)
	}
	account, _ := db.Get(1)
	fmt.Printf("duplicates=%v,%v,%v balance=%d\n", retried[0].Duplicate, retried[1].Duplicate, retried[2].Duplicate, account.Balance)
}
