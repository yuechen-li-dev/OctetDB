package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/yuechen-li-dev/octetdb"
)

func main() {
	dir, err := os.MkdirTemp("", "octetdb-minimal-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	db, err := octetdb.Open(context.Background(), octetdb.Options{Path: dir})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	result, err := db.Submit(context.Background(), octetdb.Command{
		ID: "create-primary", Kind: octetdb.Create, AccountID: 1, Amount: 100,
	})
	if err != nil {
		log.Fatal(err)
	}
	account, ok := db.Get(1)
	fmt.Printf("accepted=%v account=%+v found=%v\n", result.Accepted, account, ok)
}
