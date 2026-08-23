package octetdb_test

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/yuechen-li-dev/octetdb"
)

func Example() {
	dir, err := os.MkdirTemp("", "octetdb-example-")
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
		ID: "create-1", Kind: octetdb.Create, AccountID: 1, Amount: 100,
	})
	if err != nil {
		log.Fatal(err)
	}
	account, found := db.Get(1)
	fmt.Println(result.Accepted, account.Balance, found)
	// Output: true 100 true
}
