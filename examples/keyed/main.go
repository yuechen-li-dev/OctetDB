package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/yuechen-li-dev/octetdb"
)

type Job struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func main() {
	ctx := context.Background()
	path, err := os.MkdirTemp("", "octetdb-keyed-example-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(path)

	db, err := octetdb.OpenKeyed(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		log.Fatal(err)
	}
	decision, err := db.SubmitKeyed(ctx, octetdb.KeyedCommand{ID: "create-job-42"}, func(tx *octetdb.KeyedTx) (any, error) {
		job := Job{ID: "42", Status: "ready"}
		return job, tx.Put("jobs/42", job)
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, err = octetdb.OpenKeyed(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	var job Job
	ok, err := db.GetKeyed(ctx, "jobs/42", &job)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(decision.Applied, ok, job.Status)
}
