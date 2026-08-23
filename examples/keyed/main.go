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

	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		log.Fatal(err)
	}
	workers, err := db.Bucket(ctx, "workers")
	if err != nil {
		log.Fatal(err)
	}
	jobs, err := workers.Dataset(ctx, "jobs", octetdb.DatasetOptions{TypeIdentity: "example.Job/v1"})
	if err != nil {
		log.Fatal(err)
	}
	decision, err := jobs.Mutate(ctx, octetdb.KeyedCommand{ID: "create-job-42"}, func(tx *octetdb.DatasetTx) (any, error) {
		job := Job{ID: "42", Status: "ready"}
		return job, tx.Put("42", job)
	})
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Close(); err != nil {
		log.Fatal(err)
	}

	db, err = octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	workers, err = db.Bucket(ctx, "workers")
	if err != nil {
		log.Fatal(err)
	}
	jobs, err = workers.Dataset(ctx, "jobs", octetdb.DatasetOptions{TypeIdentity: "example.Job/v1"})
	if err != nil {
		log.Fatal(err)
	}
	var job Job
	ok, err := jobs.Get(ctx, "42", &job)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(decision.Applied, ok, job.Status)
}
