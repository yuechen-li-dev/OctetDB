package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yuechen-li-dev/octetdb"
)

type row struct {
	ID      int    `json:"id"`
	Balance int64  `json:"balance,omitempty"`
	Status  string `json:"status,omitempty"`
}
type result struct {
	Mode, Workload          string
	Population              int
	OpenNS                  int64
	Records                 int
	Invariant               bool
	SnapshotBytes, WALBytes int64
}

func main() {
	mode := flag.String("mode", "measure", "prepare-snapshot, prepare-wal, measure, or cold")
	workload := flag.String("workload", "w1", "w1 or w3")
	population := flag.Int("population", 10000, "records")
	path := flag.String("data", "", "database directory")
	output := flag.String("output", "", "measurement JSON")
	flag.Parse()
	if *path == "" {
		fail(fmt.Errorf("data is required"))
	}
	ctx := context.Background()
	if *mode == "cold" {
		start := time.Now()
		db, err := octetdb.OpenCatalog(ctx, *path, octetdb.DefaultKeyedOptions())
		elapsed := time.Since(start)
		if err != nil {
			fail(err)
		}
		if err := db.Close(); err != nil {
			fail(err)
		}
		write(*output, result{Mode: *mode, Workload: *workload, Population: 0, OpenNS: elapsed.Nanoseconds(), Invariant: true})
		return
	}
	if *mode == "prepare-snapshot" || *mode == "prepare-wal" {
		db, dataset := open(ctx, *path, *workload)
		for id := 0; id < *population; id++ {
			value := row{ID: id, Balance: 100000, Status: "ready"}
			_, err := db.Mutate(ctx, octetdb.KeyedCommand{ID: fmt.Sprintf("seed-%d", id)}, func(tx *octetdb.Tx) (any, error) { return value, tx.Put(dataset, fmt.Sprintf("%012d", id), value) })
			if err != nil {
				fail(err)
			}
		}
		if *mode == "prepare-snapshot" {
			if err := db.Close(); err != nil {
				fail(err)
			}
		}
		// prepare-wal intentionally exits without Close: the OS closes the file,
		// leaving the synchronized WAL tail as the recovery authority.
		return
	}
	start := time.Now()
	db, dataset := open(ctx, *path, *workload)
	elapsed := time.Since(start)
	count := 0
	total := int64(0)
	invariant := true
	err := octetdb.ScanDataset(ctx, dataset, func(_ string, value row) (octetdb.ScanAction, error) {
		count++
		total += value.Balance
		if *workload == "w3" && value.Status != "ready" {
			invariant = false
		}
		return octetdb.ScanContinue, nil
	})
	if err != nil {
		fail(err)
	}
	if *workload == "w1" && total != int64(*population)*100000 {
		invariant = false
	}
	if count != *population {
		invariant = false
	}
	result := result{Mode: *mode, Workload: *workload, Population: *population, OpenNS: elapsed.Nanoseconds(), Records: count, Invariant: invariant, SnapshotBytes: size(filepath.Join(*path, "snapshot")), WALBytes: size(filepath.Join(*path, "wal"))}
	if err := db.Close(); err != nil {
		fail(err)
	}
	write(*output, result)
}

func open(ctx context.Context, path, workload string) (*octetdb.Database, *octetdb.Dataset) {
	db, err := octetdb.OpenCatalog(ctx, path, octetdb.DefaultKeyedOptions())
	if err != nil {
		fail(err)
	}
	bucket, err := db.Bucket(ctx, "perfm4")
	if err != nil {
		fail(err)
	}
	dataset, err := bucket.Dataset(ctx, workload, octetdb.DatasetOptions{TypeIdentity: "perfm4.RecoveryRow/v1"})
	if err != nil {
		fail(err)
	}
	return db, dataset
}
func size(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
func write(path string, value result) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fail(err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, 0o644); err != nil {
		fail(err)
	}
}
func fail(err error) { fmt.Fprintln(os.Stderr, err); os.Exit(1) }
