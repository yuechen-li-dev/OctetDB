package octetdb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

var catalogBenchmarkRecord catalogTestRecord
var catalogBenchmarkDecision KeyedDecision

func BenchmarkCatalogPointRead(b *testing.B) {
	ctx := context.Background()
	db, dataset := openQueryTestDataset(b, filepath.Join(b.TempDir(), "db"), "bench", "records", DefaultKeyedOptions())
	defer db.Close()
	putQueryRecords(b, dataset, "seed", map[string]any{"record": catalogTestRecord{Value: 1}})
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var record catalogTestRecord
		found, err := dataset.Get(ctx, "record", &record)
		if err != nil || !found {
			b.Fatalf("found=%v err=%v", found, err)
		}
		catalogBenchmarkRecord = record
	}
}

func BenchmarkCatalogAtomicMutation(b *testing.B) {
	ctx := context.Background()
	options := DefaultKeyedOptions()
	options.DedupeHorizon = b.N + 10
	db, dataset := openQueryTestDataset(b, filepath.Join(b.TempDir(), "db"), "bench", "records", options)
	defer db.Close()
	putQueryRecords(b, dataset, "seed", map[string]any{"record": catalogTestRecord{Value: 0}})
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		decision, err := db.Mutate(ctx, KeyedCommand{ID: fmt.Sprintf("mutation-%d", index)}, func(tx *Tx) (any, error) {
			var record catalogTestRecord
			found, err := tx.Get(dataset, "record", &record)
			if err != nil || !found {
				return nil, err
			}
			record.Value++
			return record, tx.Put(dataset, "record", record)
		})
		if err != nil {
			b.Fatal(err)
		}
		catalogBenchmarkDecision = decision
	}
}
