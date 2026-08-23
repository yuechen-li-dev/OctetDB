package octetdb

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

var queryBenchmarkCount int
var queryBenchmarkProjection string

func BenchmarkDatasetQuery(b *testing.B) {
	for _, size := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("records=%d", size), func(b *testing.B) {
			options := DefaultKeyedOptions()
			options.MaxRecords = size + 10
			options.MaxTransactionBytes = 32 << 20
			db, dataset := openQueryTestDataset(b, filepath.Join(b.TempDir(), "db"), "bench", "records", options)
			defer db.Close()
			seed := make(map[string]any, size)
			control := make([]queryTestRecord, size)
			for index := 0; index < size; index++ {
				key := fmt.Sprintf("%06d", index)
				value := queryTestRecord{ID: key, Status: map[bool]string{true: "ready", false: "claimed"}[index%4 == 0], Stock: index}
				seed[key] = value
				control[index] = value
			}
			putQueryRecords(b, dataset, "seed", seed)

			benchmarkHandwrittenQueries(b, control)
			benchmarkPublicQueries(b, dataset)
		})
	}
}

func benchmarkHandwrittenQueries(b *testing.B, records []queryTestRecord) {
	for _, operation := range []struct {
		name string
		run  func() (int, string)
	}{
		{"Filter", func() (int, string) {
			count := 0
			for _, value := range records {
				if value.Status == "ready" {
					count++
				}
			}
			return count, ""
		}},
		{"FilterTake10", func() (int, string) {
			count := 0
			for _, value := range records {
				if value.Status == "ready" {
					count++
					if count == 10 {
						break
					}
				}
			}
			return count, ""
		}},
		{"FilterMap", func() (int, string) {
			count := 0
			projected := ""
			for _, value := range records {
				if value.Status == "ready" {
					count++
					projected = value.ID
				}
			}
			return count, projected
		}},
		{"Count", func() (int, string) { return len(records), "" }},
	} {
		b.Run("Handwritten/"+operation.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				queryBenchmarkCount, queryBenchmarkProjection = operation.run()
			}
		})
	}
}

func benchmarkPublicQueries(b *testing.B, dataset *Dataset) {
	for _, operation := range []struct {
		name string
		run  func() (int, string, error)
	}{
		{"Filter", func() (int, string, error) {
			count := 0
			err := ScanDataset(context.Background(), dataset, func(_ string, value queryTestRecord) (ScanAction, error) {
				if value.Status == "ready" {
					count++
				}
				return ScanContinue, nil
			})
			return count, "", err
		}},
		{"FilterTake10", func() (int, string, error) {
			count := 0
			err := ScanDataset(context.Background(), dataset, func(_ string, value queryTestRecord) (ScanAction, error) {
				if value.Status == "ready" {
					count++
					if count == 10 {
						return ScanStop, nil
					}
				}
				return ScanContinue, nil
			})
			return count, "", err
		}},
		{"FilterMap", func() (int, string, error) {
			count := 0
			projected := ""
			err := ScanDataset(context.Background(), dataset, func(_ string, value queryTestRecord) (ScanAction, error) {
				if value.Status == "ready" {
					count++
					projected = value.ID
				}
				return ScanContinue, nil
			})
			return count, projected, err
		}},
		{"Count", func() (int, string, error) {
			count := 0
			err := dataset.Scan(context.Background(), func(DatasetRecord) (ScanAction, error) { count++; return ScanContinue, nil })
			return count, "", err
		}},
	} {
		b.Run("Public/"+operation.name, func(b *testing.B) {
			b.ReportAllocs()
			for iteration := 0; iteration < b.N; iteration++ {
				count, projection, err := operation.run()
				if err != nil {
					b.Fatal(err)
				}
				queryBenchmarkCount, queryBenchmarkProjection = count, projection
			}
		})
	}
}
