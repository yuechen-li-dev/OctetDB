package octetdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var groupDevOutput = flag.String("group-dev-output", "", "write the opt-in group-commit development harness JSON")
var groupDevSmoke = flag.Bool("group-dev-smoke", false, "run the <=15 second development shape")

type groupDevMetric struct {
	Workload          string  `json:"workload"`
	Mode              string  `json:"mode"`
	Concurrency       int     `json:"concurrency"`
	Ops               int     `json:"ops"`
	OpsPerSecond      float64 `json:"ops_per_second"`
	P50NS             int64   `json:"p50_ns"`
	P95NS             int64   `json:"p95_ns"`
	P99NS             int64   `json:"p99_ns"`
	AllocsPerOp       float64 `json:"allocs_per_op"`
	BytesPerOp        float64 `json:"bytes_per_op"`
	WALBytesPerOp     float64 `json:"wal_bytes_per_op"`
	SyncCalls         uint64  `json:"sync_calls"`
	CommandsPerSync   float64 `json:"commands_per_sync"`
	GroupMin          int     `json:"group_size_min"`
	GroupMedian       int     `json:"group_size_median"`
	GroupP95          int     `json:"group_size_p95"`
	GroupMax          int     `json:"group_size_max"`
	CorrectnessDigest string  `json:"correctness_digest"`
}

type groupDevReport struct {
	GeneratedAt string           `json:"generated_at"`
	GoVersion   string           `json:"go_version"`
	OS          string           `json:"os"`
	Arch        string           `json:"arch"`
	Smoke       bool             `json:"smoke"`
	ElapsedMS   int64            `json:"elapsed_ms"`
	Metrics     []groupDevMetric `json:"metrics"`
}

type groupDevRecord struct {
	Value, Stock int
	State        string
	Processed    bool
}

func TestGroupCommitDevHarness(t *testing.T) {
	if *groupDevOutput == "" {
		t.Skip("opt in with -group-dev-output")
	}
	started := time.Now()
	population, ops := 256, 384
	if *groupDevSmoke {
		population, ops = 64, 96
	}
	report := groupDevReport{GeneratedAt: started.UTC().Format(time.RFC3339), GoVersion: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, Smoke: *groupDevSmoke}
	for _, mode := range []string{"baseline", "group_commit"} {
		for _, workload := range []string{"D1-transfer", "D2-inventory", "D3-job", "D4-webhook"} {
			for _, concurrency := range []int{1, 8, 32} {
				report.Metrics = append(report.Metrics, runGroupDevLane(t, mode, workload, concurrency, population, ops))
			}
		}
	}
	report.ElapsedMS = time.Since(started).Milliseconds()
	payload, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*groupDevOutput, append(payload, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("group-commit dev harness: %s (%s)", time.Since(started), *groupDevOutput)
}

func runGroupDevLane(t *testing.T, mode, workload string, concurrency, population, operations int) groupDevMetric {
	t.Helper()
	path := filepath.Join(t.TempDir(), fmt.Sprintf("%s-%s-c%d", mode, workload, concurrency))
	db, err := OpenCatalog(context.Background(), path, KeyedOptions{MaxRecords: population*4 + operations})
	if err != nil {
		t.Fatal(err)
	}
	bucket, err := db.Bucket(context.Background(), "dev")
	if err != nil {
		t.Fatal(err)
	}
	dataset, err := bucket.Dataset(context.Background(), "records", DatasetOptions{TypeIdentity: "group-dev/v1", MaxRecords: population*4 + operations})
	if err != nil {
		t.Fatal(err)
	}
	db.keyed.commit.disabled = mode == "baseline"

	for id := 0; id < population; id++ {
		_, err := db.Mutate(context.Background(), KeyedCommand{ID: fmt.Sprintf("seed-%d", id)}, func(tx *Tx) (any, error) {
			return nil, tx.Put(dataset, fmt.Sprintf("%06d", id), groupDevRecord{Value: 1000, Stock: 100, State: "ready"})
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	walBefore, _ := os.Stat(filepath.Join(path, "wal"))
	for field := range db.keyed.commitStats.groupSizes {
		db.keyed.commitStats.groupSizes[field].Store(0)
	}
	db.keyed.commitStats.syncCalls.Store(0)
	db.keyed.commitStats.framesSynced.Store(0)
	db.keyed.commitStats.groups.Store(0)
	db.keyed.commitStats.maxGroup.Store(0)

	latencies := make([]int64, operations)
	var next atomic.Int64
	var memBefore, memAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&memBefore)
	begin := time.Now()
	var workers sync.WaitGroup
	workers.Add(concurrency)
	for worker := 0; worker < concurrency; worker++ {
		go func() {
			defer workers.Done()
			for {
				operation := int(next.Add(1) - 1)
				if operation >= operations {
					return
				}
				start := time.Now()
				err := runGroupDevOperation(db, dataset, workload, operation, population)
				latencies[operation] = time.Since(start).Nanoseconds()
				if err != nil {
					t.Errorf("%s %s c%d op %d: %v", mode, workload, concurrency, operation, err)
					return
				}
			}
		}()
	}
	workers.Wait()
	elapsed := time.Since(begin)
	runtime.ReadMemStats(&memAfter)
	walAfter, _ := os.Stat(filepath.Join(path, "wal"))
	digest := groupDevDigest(t, dataset, population, workload, operations)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	syncCalls := db.keyed.commitStats.syncCalls.Load()
	frames := db.keyed.commitStats.framesSynced.Load()
	min, median, p95, max := groupDevGroupQuantiles(&db.keyed.commitStats)
	metric := groupDevMetric{Workload: workload, Mode: mode, Concurrency: concurrency, Ops: operations, OpsPerSecond: float64(operations) / elapsed.Seconds(), P50NS: percentileNS(latencies, .50), P95NS: percentileNS(latencies, .95), P99NS: percentileNS(latencies, .99), SyncCalls: syncCalls, GroupMin: min, GroupMedian: median, GroupP95: p95, GroupMax: max, CorrectnessDigest: digest}
	metric.AllocsPerOp = float64(memAfter.Mallocs-memBefore.Mallocs) / float64(operations)
	metric.BytesPerOp = float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / float64(operations)
	metric.WALBytesPerOp = float64(walAfter.Size()-walBefore.Size()) / float64(operations)
	if syncCalls != 0 {
		metric.CommandsPerSync = float64(frames) / float64(syncCalls)
	}
	return metric
}

func runGroupDevOperation(db *Database, dataset *Dataset, workload string, operation, population int) error {
	id := operation % population
	commandID := fmt.Sprintf("%s-%d", workload, operation)
	if workload == "D4-webhook" {
		commandID = fmt.Sprintf("%s-%d", workload, operation/2)
	}
	_, err := db.Mutate(context.Background(), KeyedCommand{ID: commandID}, func(tx *Tx) (any, error) {
		key := fmt.Sprintf("%06d", id)
		var record groupDevRecord
		found, err := tx.Get(dataset, key, &record)
		if err != nil || !found {
			return nil, fmt.Errorf("read %s: found=%v err=%w", key, found, err)
		}
		switch workload {
		case "D1-transfer":
			otherKey := fmt.Sprintf("%06d", (id+1)%population)
			var other groupDevRecord
			if _, err := tx.Get(dataset, otherKey, &other); err != nil {
				return nil, err
			}
			record.Value--
			other.Value++
			if err := tx.Put(dataset, key, record); err != nil {
				return nil, err
			}
			return record.Value, tx.Put(dataset, otherKey, other)
		case "D2-inventory":
			record.Stock--
			return record.Stock, tx.Put(dataset, key, record)
		case "D3-job":
			if record.State == "ready" {
				record.State = "claimed"
			} else {
				record.State = "ready"
			}
			return record.State, tx.Put(dataset, key, record)
		case "D4-webhook":
			record.Processed = true
			return record, tx.Put(dataset, key, record)
		default:
			return nil, fmt.Errorf("unknown workload %q", workload)
		}
	})
	return err
}

func groupDevDigest(t *testing.T, dataset *Dataset, population int, workload string, operations int) string {
	t.Helper()
	hash := sha256.New()
	for id := 0; id < population; id++ {
		var record groupDevRecord
		found, err := dataset.Get(context.Background(), fmt.Sprintf("%06d", id), &record)
		if err != nil || !found {
			t.Fatalf("digest record %d: found=%v err=%v", id, found, err)
		}
		encoded, _ := json.Marshal(record)
		hash.Write(encoded)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func percentileNS(sorted []int64, quantile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1)*quantile + .5)
	return sorted[index]
}

func groupDevGroupQuantiles(stats *keyedCommitStats) (min, median, p95, max int) {
	total := stats.groups.Load()
	if total == 0 {
		return 0, 0, 0, 0
	}
	target50, target95 := (total+1)/2, (total*95+99)/100
	var seen uint64
	for size := 1; size <= keyedMaxCommitGroup; size++ {
		count := stats.groupSizes[size].Load()
		if count == 0 {
			continue
		}
		if min == 0 {
			min = size
		}
		seen += count
		max = size
		if median == 0 && seen >= target50 {
			median = size
		}
		if p95 == 0 && seen >= target95 {
			p95 = size
		}
	}
	return
}
