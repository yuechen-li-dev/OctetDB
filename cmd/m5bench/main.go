package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yuechen-li-dev/database-scheduler/internal/m5"
	"github.com/yuechen-li-dev/database-scheduler/internal/m5compiled"
)

type startupResult struct {
	Lane           string  `json:"lane"`
	ReadNS         int64   `json:"read_ns,omitempty"`
	DecodeNS       int64   `json:"decode_ns,omitempty"`
	IndexNS        int64   `json:"index_ns,omitempty"`
	FirstQueryNS   int64   `json:"first_query_ns"`
	TotalNS        int64   `json:"total_ns"`
	Allocations    float64 `json:"allocations"`
	AllocatedBytes uint64  `json:"allocated_bytes"`
	LiveHeapDelta  int64   `json:"live_heap_delta_bytes"`
}

type queryResult struct {
	Lane        string  `json:"lane"`
	Query       string  `json:"query"`
	NSPerOp     float64 `json:"ns_per_op"`
	Allocations float64 `json:"allocations_per_op"`
	Bytes       uint64  `json:"bytes_per_op"`
	Iterations  int     `json:"iterations"`
}

type parallelResult struct {
	Workers      int     `json:"workers"`
	OpsPerSecond float64 `json:"ops_per_second"`
}

type summary struct {
	GeneratedAt         string           `json:"generated_at"`
	GoVersion           string           `json:"go_version"`
	Rows                int              `json:"rows"`
	Identity            m5.Identity      `json:"identity"`
	Startup             []startupResult  `json:"startup"`
	Queries             []queryResult    `json:"queries"`
	ParallelE           []parallelResult `json:"parallel_e"`
	PostgreSQLAvailable bool             `json:"postgresql_available"`
	PostgreSQLVersion   string           `json:"postgresql_version,omitempty"`
	Correctness         string           `json:"correctness"`
}

var retained any
var productSink m5.Product
var countSink int
var digestSink uint64

func main() {
	dataRoot := flag.String("data", "experiments/M5/data", "snapshot data directory")
	output := flag.String("output", "experiments/M5/summary.json", "summary output")
	dsn := flag.String("dsn", "postgres://dbsched:dbsched@localhost:54329/dbsched?sslmode=disable", "PostgreSQL DSN")
	flag.Parse()
	ctx := context.Background()
	canonical := m5.GenerateCatalog(m5.DefaultRowCount)
	wantIdentity := m5.SnapshotIdentity(canonical)
	out := summary{GeneratedAt: time.Now().UTC().Format(time.RFC3339), GoVersion: runtime.Version(), Rows: len(canonical), Identity: wantIdentity, Correctness: "pending"}

	jsonPath := filepath.Join(*dataRoot, "catalog.json")
	binPath := filepath.Join(*dataRoot, "catalog.bin")
	jsonCache, jsonStartup := loadJSON(jsonPath)
	binCache, binaryStartup := loadBinary(binPath)
	out.Startup = append(out.Startup, jsonStartup, binaryStartup, compiledStartup("D", m5compiled.ScanLane{}), compiledStartup("E", m5compiled.IndexedLane{}))
	lanes := []struct {
		name string
		lane m5.Lane
	}{{"B_json", jsonCache}, {"C_binary", binCache}, {"D_compiled_scan", m5compiled.ScanLane{}}, {"E_compiled_indexed", m5compiled.IndexedLane{}}}
	for _, lane := range lanes {
		mustEquivalent(wantIdentity, canonical, lane.name, lane.lane)
		out.Queries = append(out.Queries, benchmarkMemoryLane(lane.name, lane.lane)...)
	}
	out.ParallelE = benchmarkParallelE(m5compiled.IndexedLane{})

	pool, err := pgxpool.New(ctx, *dsn)
	if err == nil {
		err = pool.Ping(ctx)
	}
	if err == nil {
		defer pool.Close()
		out.PostgreSQLAvailable = true
		if e := pool.QueryRow(ctx, "select version()").Scan(&out.PostgreSQLVersion); e != nil {
			panic(e)
		}
		pgPublication := loadPostgres(ctx, pool, canonical)
		pgStartup := postgresReadyStartup(ctx, pool)
		out.Startup = append([]startupResult{pgPublication, pgStartup}, out.Startup...)
		out.Queries = append(benchmarkPostgres(ctx, pool), out.Queries...)
		verifyPostgres(ctx, pool, canonical)
	} else {
		fmt.Fprintf(os.Stderr, "PostgreSQL lane unavailable: %v\n", err)
	}
	out.Correctness = "all available lanes match canonical logical results and identity"
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0o755); err != nil {
		panic(err)
	}
	if err := os.WriteFile(*output, append(data, '\n'), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("wrote %s (%d query measurements)\n", *output, len(out.Queries))
}

func loadJSON(path string) (*m5.RuntimeCache, startupResult) {
	start, allocStart := time.Now(), memstats()
	readStart := time.Now()
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	readNS := time.Since(readStart).Nanoseconds()
	decodeStart := time.Now()
	var rows []m5.Product
	if err := json.Unmarshal(data, &rows); err != nil {
		panic(err)
	}
	decodeNS := time.Since(decodeStart).Nanoseconds()
	indexStart := time.Now()
	cache := m5.NewRuntimeCache(rows)
	indexNS := time.Since(indexStart).Nanoseconds()
	queryStart := time.Now()
	_, ok := cache.FindByID(m5.FirstProductID + 417)
	if !ok {
		panic("JSON first query failed")
	}
	firstNS := time.Since(queryStart).Nanoseconds()
	retained = cache
	end := memstats()
	totalNS := time.Since(start).Nanoseconds()
	if readNS == 0 {
		readNS = phaseNS(20, func() { retained, _ = os.ReadFile(path) })
	}
	if firstNS == 0 {
		firstNS = phaseNS(1_000_000, func() { productSink, _ = cache.FindByID(m5.FirstProductID + 417) })
	}
	return cache, startupResult{Lane: "B_json", ReadNS: readNS, DecodeNS: decodeNS, IndexNS: indexNS, FirstQueryNS: firstNS, TotalNS: totalNS, Allocations: float64(end.Mallocs - allocStart.Mallocs), AllocatedBytes: end.TotalAlloc - allocStart.TotalAlloc, LiveHeapDelta: int64(end.HeapAlloc) - int64(allocStart.HeapAlloc)}
}

func loadBinary(path string) (*m5.RuntimeCache, startupResult) {
	start, allocStart := time.Now(), memstats()
	readStart := time.Now()
	data, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	readNS := time.Since(readStart).Nanoseconds()
	decodeStart := time.Now()
	rows, err := m5.DecodeBinary(data)
	if err != nil {
		panic(err)
	}
	decodeNS := time.Since(decodeStart).Nanoseconds()
	indexStart := time.Now()
	cache := m5.NewRuntimeCache(rows)
	indexNS := time.Since(indexStart).Nanoseconds()
	queryStart := time.Now()
	_, ok := cache.FindByID(m5.FirstProductID + 417)
	if !ok {
		panic("binary first query failed")
	}
	firstNS := time.Since(queryStart).Nanoseconds()
	retained = cache
	end := memstats()
	totalNS := time.Since(start).Nanoseconds()
	if readNS == 0 {
		readNS = phaseNS(20, func() { retained, _ = os.ReadFile(path) })
	}
	if firstNS == 0 {
		firstNS = phaseNS(1_000_000, func() { productSink, _ = cache.FindByID(m5.FirstProductID + 417) })
	}
	return cache, startupResult{Lane: "C_binary", ReadNS: readNS, DecodeNS: decodeNS, IndexNS: indexNS, FirstQueryNS: firstNS, TotalNS: totalNS, Allocations: float64(end.Mallocs - allocStart.Mallocs), AllocatedBytes: end.TotalAlloc - allocStart.TotalAlloc, LiveHeapDelta: int64(end.HeapAlloc) - int64(allocStart.HeapAlloc)}
}

func compiledStartup(name string, lane m5.Lane) startupResult {
	before := memstats()
	queryStart := time.Now()
	_, ok := lane.FindByID(m5.FirstProductID + 417)
	if !ok {
		panic("compiled first query failed")
	}
	first := time.Since(queryStart)
	after := memstats()
	firstNS := first.Nanoseconds()
	if firstNS == 0 {
		firstNS = phaseNS(1_000_000, func() { productSink, _ = lane.FindByID(m5.FirstProductID + 417) })
	}
	return startupResult{Lane: name + "_compiled", FirstQueryNS: firstNS, TotalNS: firstNS, Allocations: float64(after.Mallocs - before.Mallocs), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, LiveHeapDelta: int64(after.HeapAlloc) - int64(before.HeapAlloc)}
}

func phaseNS(iterations int, fn func()) int64 {
	start := time.Now()
	for i := 0; i < iterations; i++ {
		fn()
	}
	return time.Since(start).Nanoseconds() / int64(iterations)
}

func benchmarkMemoryLane(name string, lane m5.Lane) []queryResult {
	return []queryResult{
		measure(name, "Q1_point", 100_000, func() { productSink, _ = lane.FindByID(m5.FirstProductID + 417) }),
		measure(name, "Q2_category", 2_000, func() { countSink, digestSink = m5.DigestProducts(lane.FindByCategory(3)) }),
		measure(name, "Q3_price", 2_000, func() { countSink, digestSink = m5.DigestProducts(lane.ProductsWithPriceBetween(50_000, 80_000)) }),
		measure(name, "Q4_projection", 5_000, func() { countSink, digestSink = m5.DigestProjections(lane.ActiveProductsInRegion(2)) }),
	}
}

func measure(lane, query string, iterations int, fn func()) queryResult {
	allocs := testing.AllocsPerRun(100, fn)
	before := memstats()
	start := time.Now()
	for i := 0; i < iterations; i++ {
		fn()
	}
	elapsed := time.Since(start)
	nsPerOp := float64(elapsed.Nanoseconds()) / float64(iterations)
	if elapsed.Nanoseconds() == 0 {
		nsPerOp = float64(phaseNS(10_000_000, fn))
	}
	after := memstats()
	return queryResult{Lane: lane, Query: query, NSPerOp: nsPerOp, Allocations: allocs, Bytes: (after.TotalAlloc - before.TotalAlloc) / uint64(iterations), Iterations: iterations}
}

func benchmarkParallelE(lane m5.Lane) []parallelResult {
	const operations = 100_000
	var results []parallelResult
	for _, workers := range []int{1, 2, 4, 8} {
		start := time.Now()
		var wg sync.WaitGroup
		for worker := 0; worker < workers; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				var product m5.Product
				for i := worker; i < operations; i += workers {
					switch i & 3 {
					case 0:
						product, _ = lane.FindByID(m5.FirstProductID + uint32(i%m5.DefaultRowCount))
					case 1:
						_, _ = m5.DigestProducts(lane.FindByCategory(m5.Category(i % m5.CategoryCount)))
					case 2:
						_, _ = m5.DigestProducts(lane.ProductsWithPriceBetween(50_000, 50_500))
					case 3:
						_, _ = m5.DigestProjections(lane.ActiveProductsInRegion(m5.Region(i % m5.RegionCount)))
					}
				}
				runtime.KeepAlive(product)
			}(worker)
		}
		wg.Wait()
		results = append(results, parallelResult{Workers: workers, OpsPerSecond: float64(operations) / time.Since(start).Seconds()})
	}
	return results
}

func loadPostgres(ctx context.Context, pool *pgxpool.Pool, rows []m5.Product) startupResult {
	start, before := time.Now(), memstats()
	if _, err := pool.Exec(ctx, "drop table if exists m5_products; create table m5_products(id bigint primary key, category smallint not null, status smallint not null, price_cents integer not null, rating_tenths smallint not null, region smallint not null, name text not null)"); err != nil {
		panic(err)
	}
	values := make([][]any, len(rows))
	for i, row := range rows {
		values[i] = []any{int64(row.ID), int16(row.Category), int16(row.Status), int32(row.PriceCents), int16(row.RatingTenths), int16(row.Region), row.Name}
	}
	if _, err := pool.CopyFrom(ctx, pgx.Identifier{"m5_products"}, []string{"id", "category", "status", "price_cents", "rating_tenths", "region", "name"}, pgx.CopyFromRows(values)); err != nil {
		panic(err)
	}
	if _, err := pool.Exec(ctx, "create index m5_products_category on m5_products(category); create index m5_products_price on m5_products(price_cents,id); create index m5_products_active_region on m5_products(region,id) where status=1; analyze m5_products"); err != nil {
		panic(err)
	}
	after := memstats()
	total := time.Since(start).Nanoseconds()
	return startupResult{Lane: "A_postgresql_publication", IndexNS: total, TotalNS: total, Allocations: float64(after.Mallocs - before.Mallocs), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, LiveHeapDelta: int64(after.HeapAlloc) - int64(before.HeapAlloc)}
}

func postgresReadyStartup(ctx context.Context, pool *pgxpool.Pool) startupResult {
	before := memstats()
	start := time.Now()
	var id int64
	if err := pool.QueryRow(ctx, "select id from m5_products where id=$1", int64(m5.FirstProductID+417)).Scan(&id); err != nil {
		panic(err)
	}
	elapsed := time.Since(start).Nanoseconds()
	after := memstats()
	return startupResult{Lane: "A_postgresql_ready_pool", FirstQueryNS: elapsed, TotalNS: elapsed, Allocations: float64(after.Mallocs - before.Mallocs), AllocatedBytes: after.TotalAlloc - before.TotalAlloc, LiveHeapDelta: int64(after.HeapAlloc) - int64(before.HeapAlloc)}
}

func benchmarkPostgres(ctx context.Context, pool *pgxpool.Pool) []queryResult {
	return []queryResult{
		measure("A_postgresql", "Q1_point", 1000, func() {
			var id int64
			var name string
			if err := pool.QueryRow(ctx, "select id,name from m5_products where id=$1", int64(m5.FirstProductID+417)).Scan(&id, &name); err != nil {
				panic(err)
			}
			retained = name
		}),
		measure("A_postgresql", "Q2_category", 100, func() {
			retained = pgProductDigest(ctx, pool, "select id,price_cents from m5_products where category=$1 order by id", int16(3))
		}),
		measure("A_postgresql", "Q3_price", 100, func() {
			retained = pgProductDigest(ctx, pool, "select id,price_cents from m5_products where price_cents between $1 and $2 order by price_cents,id", int32(50_000), int32(80_000))
		}),
		measure("A_postgresql", "Q4_projection", 200, func() {
			retained = pgProductDigest(ctx, pool, "select id,price_cents from m5_products where status=1 and region=$1 order by id", int16(2))
		}),
	}
}

func pgProductDigest(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) [2]uint64 {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	var count, digest uint64
	for rows.Next() {
		var id int64
		var price int32
		if err := rows.Scan(&id, &price); err != nil {
			panic(err)
		}
		count++
		digest += uint64(id)*0x9e3779b185ebca87 + uint64(price)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
	return [2]uint64{count, digest}
}

func verifyPostgres(ctx context.Context, pool *pgxpool.Pool, canonical []m5.Product) {
	cache := m5.NewRuntimeCache(canonical)
	for _, id := range []uint32{m5.FirstProductID, m5.FirstProductID + 417, m5.FirstProductID + m5.DefaultRowCount - 1} {
		var got m5.Product
		var category, status, rating, region int16
		if err := pool.QueryRow(ctx, "select id,category,status,price_cents,rating_tenths,region,name from m5_products where id=$1", int64(id)).Scan(&got.ID, &category, &status, &got.PriceCents, &rating, &region, &got.Name); err != nil {
			panic(err)
		}
		got.Category, got.Status, got.RatingTenths, got.Region = m5.Category(category), m5.Status(status), uint8(rating), m5.Region(region)
		want, _ := cache.FindByID(id)
		if got != want {
			panic("PostgreSQL point mismatch")
		}
	}
	for category := m5.Category(0); category < m5.CategoryCount; category++ {
		count, digest := m5.DigestProducts(cache.FindByCategory(category))
		got := pgProductDigest(ctx, pool, "select id,price_cents from m5_products where category=$1", int16(category))
		if got != [2]uint64{uint64(count), digest} {
			panic("PostgreSQL category mismatch")
		}
	}
	count, digest := m5.DigestProducts(cache.ProductsWithPriceBetween(50_000, 80_000))
	if got := pgProductDigest(ctx, pool, "select id,price_cents from m5_products where price_cents between $1 and $2", int32(50_000), int32(80_000)); got != [2]uint64{uint64(count), digest} {
		panic("PostgreSQL price mismatch")
	}
	for region := m5.Region(0); region < m5.RegionCount; region++ {
		projectionCount, projectionDigest := m5.DigestProjections(cache.ActiveProductsInRegion(region))
		if got := pgProductDigest(ctx, pool, "select id,price_cents from m5_products where status=1 and region=$1", int16(region)); got != [2]uint64{uint64(projectionCount), projectionDigest} {
			panic("PostgreSQL projection mismatch")
		}
	}
}

func mustEquivalent(identity m5.Identity, canonical []m5.Product, name string, lane m5.Lane) {
	if lane.Identity() != identity {
		panic(name + " identity mismatch")
	}
	for i, row := range canonical {
		got, ok := lane.FindByID(m5.FirstProductID + uint32(i))
		if !ok || got != row {
			panic(name + " point mismatch")
		}
	}
}

func memstats() runtime.MemStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats
}
