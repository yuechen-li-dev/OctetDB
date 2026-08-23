package m5compiled

import (
	"sync"
	"testing"

	"github.com/yuechen-li-dev/octetdb/internal/m5"
)

func TestCompiledLanesMatchCanonicalAndRuntimeControls(t *testing.T) {
	canonical := m5.GenerateCatalog(m5.DefaultRowCount)
	expectedIdentity := m5.SnapshotIdentity(canonical)
	controls := []m5.Lane{m5.NewRuntimeCache(append([]m5.Product(nil), canonical...)), ScanLane{}, IndexedLane{}}
	for laneIndex, lane := range controls {
		if lane.Identity() != expectedIdentity {
			t.Fatalf("lane %d identity mismatch: got %+v want %+v", laneIndex, lane.Identity(), expectedIdentity)
		}
		for _, id := range []uint32{m5.FirstProductID, m5.FirstProductID + 417, m5.FirstProductID + m5.DefaultRowCount - 1} {
			got, ok := lane.FindByID(id)
			want := canonical[id-m5.FirstProductID]
			if !ok || got != want {
				t.Fatalf("lane %d point lookup %d: got %+v,%v want %+v,true", laneIndex, id, got, ok, want)
			}
		}
	}
	for category := m5.Category(0); category < m5.CategoryCount; category++ {
		compareProductQuery(t, controls, func(lane m5.Lane) m5.Cursor { return lane.FindByCategory(category) })
	}
	for _, bounds := range [][2]uint32{{0, 1000}, {50_000, 80_000}, {240_000, 260_000}} {
		compareProductQuery(t, controls, func(lane m5.Lane) m5.Cursor { return lane.ProductsWithPriceBetween(bounds[0], bounds[1]) })
	}
	for region := m5.Region(0); region < m5.RegionCount; region++ {
		compareProjectionQuery(t, controls, func(lane m5.Lane) m5.ProjectionCursor { return lane.ActiveProductsInRegion(region) })
	}
}

func compareProductQuery(t *testing.T, lanes []m5.Lane, query func(m5.Lane) m5.Cursor) {
	t.Helper()
	wantCount, wantDigest := m5.DigestProducts(query(lanes[0]))
	for i := 1; i < len(lanes); i++ {
		if count, digest := m5.DigestProducts(query(lanes[i])); count != wantCount || digest != wantDigest {
			t.Fatalf("lane %d product result mismatch: got %d/%x want %d/%x", i, count, digest, wantCount, wantDigest)
		}
	}
}

func compareProjectionQuery(t *testing.T, lanes []m5.Lane, query func(m5.Lane) m5.ProjectionCursor) {
	t.Helper()
	wantCount, wantDigest := m5.DigestProjections(query(lanes[0]))
	for i := 1; i < len(lanes); i++ {
		if count, digest := m5.DigestProjections(query(lanes[i])); count != wantCount || digest != wantDigest {
			t.Fatalf("lane %d projection result mismatch: got %d/%x want %d/%x", i, count, digest, wantCount, wantDigest)
		}
	}
}

func TestConcurrentReadsAreDeterministic(t *testing.T) {
	lane := IndexedLane{}
	wantCategoryCount, wantCategoryDigest := m5.DigestProducts(lane.FindByCategory(3))
	wantProjectionCount, wantProjectionDigest := m5.DigestProjections(lane.ActiveProductsInRegion(2))
	const workers, iterations = 8, 200
	var wg sync.WaitGroup
	errors := make(chan string, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				if count, digest := m5.DigestProducts(lane.FindByCategory(3)); count != wantCategoryCount || digest != wantCategoryDigest {
					errors <- "category result changed"
					return
				}
				if count, digest := m5.DigestProjections(lane.ActiveProductsInRegion(2)); count != wantProjectionCount || digest != wantProjectionDigest {
					errors <- "projection result changed"
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for problem := range errors {
		t.Fatal(problem)
	}
	if lane.Identity() != Identity() {
		t.Fatal("snapshot identity changed during concurrent reads")
	}
}

var productSink m5.Product
var countSink int
var digestSink uint64

func BenchmarkQueries(b *testing.B) {
	lanes := []struct {
		name string
		lane m5.Lane
	}{{"D_scan", ScanLane{}}, {"E_indexed", IndexedLane{}}}
	for _, item := range lanes {
		b.Run(item.name+"/Q1_point", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				productSink, _ = item.lane.FindByID(m5.FirstProductID + uint32(i%m5.DefaultRowCount))
			}
		})
		b.Run(item.name+"/Q2_category", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				countSink, digestSink = m5.DigestProducts(item.lane.FindByCategory(3))
			}
		})
		b.Run(item.name+"/Q3_price", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				countSink, digestSink = m5.DigestProducts(item.lane.ProductsWithPriceBetween(50_000, 80_000))
			}
		})
		b.Run(item.name+"/Q4_projection", func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				countSink, digestSink = m5.DigestProjections(item.lane.ActiveProductsInRegion(2))
			}
		})
	}
}
