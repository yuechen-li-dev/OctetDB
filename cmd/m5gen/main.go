package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/yuechen-li-dev/octetdb/internal/m5"
)

func main() {
	rowsFlag := flag.Int("rows", m5.DefaultRowCount, "catalog rows")
	rootFlag := flag.String("root", "experiments/M5", "M5 experiment root")
	flag.Parse()
	if *rowsFlag != m5.DefaultRowCount {
		panic("the retained M5 snapshot identity is fixed at 10,000 rows")
	}
	rows := m5.GenerateCatalog(*rowsFlag)
	identity := m5.SnapshotIdentity(rows)
	if err := os.MkdirAll(filepath.Join(*rootFlag, "data"), 0o755); err != nil {
		panic(err)
	}
	if err := os.MkdirAll(filepath.Join(*rootFlag, "generated"), 0o755); err != nil {
		panic(err)
	}
	if err := m5.WriteJSON(filepath.Join(*rootFlag, "data", "catalog.json"), rows); err != nil {
		panic(err)
	}
	if err := m5.WriteBinary(filepath.Join(*rootFlag, "data", "catalog.bin"), rows); err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(*rootFlag, "data", "catalog.octagon"), []byte(renderOctagon(rows)), 0o644); err != nil {
		panic(err)
	}
	source := renderOct(rows, identity)
	if err := os.WriteFile(filepath.Join(*rootFlag, "generated", "snapshot.octest"), []byte(source), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("rows=%d hash=%s oct_bytes=%d\n", len(rows), identity.SnapshotHash, len(source))
}

type indexes struct {
	categoryRows, priceRows []int
	categoryOffsets         []int
	activeIDs, activePrices []int
	activeOffsets           []int
}

func makeIndexes(rows []m5.Product) indexes {
	var x indexes
	for category := 0; category < m5.CategoryCount; category++ {
		x.categoryOffsets = append(x.categoryOffsets, len(x.categoryRows))
		for idx, row := range rows {
			if int(row.Category) == category {
				x.categoryRows = append(x.categoryRows, idx)
			}
		}
	}
	x.categoryOffsets = append(x.categoryOffsets, len(x.categoryRows))
	x.priceRows = make([]int, len(rows))
	for i := range rows {
		x.priceRows[i] = i
	}
	sort.Slice(x.priceRows, func(i, j int) bool {
		left, right := rows[x.priceRows[i]], rows[x.priceRows[j]]
		if left.PriceCents == right.PriceCents {
			return left.ID < right.ID
		}
		return left.PriceCents < right.PriceCents
	})
	for region := 0; region < m5.RegionCount; region++ {
		x.activeOffsets = append(x.activeOffsets, len(x.activeIDs))
		for _, row := range rows {
			if row.Status == m5.StatusActive && int(row.Region) == region {
				x.activeIDs = append(x.activeIDs, int(row.ID))
				x.activePrices = append(x.activePrices, int(row.PriceCents))
			}
		}
	}
	x.activeOffsets = append(x.activeOffsets, len(x.activeIDs))
	return x
}

func renderOct(rows []m5.Product, identity m5.Identity) string {
	idx := makeIndexes(rows)
	var b strings.Builder
	b.Grow(len(rows) * 180)
	b.WriteString("package DBSchedulerM5\n\n")
	b.WriteString("enum Category { C0 C1 core C3 C4 C5 C6 C7 }\nenum Status { Draft Active Retired }\nenum Region { R0 R1 R2 R3 R4 R5 }\n\n")
	b.WriteString("concept PositivePrice = Int { Require(Self > 0, \"price must be positive\") }\n")
	b.WriteString("record Publication { Version: Int Hash: String }\n")
	b.WriteString("record CatalogData { ID: Int[] Category: Int[] Status: Int[] PriceCents: Int[] RatingTenths: Int[] Region: Int[] Name: String[] }\n")
	b.WriteString("record table Products { ID: Int Category: Int Status: Int PriceCents: Int RatingTenths: Int Region: Int Name: String }\n\n")
	b.WriteString("fn Snapshot() -> Products {\n    let data = LoadOctagon<CatalogData>(\"../Database-Scheduler/experiments/M5/data/catalog.octagon\")!\n    return Products { ID: data.ID Category: data.Category Status: data.Status PriceCents: data.PriceCents RatingTenths: data.RatingTenths Region: data.Region Name: data.Name }\n}\n\n")
	writeArrayFunction(&b, "CategoryRows", idx.categoryRows)
	writeArrayFunction(&b, "CategoryOffsets", idx.categoryOffsets)
	writeArrayFunction(&b, "PriceRows", idx.priceRows)
	writeArrayFunction(&b, "ActiveIDs", idx.activeIDs)
	writeArrayFunction(&b, "ActivePrices", idx.activePrices)
	writeArrayFunction(&b, "ActiveOffsets", idx.activeOffsets)
	b.WriteString("fn CategoryCode(value: Int) -> String { return ToString(value) }\n")
	b.WriteString("fn StatusCode(value: Int) -> String { return ToString(value) }\n")
	b.WriteString("fn RegionCode(value: Int) -> String { return ToString(value) }\n\n")
	b.WriteString("fn PriceInt(value: Int) -> Int { return value }\n\n")
	fmt.Fprintf(&b, "[Fact]\nfn SnapshotAndIndexesAreValid() -> Void {\n    let rows = Snapshot()\n    let categories = CategoryRows()\n    let prices = PriceRows()\n    let activeIDs = ActiveIDs()\n    let activePrices = ActivePrices()\n    let publication1 = Publication { Version: 1 Hash: \"stable\" }\n    let publication2 = publication1 with { Version: 2 }\n    Assert.Equal(1, publication1.Version, \"with preserves the prior immutable publication\")\n    Assert.Equal(2, publication2.Version, \"with creates a deterministic next publication value\")\n    Assert.Equal(publication1.Hash, publication2.Hash, \"with preserves fields not replaced\")\n    Assert.Equal(%d, Len(rows), \"snapshot row count\")\n    Assert.Equal(Len(rows), Len(categories), \"category index covers every row\")\n    Assert.Equal(Len(rows), Len(prices), \"price index covers every row\")\n    Assert.Equal(Len(activeIDs), Len(activePrices), \"projection columns agree\")\n    for i in 0..Len(rows) {\n        Assert.True(rows[i].Category >= 0 and rows[i].Category < 8, \"category code is valid\")\n        Assert.True(rows[i].Status >= 0 and rows[i].Status < 3, \"status code is valid\")\n        Assert.True(rows[i].Region >= 0 and rows[i].Region < 6, \"region code is valid\")\n        Assert.True(rows[i].PriceCents > 0, \"price must be positive\")\n    }\n    for i in 1..Len(rows) { Assert.Equal(rows[i - 1].ID + 1, rows[i].ID, \"IDs are unique and dense\") }\n    for i in 1..Len(prices) { Assert.True(rows[prices[i - 1]].PriceCents <= rows[prices[i]].PriceCents, \"price index is sorted\") }\n}\n\n", len(rows))
	b.WriteString(renderArtifact(identity))
	return b.String()
}

func renderOctagon(rows []m5.Product) string {
	var b strings.Builder
	b.Grow(len(rows) * 75)
	b.WriteString("CatalogData {\n")
	writeIntColumn(&b, "ID", len(rows), func(i int) int { return int(rows[i].ID) })
	writeIntColumn(&b, "Category", len(rows), func(i int) int { return int(rows[i].Category) })
	writeIntColumn(&b, "Status", len(rows), func(i int) int { return int(rows[i].Status) })
	writeIntColumn(&b, "PriceCents", len(rows), func(i int) int { return int(rows[i].PriceCents) })
	writeIntColumn(&b, "RatingTenths", len(rows), func(i int) int { return int(rows[i].RatingTenths) })
	writeIntColumn(&b, "Region", len(rows), func(i int) int { return int(rows[i].Region) })
	b.WriteString("        Name: [")
	for i, row := range rows {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(row.Name))
	}
	b.WriteString("]\n}\n")
	return b.String()
}

func writeIntColumn(b *strings.Builder, name string, n int, value func(int) int) {
	fmt.Fprintf(b, "        %s: [", name)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprint(b, value(i))
	}
	b.WriteString("]\n")
}

func writeEnumColumn(b *strings.Builder, name string, n int, value func(int) string) {
	fmt.Fprintf(b, "        %s: [", name)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(value(i))
	}
	b.WriteString("]\n")
}

func writeArrayFunction(b *strings.Builder, name string, values []int) {
	fmt.Fprintf(b, "fn %s() -> Int[] { return [", name)
	for i, value := range values {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprint(b, value)
	}
	b.WriteString("] }\n")
}

func renderArtifact(identity m5.Identity) string {
	return fmt.Sprintf(`[Artifact]
fn EmitCompiledSnapshot() -> Void {
    let rows = Snapshot()
    let categoryRows = CategoryRows()
    let categoryOffsets = CategoryOffsets()
    let priceRows = PriceRows()
    let activeIDs = ActiveIDs()
    let activePrices = ActivePrices()
    let activeOffsets = ActiveOffsets()
    var lines = [
        "// Code generated from experiments/M5/generated/snapshot.octest by Oct artifact. DO NOT EDIT.",
        "package m5compiled", "", "import m5 \"github.com/yuechen-li-dev/octetdb/internal/m5\"", "",
        "const snapshotHash = \"%s\"", "",
        "var products = [...]m5.Product{"
    ]
    for i in 0..Len(rows) {
        let row = rows[i]
        lines = Append(lines, "{ID:" + ToString(row.ID) + ",Category:" + CategoryCode(row.Category) + ",Status:" + StatusCode(row.Status) + ",PriceCents:" + ToString(PriceInt(row.PriceCents)) + ",RatingTenths:" + ToString(row.RatingTenths) + ",Region:" + RegionCode(row.Region) + ",Name:\"" + row.Name + "\"},")
    }
    lines = Append(lines, "}")
    lines = Append(lines, "var categoryRows = [...]uint32{")
    for i in 0..Len(categoryRows) { lines = Append(lines, ToString(categoryRows[i]) + ",") }
    lines = Append(lines, "}")
    lines = Append(lines, "var categoryOffsets = [...]uint32{")
    for i in 0..Len(categoryOffsets) { lines = Append(lines, ToString(categoryOffsets[i]) + ",") }
    lines = Append(lines, "}")
    lines = Append(lines, "var priceRows = [...]uint32{")
    for i in 0..Len(priceRows) { lines = Append(lines, ToString(priceRows[i]) + ",") }
    lines = Append(lines, "}")
    lines = Append(lines, "var activeProjections = [...]m5.Projection{")
    for i in 0..Len(activeIDs) { lines = Append(lines, "{ID:" + ToString(activeIDs[i]) + ",PriceCents:" + ToString(activePrices[i]) + "},") }
    lines = Append(lines, "}")
    lines = Append(lines, "var activeOffsets = [...]uint32{")
    for i in 0..Len(activeOffsets) { lines = Append(lines, ToString(activeOffsets[i]) + ",") }
    lines = Append(lines, "}")
    lines = Append(lines, "func Identity() m5.Identity { return m5.Identity{SnapshotVersion:m5.SnapshotVersion,SnapshotHash:snapshotHash,SchemaVersion:m5.SchemaVersion,RowCount:len(products)} }")
    lines = Append(lines, "func FindByID(id uint32) (m5.Product,bool) { if id < m5.FirstProductID { return m5.Product{},false }; i:=int(id-m5.FirstProductID); if i<0 || i>=len(products) || products[i].ID!=id { return m5.Product{},false }; return products[i],true }")
    lines = Append(lines, "func FindByCategoryD(c m5.Category) m5.Cursor { return m5.CategoryScanCursor(products[:],c) }")
    lines = Append(lines, "func PriceBetweenD(low,high uint32) m5.Cursor { if high<low { return m5.Cursor{} }; return m5.PriceScanCursor(products[:],low,high) }")
    lines = Append(lines, "func ActiveRegionD(r m5.Region) m5.ProjectionCursor { return m5.ActiveRegionScanCursor(products[:],r) }")
    lines = Append(lines, "func FindByCategoryE(c m5.Category) m5.Cursor { if c>=m5.CategoryCount { return m5.Cursor{} }; a,b:=categoryOffsets[c],categoryOffsets[c+1]; return m5.IDsCursor(products[:],categoryRows[a:b]) }")
    lines = Append(lines, "func PriceBetweenE(low,high uint32) m5.Cursor { if high<low { return m5.Cursor{} }; return m5.SortedPriceCursor(products[:],priceRows[:],low,high) }")
    lines = Append(lines, "func ActiveRegionE(r m5.Region) m5.ProjectionCursor { if r>=m5.RegionCount { return m5.ProjectionCursor{} }; a,b:=activeOffsets[r],activeOffsets[r+1]; return m5.ProjectionsCursor(activeProjections[a:b]) }")
    Artifact.WriteLines("internal/m5compiled/snapshot.generated.go", lines)
}
`, identity.SnapshotHash)
}
