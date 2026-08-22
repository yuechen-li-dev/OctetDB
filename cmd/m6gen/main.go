package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/yuechen-li-dev/database-scheduler/internal/m5"
)

func main() {
	rowsFlag := flag.Int("rows", 10_000, "number of deterministic catalog rows")
	outputFlag := flag.String("output", "experiments/M6/work/10000", "scratch publication input directory")
	rowsOnlyFlag := flag.Bool("rows-only", false, "publish only the nominal table to isolate backend scaling")
	flag.Parse()
	if *rowsFlag <= 0 {
		panic("rows must be positive")
	}
	root, err := filepath.Abs(*outputFlag)
	if err != nil {
		panic(err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		panic(err)
	}
	rows := m5.GenerateCatalog(*rowsFlag)
	priceRows := make([]int, len(rows))
	for i := range priceRows {
		priceRows[i] = i
	}
	sort.Slice(priceRows, func(i, j int) bool {
		a, b := rows[priceRows[i]], rows[priceRows[j]]
		if a.PriceCents != b.PriceCents {
			return a.PriceCents < b.PriceCents
		}
		return priceRows[i] < priceRows[j]
	})
	productsPath := filepath.Join(root, "products.octagon")
	pricePath := filepath.Join(root, "price_index.octagon")
	mustWrite(productsPath, renderProducts(rows))
	mustWrite(pricePath, renderPriceIndex(priceRows))
	if *rowsOnlyFlag {
		mustWrite(filepath.Join(root, "publish.octest"), renderRowsOnlySource(slash(productsPath), len(rows)))
	} else {
		mustWrite(filepath.Join(root, "publish.octest"), renderOctSource(slash(productsPath), slash(pricePath)))
	}
	fmt.Printf("generated nominal M6 inputs: rows=%d root=%s\n", len(rows), root)
}

func renderRowsOnlySource(productsPath string, rows int) []byte {
	return []byte(fmt.Sprintf(`package DBSchedulerM6

enum Category { C0 C1 C2 C3 C4 C5 C6 C7 }
enum Status { Draft Active Retired }
enum Region { R0 R1 R2 R3 R4 R5 }
concept PositivePrice = Int { Require(Self > 0, "price must be positive") }
record table Products { ID: Int Category: Category Status: Status PriceCents: PositivePrice RatingTenths: Int Region: Region Name: String }

[Artifact]
fn PublishRows() -> Void ! Error {
    let table = LoadOctagon<Products>(%q)?
    StaticAssert.Equal(%d, Len(table), "row count")
    Artifact.WriteCompiledData("products.generated.go", "ProductsData", table)
}
`, productsPath, rows))
}

func mustWrite(path string, data []byte) {
	if err := os.WriteFile(path, data, 0o644); err != nil {
		panic(err)
	}
}

func slash(path string) string { return strings.ReplaceAll(path, `\`, "/") }

func renderProducts(rows []m5.Product) []byte {
	var b bytes.Buffer
	b.WriteString("Products {\n")
	writeInts(&b, "ID", len(rows), func(i int) int64 { return int64(rows[i].ID) })
	writeEnums(&b, "Category", len(rows), func(i int) string { return fmt.Sprintf("Category.C%d", rows[i].Category) })
	writeEnums(&b, "Status", len(rows), func(i int) string {
		return []string{"Status.Draft", "Status.Active", "Status.Retired"}[rows[i].Status]
	})
	writeInts(&b, "PriceCents", len(rows), func(i int) int64 { return int64(rows[i].PriceCents) })
	writeInts(&b, "RatingTenths", len(rows), func(i int) int64 { return int64(rows[i].RatingTenths) })
	writeEnums(&b, "Region", len(rows), func(i int) string { return fmt.Sprintf("Region.R%d", rows[i].Region) })
	b.WriteString("    Name: [")
	for i, row := range rows {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%q", row.Name)
	}
	b.WriteString("]\n}\n")
	return b.Bytes()
}

func renderPriceIndex(rows []int) []byte {
	var b bytes.Buffer
	b.WriteString("PriceIndex {\n")
	writeInts(&b, "RowID", len(rows), func(i int) int64 { return int64(rows[i]) })
	b.WriteString("}\n")
	return b.Bytes()
}

func writeInts(b *bytes.Buffer, name string, n int, value func(int) int64) {
	fmt.Fprintf(b, "    %s: [", name)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprint(b, value(i))
	}
	b.WriteString("]\n")
}

func writeEnums(b *bytes.Buffer, name string, n int, value func(int) string) {
	fmt.Fprintf(b, "    %s: [", name)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(value(i))
	}
	b.WriteString("]\n")
}

func renderOctSource(productsPath, pricePath string) []byte {
	source := fmt.Sprintf(`package DBSchedulerM6

enum Category { C0 C1 C2 C3 C4 C5 C6 C7 }
enum Status { Draft Active Retired }
enum Region { R0 R1 R2 R3 R4 R5 }

concept PositivePrice = Int { Require(Self > 0, "price must be positive") }

record table Products {
    ID: Int
    Category: Category
    Status: Status
    PriceCents: PositivePrice
    RatingTenths: Int
    Region: Region
    Name: String
}

record table PriceIndex { RowID: Int }

fn CategoryOf(value: Category) -> Int {
    if value == Category.C0 { return 0 }
    if value == Category.C1 { return 1 }
    if value == Category.C2 { return 2 }
    if value == Category.C3 { return 3 }
    if value == Category.C4 { return 4 }
    if value == Category.C5 { return 5 }
    if value == Category.C6 { return 6 }
    return 7
}

fn RegionOf(value: Region) -> Int {
    if value == Region.R0 { return 0 }
    if value == Region.R1 { return 1 }
    if value == Region.R2 { return 2 }
    if value == Region.R3 { return 3 }
    if value == Region.R4 { return 4 }
    return 5
}

fn DeriveCategoryRows(table: Products) -> Int[] {
    var ids: Int[] = []
    for category in 0..8 {
        for i in 0..Len(table) {
            if CategoryOf(table[i].Category) == category { ids = Append(ids, i) }
        }
    }
    return ids
}

fn DeriveCategoryOffsets(table: Products) -> Int[] {
    var offsets: Int[] = [0]
    var total = 0
    for category in 0..8 {
        for i in 0..Len(table) {
            if CategoryOf(table[i].Category) == category { total = total + 1 }
        }
        offsets = Append(offsets, total)
    }
    return offsets
}

fn DerivePriceRows(index: PriceIndex) -> Int[] {
    var ids: Int[] = []
    for i in 0..Len(index) { ids = Append(ids, index[i].RowID) }
    return ids
}

fn DeriveActiveIDs(table: Products) -> Int[] {
    var ids: Int[] = []
    for region in 0..6 {
        for i in 0..Len(table) {
            let row = table[i]
            if row.Status == Status.Active and RegionOf(row.Region) == region { ids = Append(ids, row.ID) }
        }
    }
    return ids
}

fn PriceInt(value: PositivePrice) -> Int { return value }

fn DeriveActivePrices(table: Products) -> Int[] {
    var prices: Int[] = []
    for region in 0..6 {
        for i in 0..Len(table) {
            let row = table[i]
            if row.Status == Status.Active and RegionOf(row.Region) == region { prices = Append(prices, PriceInt(row.PriceCents)) }
        }
    }
    return prices
}

fn DeriveActiveOffsets(table: Products) -> Int[] {
    var offsets: Int[] = [0]
    var total = 0
    for region in 0..6 {
        for i in 0..Len(table) {
            let row = table[i]
            if row.Status == Status.Active and RegionOf(row.Region) == region { total = total + 1 }
        }
        offsets = Append(offsets, total)
    }
    return offsets
}

fn Validate(table: Products, categories: Int[], categoryOffsets: Int[], prices: Int[], activeIDs: Int[], activePrices: Int[], activeOffsets: Int[]) -> Void {
    StaticAssert.True(Len(table) > 0, "snapshot is non-empty")
    StaticAssert.Equal(Len(table), Len(categories), "category index covers all rows")
    StaticAssert.Equal(Len(table), Len(prices), "price index covers all rows")
    StaticAssert.Equal(Len(activeIDs), Len(activePrices), "projection extents agree")
    StaticAssert.Equal(9, Len(categoryOffsets), "category offset extent")
    StaticAssert.Equal(7, Len(activeOffsets), "active-region offset extent")
    StaticAssert.Equal(0, categoryOffsets[0], "category offsets begin at zero")
    StaticAssert.Equal(Len(table), categoryOffsets[8], "category offsets end at row count")
    StaticAssert.Equal(0, activeOffsets[0], "active offsets begin at zero")
    StaticAssert.Equal(Len(activeIDs), activeOffsets[6], "active offsets end at projection count")
    var priceSum = 0
    var priceSquareSum = 0
    var rowsValid = true
    var categoryRowsValid = true
    var priceRowsValid = true
    for i in 0..Len(table) {
        if table[i].ID != 1000001 + i or table[i].PriceCents <= 0 { rowsValid = false }
        if categories[i] < 0 or categories[i] >= Len(table) { categoryRowsValid = false }
        if prices[i] < 0 or prices[i] >= Len(table) { priceRowsValid = false }
        priceSum = priceSum + prices[i]
        priceSquareSum = priceSquareSum + prices[i] * prices[i]
    }
    StaticAssert.True(rowsValid, "IDs are unique/dense and refined prices remain positive")
    StaticAssert.True(categoryRowsValid, "category row IDs valid")
    StaticAssert.True(priceRowsValid, "price row IDs valid")
    let expectedSum = Len(table) * (Len(table) - 1) / 2
    let expectedSquareSum = Len(table) * (Len(table) - 1) * (2 * Len(table) - 1) / 6
    StaticAssert.Equal(expectedSum, priceSum, "price index coverage sum")
    StaticAssert.Equal(expectedSquareSum, priceSquareSum, "price index coverage square sum")
    var pricesSorted = true
    for i in 1..Len(prices) {
        let previous = table[prices[i - 1]]
        let current = table[prices[i]]
        if previous.PriceCents > current.PriceCents or (previous.PriceCents == current.PriceCents and prices[i - 1] >= prices[i]) { pricesSorted = false }
    }
    StaticAssert.True(pricesSorted, "price index sorted deterministically")
    var categoryOffsetsValid = true
    var activeOffsetsValid = true
    for i in 1..Len(categoryOffsets) { if categoryOffsets[i - 1] > categoryOffsets[i] { categoryOffsetsValid = false } }
    for i in 1..Len(activeOffsets) { if activeOffsets[i - 1] > activeOffsets[i] { activeOffsetsValid = false } }
    StaticAssert.True(categoryOffsetsValid, "category offsets monotonic")
    StaticAssert.True(activeOffsetsValid, "active offsets monotonic")
}

[Artifact]
fn Publish() -> Void ! Error {
    let table = LoadOctagon<Products>(%q)?
    let priceIndex = LoadOctagon<PriceIndex>(%q)?
    let categoryRows = DeriveCategoryRows(table)
    let categoryOffsets = DeriveCategoryOffsets(table)
    let priceRows = DerivePriceRows(priceIndex)
    let activeIDs = DeriveActiveIDs(table)
    let activePrices = DeriveActivePrices(table)
    let activeOffsets = DeriveActiveOffsets(table)
    Validate(table, categoryRows, categoryOffsets, priceRows, activeIDs, activePrices, activeOffsets)
    Artifact.WriteCompiledData("products.generated.go", "ProductsData", table)
    Artifact.WriteCompiledData("category_rows.generated.go", "CategoryRows", categoryRows)
    Artifact.WriteCompiledData("category_offsets.generated.go", "CategoryOffsets", categoryOffsets)
    Artifact.WriteCompiledData("price_rows.generated.go", "PriceRows", priceRows)
    Artifact.WriteCompiledData("active_ids.generated.go", "ActiveIDs", activeIDs)
    Artifact.WriteCompiledData("active_prices.generated.go", "ActivePrices", activePrices)
    Artifact.WriteCompiledData("active_offsets.generated.go", "ActiveOffsets", activeOffsets)
}
`, productsPath, pricePath)
	return []byte(source)
}
