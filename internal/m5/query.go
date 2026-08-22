package m5

type Cursor struct {
	rows     []Product
	ids      []uint32
	pos      int
	end      int
	category Category
	region   Region
	low      uint32
	high     uint32
	mode     uint8
}

const (
	cursorRows uint8 = iota
	cursorIDs
	cursorCategoryScan
	cursorPriceScan
)

func RowsCursor(rows []Product) Cursor { return Cursor{rows: rows, end: len(rows), mode: cursorRows} }
func IDsCursor(rows []Product, ids []uint32) Cursor {
	return Cursor{rows: rows, ids: ids, end: len(ids), mode: cursorIDs}
}
func CategoryScanCursor(rows []Product, category Category) Cursor {
	return Cursor{rows: rows, end: len(rows), category: category, mode: cursorCategoryScan}
}
func PriceScanCursor(rows []Product, low, high uint32) Cursor {
	return Cursor{rows: rows, end: len(rows), low: low, high: high, mode: cursorPriceScan}
}

func (c *Cursor) Next() (Product, bool) {
	for c.pos < c.end {
		pos := c.pos
		c.pos++
		switch c.mode {
		case cursorRows:
			return c.rows[pos], true
		case cursorIDs:
			return c.rows[c.ids[pos]], true
		case cursorCategoryScan:
			if c.rows[pos].Category == c.category {
				return c.rows[pos], true
			}
		case cursorPriceScan:
			row := c.rows[pos]
			if row.PriceCents >= c.low && row.PriceCents <= c.high {
				return row, true
			}
		}
	}
	return Product{}, false
}

type ProjectionCursor struct {
	rows     []Projection
	products []Product
	region   Region
	pos      int
	end      int
	scan     bool
}

func ProjectionsCursor(rows []Projection) ProjectionCursor {
	return ProjectionCursor{rows: rows, end: len(rows)}
}

func ActiveRegionScanCursor(rows []Product, region Region) ProjectionCursor {
	return ProjectionCursor{products: rows, region: region, end: len(rows), scan: true}
}

func (c *ProjectionCursor) Next() (Projection, bool) {
	for c.pos < c.end {
		pos := c.pos
		c.pos++
		if !c.scan {
			return c.rows[pos], true
		}
		row := c.products[pos]
		if row.Status == StatusActive && row.Region == c.region {
			return Projection{ID: row.ID, PriceCents: row.PriceCents}, true
		}
	}
	return Projection{}, false
}

func SortedPriceCursor(rows []Product, ids []uint32, low, high uint32) Cursor {
	start := lowerPrice(rows, ids, low)
	end := len(ids)
	if high != ^uint32(0) {
		end = lowerPrice(rows, ids, high+1)
	}
	return IDsCursor(rows, ids[start:end])
}

func lowerPrice(rows []Product, ids []uint32, target uint32) int {
	lo, hi := 0, len(ids)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if rows[ids[mid]].PriceCents < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

type Lane interface {
	Identity() Identity
	FindByID(uint32) (Product, bool)
	FindByCategory(Category) Cursor
	ProductsWithPriceBetween(uint32, uint32) Cursor
	ActiveProductsInRegion(Region) ProjectionCursor
}

func DigestProducts(cursor Cursor) (int, uint64) {
	var count int
	var digest uint64
	for {
		row, ok := cursor.Next()
		if !ok {
			return count, digest
		}
		count++
		digest += uint64(row.ID)*0x9e3779b185ebca87 + uint64(row.PriceCents)
	}
}

func DigestProjections(cursor ProjectionCursor) (int, uint64) {
	var count int
	var digest uint64
	for {
		row, ok := cursor.Next()
		if !ok {
			return count, digest
		}
		count++
		digest += uint64(row.ID)*0x9e3779b185ebca87 + uint64(row.PriceCents)
	}
}
