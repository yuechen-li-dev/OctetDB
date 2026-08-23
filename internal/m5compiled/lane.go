package m5compiled

import "github.com/yuechen-li-dev/octetdb/internal/m5"

type ScanLane struct{}

func (ScanLane) Identity() m5.Identity                               { return Identity() }
func (ScanLane) FindByID(id uint32) (m5.Product, bool)               { return FindByID(id) }
func (ScanLane) FindByCategory(category m5.Category) m5.Cursor       { return FindByCategoryD(category) }
func (ScanLane) ProductsWithPriceBetween(low, high uint32) m5.Cursor { return PriceBetweenD(low, high) }
func (ScanLane) ActiveProductsInRegion(region m5.Region) m5.ProjectionCursor {
	return ActiveRegionD(region)
}

type IndexedLane struct{}

func (IndexedLane) Identity() m5.Identity                         { return Identity() }
func (IndexedLane) FindByID(id uint32) (m5.Product, bool)         { return FindByID(id) }
func (IndexedLane) FindByCategory(category m5.Category) m5.Cursor { return FindByCategoryE(category) }
func (IndexedLane) ProductsWithPriceBetween(low, high uint32) m5.Cursor {
	return PriceBetweenE(low, high)
}
func (IndexedLane) ActiveProductsInRegion(region m5.Region) m5.ProjectionCursor {
	return ActiveRegionE(region)
}
