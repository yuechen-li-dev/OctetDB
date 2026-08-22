package m5

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)

var binaryMagic = [8]byte{'D', 'B', 'S', 'M', '5', 'B', 'I', 'N'}

type RuntimeCache struct {
	identity            Identity
	rows                []Product
	categoryRows        []uint32
	categoryOffsets     [CategoryCount + 1]int
	priceRows           []uint32
	activeRegionRows    []Projection
	activeRegionOffsets [RegionCount + 1]int
}

func NewRuntimeCache(rows []Product) *RuntimeCache {
	c := &RuntimeCache{identity: SnapshotIdentity(rows), rows: rows}
	for category := 0; category < CategoryCount; category++ {
		c.categoryOffsets[category] = len(c.categoryRows)
		for idx, row := range rows {
			if int(row.Category) == category {
				c.categoryRows = append(c.categoryRows, uint32(idx))
			}
		}
	}
	c.categoryOffsets[CategoryCount] = len(c.categoryRows)
	c.priceRows = make([]uint32, len(rows))
	for i := range rows {
		c.priceRows[i] = uint32(i)
	}
	sort.Slice(c.priceRows, func(i, j int) bool {
		left, right := rows[c.priceRows[i]], rows[c.priceRows[j]]
		if left.PriceCents == right.PriceCents {
			return left.ID < right.ID
		}
		return left.PriceCents < right.PriceCents
	})
	for region := 0; region < RegionCount; region++ {
		c.activeRegionOffsets[region] = len(c.activeRegionRows)
		for _, row := range rows {
			if row.Status == StatusActive && int(row.Region) == region {
				c.activeRegionRows = append(c.activeRegionRows, Projection{ID: row.ID, PriceCents: row.PriceCents})
			}
		}
	}
	c.activeRegionOffsets[RegionCount] = len(c.activeRegionRows)
	return c
}

func (c *RuntimeCache) Identity() Identity { return c.identity }

func (c *RuntimeCache) FindByID(id uint32) (Product, bool) {
	if id < FirstProductID {
		return Product{}, false
	}
	idx := int(id - FirstProductID)
	if idx < 0 || idx >= len(c.rows) || c.rows[idx].ID != id {
		return Product{}, false
	}
	return c.rows[idx], true
}

func (c *RuntimeCache) FindByCategory(category Category) Cursor {
	if category >= CategoryCount {
		return Cursor{}
	}
	start, end := c.categoryOffsets[category], c.categoryOffsets[category+1]
	return IDsCursor(c.rows, c.categoryRows[start:end])
}

func (c *RuntimeCache) ProductsWithPriceBetween(low, high uint32) Cursor {
	if high < low {
		return Cursor{}
	}
	return SortedPriceCursor(c.rows, c.priceRows, low, high)
}

func (c *RuntimeCache) ActiveProductsInRegion(region Region) ProjectionCursor {
	if region >= RegionCount {
		return ProjectionCursor{}
	}
	start, end := c.activeRegionOffsets[region], c.activeRegionOffsets[region+1]
	return ProjectionsCursor(c.activeRegionRows[start:end])
}

func WriteJSON(path string, rows []Product) error {
	data, err := json.Marshal(rows)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

func ReadJSON(path string) ([]Product, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rows []Product
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func WriteBinary(path string, rows []Product) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	if _, err = w.Write(binaryMagic[:]); err != nil {
		return err
	}
	if err = binary.Write(w, binary.LittleEndian, uint32(len(rows))); err != nil {
		return err
	}
	for _, row := range rows {
		if err = binary.Write(w, binary.LittleEndian, row.ID); err != nil {
			return err
		}
		if err = w.WriteByte(byte(row.Category)); err != nil {
			return err
		}
		if err = w.WriteByte(byte(row.Status)); err != nil {
			return err
		}
		if err = binary.Write(w, binary.LittleEndian, row.PriceCents); err != nil {
			return err
		}
		if err = w.WriteByte(row.RatingTenths); err != nil {
			return err
		}
		if err = w.WriteByte(byte(row.Region)); err != nil {
			return err
		}
		if len(row.Name) > 65535 {
			return fmt.Errorf("name too long")
		}
		if err = binary.Write(w, binary.LittleEndian, uint16(len(row.Name))); err != nil {
			return err
		}
		if _, err = w.WriteString(row.Name); err != nil {
			return err
		}
	}
	return w.Flush()
}

func ReadBinary(path string) ([]Product, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return DecodeBinary(data)
}

func DecodeBinary(data []byte) ([]Product, error) {
	r := bufio.NewReader(bytes.NewReader(data))
	var err error
	var magic [8]byte
	if _, err = io.ReadFull(r, magic[:]); err != nil {
		return nil, err
	}
	if magic != binaryMagic {
		return nil, fmt.Errorf("invalid M5 binary magic")
	}
	var count uint32
	if err = binary.Read(r, binary.LittleEndian, &count); err != nil {
		return nil, err
	}
	rows := make([]Product, count)
	for i := range rows {
		row := &rows[i]
		if err = binary.Read(r, binary.LittleEndian, &row.ID); err != nil {
			return nil, err
		}
		category, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		row.Category = Category(category)
		status, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		row.Status = Status(status)
		if err = binary.Read(r, binary.LittleEndian, &row.PriceCents); err != nil {
			return nil, err
		}
		rating, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		row.RatingTenths = rating
		region, e := r.ReadByte()
		if e != nil {
			return nil, e
		}
		row.Region = Region(region)
		var nameLen uint16
		if err = binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}
		name := make([]byte, nameLen)
		if _, err = io.ReadFull(r, name); err != nil {
			return nil, err
		}
		row.Name = string(name)
	}
	return rows, nil
}
