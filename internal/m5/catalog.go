package m5

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
)

const (
	SchemaVersion   = "product-v1"
	SnapshotVersion = "m5-seed-20260821-n10000"
	DefaultRowCount = 10_000
	FirstProductID  = uint32(1_000_001)
	CategoryCount   = 8
	RegionCount     = 6
)

type Category uint8
type Status uint8
type Region uint8

const (
	StatusDraft Status = iota
	StatusActive
	StatusRetired
)

type Product struct {
	ID           uint32
	Category     Category
	Status       Status
	PriceCents   uint32
	RatingTenths uint8
	Region       Region
	Name         string
}

type Projection struct {
	ID         uint32
	PriceCents uint32
}

type Identity struct {
	SnapshotVersion string
	SnapshotHash    string
	SchemaVersion   string
	RowCount        int
}

// GenerateCatalog is the canonical logical dataset authority. Its output is
// deterministic across lanes and does not depend on map iteration or a clock.
func GenerateCatalog(n int) []Product {
	rows := make([]Product, n)
	state := uint64(20260821)
	for i := range rows {
		state = state*6364136223846793005 + 1442695040888963407
		rows[i] = Product{
			ID:           FirstProductID + uint32(i),
			Category:     Category((state >> 17) % CategoryCount),
			Status:       Status((state >> 29) % 3),
			PriceCents:   199 + uint32((state>>7)%250_000),
			RatingTenths: 10 + uint8((state>>41)%41),
			Region:       Region((state >> 53) % RegionCount),
			Name:         fmt.Sprintf("Product-%06d", i+1),
		}
	}
	return rows
}

func SnapshotIdentity(rows []Product) Identity {
	return Identity{SnapshotVersion: SnapshotVersion, SnapshotHash: HashRows(rows), SchemaVersion: SchemaVersion, RowCount: len(rows)}
}

func HashRows(rows []Product) string {
	h := sha256.New()
	var buf [8]byte
	for _, row := range rows {
		writeProductHash(h, row, buf[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func writeProductHash(h hash.Hash, row Product, buf []byte) {
	binary.LittleEndian.PutUint32(buf[:4], row.ID)
	_, _ = h.Write(buf[:4])
	_, _ = h.Write([]byte{byte(row.Category), byte(row.Status)})
	binary.LittleEndian.PutUint32(buf[:4], row.PriceCents)
	_, _ = h.Write(buf[:4])
	_, _ = h.Write([]byte{row.RatingTenths, byte(row.Region)})
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(row.Name)))
	_, _ = h.Write(buf[:4])
	_, _ = h.Write([]byte(row.Name))
}
