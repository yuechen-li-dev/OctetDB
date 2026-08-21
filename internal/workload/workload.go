package workload

import (
	"fmt"
	"math/rand/v2"
	"time"
)

type Kind uint8

const (
	PointRead Kind = iota
	RangeRead
	OrderWrite
	InventoryWrite
)

func (k Kind) String() string {
	switch k {
	case PointRead:
		return "point_read"
	case RangeRead:
		return "range_read"
	case OrderWrite:
		return "order_write"
	case InventoryWrite:
		return "inventory_write"
	default:
		return fmt.Sprintf("kind_%d", k)
	}
}

type Operation struct {
	Sequence   int64
	Kind       Kind
	CustomerID int64
	OrderID    int64
	ProductID  int64
	At         time.Time
}

// ConflictRegime controls only the distribution of otherwise identical
// logical operations. It is deliberately closed and deterministic so every
// benchmark lane sees the same hot and cold keys.
type ConflictRegime uint8

const (
	LowContention ConflictRegime = iota
	MixedContention
	HotKeyContention
)

// Generate returns the authoritative deterministic logical sequence used by
// both lanes. Writes use unique IDs, making duplicates visible as failures.
func Generate(seed uint64, count int, start time.Time) []Operation {
	return GenerateRegime(seed, count, start, LowContention)
}

// GenerateRegime produces a realistic contention probe: low uses the full
// dataset, mixed sends half of write traffic to a small hot set, and hot sends
// most write traffic to four customers and two products. Unique order IDs and
// stable timestamps preserve the original duplicate-write oracle.
func GenerateRegime(seed uint64, count int, start time.Time, regime ConflictRegime) []Operation {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	ops := make([]Operation, count)
	for i := range ops {
		roll := rng.IntN(100)
		kind := PointRead
		if regime == HotKeyContention {
			switch {
			case roll < 20:
				kind = PointRead
			case roll < 35:
				kind = RangeRead
			case roll < 60:
				kind = OrderWrite
			default:
				kind = InventoryWrite
			}
		} else {
			switch {
			case roll < 55:
				kind = PointRead
			case roll < 80:
				kind = RangeRead
			case roll < 90:
				kind = OrderWrite
			default:
				kind = InventoryWrite
			}
		}
		customerID := int64(rng.IntN(100) + 1)
		productID := int64(rng.IntN(20) + 1)
		if regime == HotKeyContention || (regime == MixedContention && rng.IntN(2) == 0) {
			customerID = int64(rng.IntN(4) + 1)
			productID = int64(rng.IntN(2) + 1)
		}
		ops[i] = Operation{
			Sequence:   int64(i),
			Kind:       kind,
			CustomerID: customerID,
			OrderID:    1_000_000 + int64(i),
			ProductID:  productID,
			At:         start.Add(time.Duration(i) * time.Microsecond),
		}
	}
	return ops
}
