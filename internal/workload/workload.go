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

// Generate returns the authoritative deterministic logical sequence used by
// both lanes. Writes use unique IDs, making duplicates visible as failures.
func Generate(seed uint64, count int, start time.Time) []Operation {
	rng := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	ops := make([]Operation, count)
	for i := range ops {
		roll := rng.IntN(100)
		kind := PointRead
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
		ops[i] = Operation{
			Sequence:   int64(i),
			Kind:       kind,
			CustomerID: int64(rng.IntN(100) + 1),
			OrderID:    1_000_000 + int64(i),
			ProductID:  int64(rng.IntN(20) + 1),
			At:         start.Add(time.Duration(i) * time.Microsecond),
		}
	}
	return ops
}
