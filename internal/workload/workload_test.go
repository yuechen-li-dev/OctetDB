package workload

import (
	"reflect"
	"testing"
	"time"
)

func TestGenerateDeterministicAndWriteIDsUnique(t *testing.T) {
	a := Generate(42, 1000, time.Unix(0, 0).UTC())
	b := Generate(42, 1000, time.Unix(0, 0).UTC())
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different workloads")
	}
	seen := map[int64]bool{}
	for _, op := range a {
		if op.Kind == OrderWrite {
			if seen[op.OrderID] {
				t.Fatalf("duplicate order ID %d", op.OrderID)
			}
			seen[op.OrderID] = true
		}
	}
}

func TestContentionRegimesAreDeterministicAndHotIsBounded(t *testing.T) {
	a := GenerateRegime(42, 1000, time.Unix(0, 0).UTC(), HotKeyContention)
	b := GenerateRegime(42, 1000, time.Unix(0, 0).UTC(), HotKeyContention)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("hot regime is not deterministic")
	}
	writes := 0
	for _, op := range a {
		if op.Kind == OrderWrite || op.Kind == InventoryWrite {
			writes++
			if op.CustomerID > 4 || op.ProductID > 2 {
				t.Fatalf("hot write escaped bounded hot set: %+v", op)
			}
		}
	}
	if writes < 500 {
		t.Fatalf("hot regime lacks contention signal: writes=%d", writes)
	}
}
