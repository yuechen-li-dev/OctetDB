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
