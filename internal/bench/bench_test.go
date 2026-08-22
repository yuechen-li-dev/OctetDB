package bench

import (
	"testing"
	"time"
)

func TestM4PhaseScheduleIsAGenuineDeterministicRamp(t *testing.T) {
	phase := Phase{Name: "ramp_up", Duration: 2 * time.Second, Rate: 250, EndRate: 1800}
	a := phaseSchedule(phase)
	b := phaseSchedule(phase)
	if len(a) != 2050 || len(b) != len(a) {
		t.Fatalf("ramp count=%d/%d, want deterministic average-rate count 2050", len(a), len(b))
	}
	first, last := 0, 0
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("schedule differs at %d", i)
		}
		if a[i] < 500*time.Millisecond {
			first++
		}
		if a[i] >= 1500*time.Millisecond {
			last++
		}
	}
	if first >= last {
		t.Fatalf("not a ramp: first-half-second=%d last-half-second=%d", first, last)
	}
}
