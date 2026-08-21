package metrics

import (
	"testing"
	"time"
)

func TestSummarizeCountsAndPercentiles(t *testing.T) {
	s := []Sample{
		{Admitted: true, Latency: time.Millisecond, Queue: time.Microsecond, Service: time.Millisecond, BatchSize: 2},
		{Admitted: true, Latency: 2 * time.Millisecond, Queue: time.Microsecond, Service: time.Millisecond, BatchSize: 2},
		{Rejected: true},
	}
	got := Summarize(s, time.Second, 4, time.Time{})
	if got.Attempted != 3 || got.Admitted != 2 || got.Completed != 2 || got.Rejected != 1 {
		t.Fatalf("bad counts: %+v", got)
	}
	if got.BatchCount != 1 || got.AverageBatchSize != 2 || got.Latency.P50 != 1 {
		t.Fatalf("bad aggregate: %+v", got)
	}
}
