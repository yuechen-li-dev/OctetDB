package metrics

import (
	"math"
	"sort"
	"time"
)

type Sample struct {
	CompletedAt  time.Time
	Phase        string
	Latency      time.Duration
	Queue        time.Duration
	Service      time.Duration
	ConflictWait time.Duration
	BatchSize    int
	Admitted     bool
	Rejected     bool
	Failed       bool
	Priority     int
}

type Percentiles struct {
	P50  float64 `json:"p50_ms"`
	P95  float64 `json:"p95_ms"`
	P99  float64 `json:"p99_ms"`
	P999 float64 `json:"p99_9_ms,omitempty"`
}

type Summary struct {
	Attempted        int                     `json:"attempted"`
	Admitted         int                     `json:"admitted"`
	Completed        int                     `json:"completed"`
	Rejected         int                     `json:"rejected"`
	Failed           int                     `json:"failed"`
	ThroughputPerSec float64                 `json:"throughput_per_sec"`
	Latency          Percentiles             `json:"latency"`
	Queue            Percentiles             `json:"queue_residence"`
	Service          Percentiles             `json:"database_service"`
	ConflictWait     Percentiles             `json:"conflict_wait"`
	BatchCount       int                     `json:"batch_count"`
	BatchSizes       map[int]int             `json:"batch_size_distribution"`
	AverageBatchSize float64                 `json:"average_batch_size"`
	BatchFillRatio   float64                 `json:"batch_fill_ratio"`
	RecoverySeconds  *float64                `json:"recovery_seconds"`
	Phases           map[string]PhaseSummary `json:"phases"`
	Priorities       map[int]PrioritySummary `json:"priorities"`
}

type PrioritySummary struct {
	Completed int         `json:"completed"`
	Wait      Percentiles `json:"wait"`
	MaxWaitMS float64     `json:"max_wait_ms"`
}

type PhaseSummary struct {
	Attempted int         `json:"attempted"`
	Admitted  int         `json:"admitted"`
	Completed int         `json:"completed"`
	Rejected  int         `json:"rejected"`
	Failed    int         `json:"failed"`
	Latency   Percentiles `json:"latency"`
	Queue     Percentiles `json:"queue_residence"`
	Service   Percentiles `json:"database_service"`
}

func Summarize(samples []Sample, elapsed time.Duration, maxBatch int, overloadEnd time.Time) Summary {
	out := Summary{Attempted: len(samples), BatchSizes: map[int]int{}, Phases: map[string]PhaseSummary{}, Priorities: map[int]PrioritySummary{}}
	latencies, queues, services, conflictWaits := []time.Duration{}, []time.Duration{}, []time.Duration{}, []time.Duration{}
	totalBatchItems := 0
	for _, sample := range samples {
		if sample.Admitted {
			out.Admitted++
		}
		if sample.Rejected {
			out.Rejected++
			continue
		}
		if sample.Failed {
			out.Failed++
			continue
		}
		out.Completed++
		latencies = append(latencies, sample.Latency)
		queues = append(queues, sample.Queue)
		services = append(services, sample.Service)
		if sample.ConflictWait > 0 {
			conflictWaits = append(conflictWaits, sample.ConflictWait)
		}
		if sample.BatchSize > 0 {
			out.BatchSizes[sample.BatchSize]++
		}
	}
	// Each request reports its batch size. Divide counts by size to recover the
	// number of physical batch dispatches without a second instrumentation path.
	for size, requests := range out.BatchSizes {
		batches := requests / size
		out.BatchCount += batches
		totalBatchItems += batches * size
	}
	if out.BatchCount > 0 {
		out.AverageBatchSize = float64(totalBatchItems) / float64(out.BatchCount)
		out.BatchFillRatio = out.AverageBatchSize / float64(maxBatch)
	}
	if elapsed > 0 {
		out.ThroughputPerSec = float64(out.Completed) / elapsed.Seconds()
	}
	out.Latency = percentiles(latencies)
	out.Queue = percentiles(queues)
	out.Service = percentiles(services)
	out.ConflictWait = percentiles(conflictWaits)
	out.RecoverySeconds = recovery(samples, overloadEnd)
	groups := map[string][]Sample{}
	for _, sample := range samples {
		groups[sample.Phase] = append(groups[sample.Phase], sample)
	}
	for name, group := range groups {
		out.Phases[name] = summarizePhase(group)
	}
	priorityGroups := map[int][]time.Duration{}
	for _, sample := range samples {
		if !sample.Rejected && !sample.Failed {
			priorityGroups[sample.Priority] = append(priorityGroups[sample.Priority], sample.Queue)
		}
	}
	for priority, waits := range priorityGroups {
		maximum := time.Duration(0)
		for _, wait := range waits {
			if wait > maximum {
				maximum = wait
			}
		}
		out.Priorities[priority] = PrioritySummary{Completed: len(waits), Wait: percentiles(waits), MaxWaitMS: float64(maximum) / float64(time.Millisecond)}
	}
	return out
}

func summarizePhase(samples []Sample) PhaseSummary {
	out := PhaseSummary{Attempted: len(samples)}
	latencies, queues, services := []time.Duration{}, []time.Duration{}, []time.Duration{}
	for _, sample := range samples {
		if sample.Admitted {
			out.Admitted++
		}
		if sample.Rejected {
			out.Rejected++
			continue
		}
		if sample.Failed {
			out.Failed++
			continue
		}
		out.Completed++
		latencies = append(latencies, sample.Latency)
		queues = append(queues, sample.Queue)
		services = append(services, sample.Service)
	}
	out.Latency = percentiles(latencies)
	out.Queue = percentiles(queues)
	out.Service = percentiles(services)
	return out
}

func percentiles(values []time.Duration) Percentiles {
	if len(values) == 0 {
		return Percentiles{}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	pick := func(q float64) float64 {
		index := int(math.Ceil(q*float64(len(values)))) - 1
		if index < 0 {
			index = 0
		}
		return float64(values[index]) / float64(time.Millisecond)
	}
	out := Percentiles{P50: pick(.5), P95: pick(.95), P99: pick(.99)}
	if len(values) >= 1000 {
		out.P999 = pick(.999)
	}
	return out
}

// recovery returns the first of two consecutive one-second post-overload
// windows whose p95 is within 25% of the pre-overload normal p95.
func recovery(samples []Sample, overloadEnd time.Time) *float64 {
	steady := []time.Duration{}
	for _, s := range samples {
		if s.Phase == "normal_before" && !s.Failed && !s.Rejected {
			steady = append(steady, s.Latency)
		}
	}
	if len(steady) < 20 || overloadEnd.IsZero() {
		return nil
	}
	threshold := time.Duration(percentiles(steady).P95 * 1.25 * float64(time.Millisecond))
	good := 0
	for window := 0; window < 120; window++ {
		start := overloadEnd.Add(time.Duration(window) * time.Second)
		end := start.Add(time.Second)
		values := []time.Duration{}
		for _, s := range samples {
			if !s.Failed && !s.Rejected && !s.CompletedAt.Before(start) && s.CompletedAt.Before(end) {
				values = append(values, s.Latency)
			}
		}
		if len(values) >= 5 && time.Duration(percentiles(values).P95*float64(time.Millisecond)) <= threshold {
			good++
		} else {
			good = 0
		}
		if good == 2 {
			value := float64(window) / 1.0
			return &value
		}
	}
	return nil
}
