package specialized

import (
	"testing"
	"time"
)

var benchmarkResult int

func templateBenchmarkJobs() []Main_Job {
	jobs := make([]Main_Job, 10000)
	for i := range jobs {
		jobs[i] = Main_Job{ID: i, Status: i % 4}
	}
	return jobs
}

func templateBenchmarkReady(job Main_Job) bool { return job.Status == 1 }

func runBespoke(jobs []Main_Job, limit int) int {
	machine := NewBespokeFilteredView(jobs, templateBenchmarkReady, limit)
	count := 0
	for {
		turn, err := machine.Step()
		if err != nil {
			panic(err)
		}
		if turn.DidYield() {
			count++
		}
		if turn.Complete() {
			return count
		}
	}
}

func runTemplate(jobs []Main_Job, limit int) int {
	machine := NewDatabaseTemplates__FilteredView__Job(jobs, templateBenchmarkReady, limit)
	count := 0
	for {
		turn, err := machine.Step()
		if err != nil {
			panic(err)
		}
		if turn.DidYield() {
			count++
		}
		if turn.Complete() {
			return count
		}
	}
}

func TestTemplateAndBespokeW5ResultParity(t *testing.T) {
	jobs := templateBenchmarkJobs()
	for _, limit := range []int{1, 10, 2500, 5000} {
		bespoke := runBespoke(jobs, limit)
		templated := runTemplate(jobs, limit)
		if bespoke != templated {
			t.Fatalf("limit=%d bespoke=%d template=%d", limit, bespoke, templated)
		}
	}
}

func BenchmarkTemplateVersusBespokeW5(b *testing.B) {
	jobs := templateBenchmarkJobs()
	const limit = 2500
	benchmarkBespokeW5(b, jobs, limit)
	benchmarkTemplateW5(b, jobs, limit)
}

// Reverse order is a control for thermal/turbo and sub-benchmark ordering.
func BenchmarkTemplateVersusBespokeW5Reverse(b *testing.B) {
	jobs := templateBenchmarkJobs()
	const limit = 2500
	benchmarkTemplateW5(b, jobs, limit)
	benchmarkBespokeW5(b, jobs, limit)
}

func benchmarkBespokeW5(b *testing.B, jobs []Main_Job, limit int) {
	b.Run("bespoke", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkResult = runBespoke(jobs, limit)
		}
	})
}

func benchmarkTemplateW5(b *testing.B, jobs []Main_Job, limit int) {
	b.Run("template", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkResult = runTemplate(jobs, limit)
		}
	})
}

// PairedNormalized measures both lanes in each benchmark operation and
// alternates their order. This removes sub-benchmark thermal/turbo ordering
// while retaining the two independently emitted facades.
func BenchmarkW5PairedNormalized(b *testing.B) {
	jobs := templateBenchmarkJobs()
	const limit = 2500
	var bespokeNanos int64
	var templateNanos int64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%2 == 0 {
			started := time.Now()
			benchmarkResult = runBespoke(jobs, limit)
			bespokeNanos += time.Since(started).Nanoseconds()
			started = time.Now()
			benchmarkResult = runTemplate(jobs, limit)
			templateNanos += time.Since(started).Nanoseconds()
		} else {
			started := time.Now()
			benchmarkResult = runTemplate(jobs, limit)
			templateNanos += time.Since(started).Nanoseconds()
			started = time.Now()
			benchmarkResult = runBespoke(jobs, limit)
			bespokeNanos += time.Since(started).Nanoseconds()
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(bespokeNanos)/float64(b.N), "bespoke-ns/op")
	b.ReportMetric(float64(templateNanos)/float64(b.N), "template-ns/op")
	b.ReportAllocs()
}
