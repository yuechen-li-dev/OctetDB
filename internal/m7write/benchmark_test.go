package m7write

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	generated "github.com/yuechen-li-dev/database-scheduler/internal/m7generated"
)

var benchmarkDecision generated.Main_TransitionDecision
var benchmarkCheckpoint generated.AccountAgentCheckpoint
var benchmarkMachine *generated.AccountAgent
var benchmarkResult Result

func BenchmarkGeneratedFacadeStepYield(b *testing.B) {
	machine := generated.NewAccountAgent(1)
	input := generated.Main_CommandContext{Kind: generated.NewCommandKindDeposit(), AccountA: 1, Amount: 1, ExistsA: true, BalanceA: 100, StatusA: generated.NewAccountStatusOpen(), VersionA: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		turn, err := machine.Step(input)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkDecision, err = turn.Yielded()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGeneratedCheckpoint(b *testing.B) {
	machine := generated.NewAccountAgent(1)
	turn, _ := machine.Step(generated.Main_CommandContext{Kind: generated.NewCommandKindDeposit(), AccountA: 1, Amount: 1})
	_, _ = turn.Yielded()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchmarkCheckpoint, err = machine.Checkpoint()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGeneratedRestore(b *testing.B) {
	machine := generated.NewAccountAgent(1)
	machine.Step(generated.Main_CommandContext{Kind: generated.NewCommandKindDeposit(), AccountA: 1, Amount: 1})
	checkpoint, _ := machine.Checkpoint()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchmarkMachine, err = generated.RestoreAccountAgent(checkpoint)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOctEngineMemoryOnly(b *testing.B) {
	e, _ := Open(Config{Durability: MemoryOnly, MailboxCapacity: 64})
	defer e.Close()
	ctx := context.Background()
	e.Submit(ctx, Command{ID: "setup", Kind: Create, Account: 1, Amount: 100000000})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchmarkResult, err = e.Submit(ctx, Command{ID: fmt.Sprintf("oct-%d", i), Kind: Deposit, Account: 1, Amount: 1})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGoControlMemoryOnly(b *testing.B) {
	e, _ := OpenGoBaseline(Config{Durability: MemoryOnly})
	defer e.Close()
	e.Execute(Command{ID: "setup", Kind: Create, Account: 1, Amount: 100000000})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchmarkResult, err = e.Execute(Command{ID: fmt.Sprintf("go-%d", i), Kind: Deposit, Account: 1, Amount: 1})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestAgentPopulationEvidence(t *testing.T) {
	if os.Getenv("DBSCHED_M7_POPULATION") != "1" {
		t.Skip("evidence-only 100k population probe")
	}
	for _, population := range []int{1, 1000, 10000, 100000} {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)
		e, _ := Open(Config{Durability: MemoryOnly, MailboxCapacity: 1})
		for i := 1; i <= population; i++ {
			id := AccountID(i)
			entry := e.entry(id)
			machine := generated.NewAccountAgent(i)
			machine.Step(generated.Main_CommandContext{Kind: generated.NewCommandKindDeposit(), AccountA: i, Amount: 1, StatusA: generated.NewAccountStatusMissing()})
			checkpoint, err := machine.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			entry.machine = machine
			entry.checkpoint = checkpoint.Bytes()
		}
		runtime.GC()
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		passes := 10
		if passes*population < 1000000 {
			passes = 1000000 / population
		}
		started := time.Now()
		for pass := 0; pass < passes; pass++ {
			for i := 1; i <= population; i++ {
				_ = e.entry(AccountID(i))
			}
		}
		lookup := time.Since(started)
		t.Logf("population=%d heap_delta=%d objects_delta=%d gc_cycles=%d gc_pause_ns=%d lookup_ns=%.2f checkpoint_bytes=%d", population, int64(after.HeapAlloc)-int64(before.HeapAlloc), int64(after.HeapObjects)-int64(before.HeapObjects), after.NumGC-before.NumGC, after.PauseTotalNs-before.PauseTotalNs, float64(lookup.Nanoseconds())/float64(population*passes), len(e.entry(1).checkpoint))
		e.Close()
	}
}
