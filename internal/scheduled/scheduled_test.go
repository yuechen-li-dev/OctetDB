package scheduled

import (
	"testing"

	"github.com/yuechen-li-dev/database-scheduler/internal/workload"
)

func TestGeneratedPolicyDecisionsAndStateHistory(t *testing.T) {
	tests := []struct {
		name string
		args [6]int
		want int
	}{
		{"reject full", [6]int{4, 4, 0, 0, 0, 8}, 0},
		{"admit empty batch", [6]int{0, 4, 0, 0, 0, 8}, 1},
		{"write no batch", [6]int{0, 4, 2, 2, 1, 8}, 1},
		{"incompatible read", [6]int{0, 4, 1, 0, 1, 8}, 1},
		{"compatible read", [6]int{0, 4, 0, 0, 1, 8}, 2},
		{"batch ready", [6]int{0, 4, 0, 0, 7, 8}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := policyDecision(tt.args[0], tt.args[1], tt.args[2], tt.args[3], tt.args[4], tt.args[5])
			if got != tt.want {
				t.Fatalf("decision=%d want=%d", got, tt.want)
			}
		})
	}
	m := fn_Scheduler_SchedulerDecision(4, 4, 0, 0, 0, 8)
	m.__octStep()
	history := m.__octStateHistory()
	if len(history) < 3 || history[0] != "Idle" || history[1] != "Observe" || history[2] != "Complete" {
		t.Fatalf("unexpected explicit state history: %v", history)
	}
}

func TestStaticPlanRemovesRuntimeMetadataConstruction(t *testing.T) {
	runtimeScheduler := NewRuntime(nil, 128, 8, 8, 2)
	runtimeMetrics := runtimeScheduler.Initialization()
	runtimeScheduler.Close()
	staticScheduler := NewStatic(nil, 128, 8, 8, 2)
	staticMetrics := staticScheduler.Initialization()
	staticScheduler.Close()
	t.Logf("runtime=%+v static=%+v", runtimeMetrics, staticMetrics)
	if staticMetrics.MetadataAllocations >= runtimeMetrics.MetadataAllocations {
		t.Fatalf("static metadata allocations=%d, want less than runtime=%d", staticMetrics.MetadataAllocations, runtimeMetrics.MetadataAllocations)
	}
	if staticMetrics.MetadataAllocatedBytes >= runtimeMetrics.MetadataAllocatedBytes {
		t.Fatalf("static metadata bytes=%d, want less than runtime=%d", staticMetrics.MetadataAllocatedBytes, runtimeMetrics.MetadataAllocatedBytes)
	}
}

func TestRuntimeAndStaticPlansAreEquivalent(t *testing.T) {
	runtimePlan := buildRuntimePlan(128, 8, 8)
	staticPlan := staticPlanLookup{plan: &staticExecutionPlan}
	for kind := 0; kind < 4; kind++ {
		runtimeCommand, runtimeOK := runtimePlan.command(workload.Kind(kind))
		staticCommand, staticOK := staticPlan.command(workload.Kind(kind))
		if runtimeOK != staticOK || runtimeCommand != staticCommand {
			t.Fatalf("command %d differs: runtime=%+v static=%+v", kind, runtimeCommand, staticCommand)
		}
		for right := 0; right < 4; right++ {
			if runtimePlan.compatible(workload.Kind(kind), workload.Kind(right)) != staticPlan.compatible(workload.Kind(kind), workload.Kind(right)) {
				t.Fatalf("compatibility[%d][%d] differs", kind, right)
			}
		}
	}
}
