package scheduled

import "testing"

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
