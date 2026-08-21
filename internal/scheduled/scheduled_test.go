package scheduled

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestUtilityPolicyEligibilityRankingAndDeterminism(t *testing.T) {
	if got := utilityDecision(false, 1, 10_000, 8, 8); got != utilityDefer {
		t.Fatalf("illegal candidate=%d, want defer/ineligible", got)
	}
	if got := utilityDecision(true, 0, 6_000, 1, 8); got != utilityPromote {
		t.Fatalf("aged candidate=%d, want promote", got)
	}
	if got := utilityDecision(true, 0, 2_500, 4, 8); got != utilityJoinBatch {
		t.Fatalf("batch candidate=%d, want join", got)
	}
	for i := 0; i < 100; i++ {
		if got := utilityDecision(true, 1, 2_500, 1, 8); got != utilityDispatch {
			t.Fatalf("run %d decision=%d, want deterministic dispatch", i, got)
		}
	}
}

func TestControllerPolicyHysteresisAndMinimumCommit(t *testing.T) {
	keep := fn_Scheduler_PolicyHysteresisProbe(false)
	keep.__octStep()
	if value, complete := keep.__octResult(); !complete || value != 1 {
		t.Fatalf("hysteresis probe value=%d complete=%v, want retained choice 1", value, complete)
	}
	switcher := fn_Scheduler_PolicyHysteresisProbe(true)
	switcher.__octStep()
	if value, complete := switcher.__octResult(); !complete || value != 2 {
		t.Fatalf("clear-win probe value=%d complete=%v, want switched choice 2", value, complete)
	}
	commit := fn_Scheduler_PolicyMinCommitProbe()
	commit.__octStep()
	if value, complete := commit.__octResult(); !complete || value != 1 {
		t.Fatalf("min-commit probe value=%d complete=%v, want retained choice 1", value, complete)
	}
}

func TestConflictProjectionUsesStaticDomainAndRuntimeKey(t *testing.T) {
	plan := staticPlanLookup{plan: &staticExecutionPlan}
	order, ok := conflictToken(plan, workload.Operation{Kind: workload.OrderWrite, CustomerID: 42})
	if !ok || order != (ConflictToken{Domain: ConflictOrders, Key: 42}) {
		t.Fatalf("order token=%+v ok=%v", order, ok)
	}
	inventory, ok := conflictToken(plan, workload.Operation{Kind: workload.InventoryWrite, ProductID: 7})
	if !ok || inventory != (ConflictToken{Domain: ConflictInventory, Key: 7}) {
		t.Fatalf("inventory token=%+v ok=%v", inventory, ok)
	}
	if token, ok := conflictToken(plan, workload.Operation{Kind: workload.PointRead, CustomerID: 42}); ok || token.valid() {
		t.Fatalf("MVCC point read unexpectedly owns %+v", token)
	}
}

func TestUtilityFairnessPromotesAgedRequest(t *testing.T) {
	s := NewUtility(nil, 128, 8, 8, 2*time.Millisecond)
	defer s.Close()
	now := time.Now()
	oldRead := &request{op: workload.Operation{Sequence: 1, Kind: workload.PointRead}, queuedAt: now.Add(-10 * time.Millisecond)}
	newWrite := &request{op: workload.Operation{Sequence: 2, Kind: workload.InventoryWrite, ProductID: 2}, queuedAt: now}
	newWrite.token, _ = conflictToken(s.plan, newWrite.op)
	if got := s.selectRequest([]*request{newWrite, oldRead}, map[ConflictToken]owner{}, now); got != 1 {
		t.Fatalf("selected index=%d, want aged low-priority request at index 1", got)
	}
}

func TestCentralizedSchedulerSerializesSameTokenAndRunsIndependentTokens(t *testing.T) {
	s := NewConflictAware(nil, 128, 8, 8, 0)
	defer s.Close()
	var mu sync.Mutex
	active := map[ConflictToken]int{}
	var violations, concurrent atomic.Int64
	s.executeOne = func(_ context.Context, op workload.Operation) error {
		token, _ := conflictToken(s.plan, op)
		mu.Lock()
		active[token]++
		if active[token] > 1 {
			violations.Add(1)
		}
		if len(active) > 1 {
			concurrent.Store(1)
		}
		mu.Unlock()
		time.Sleep(3 * time.Millisecond)
		mu.Lock()
		active[token]--
		if active[token] == 0 {
			delete(active, token)
		}
		mu.Unlock()
		return nil
	}
	s.executeBatch = func(context.Context, []workload.Operation) error { return nil }
	ops := []workload.Operation{
		{Sequence: 0, Kind: workload.InventoryWrite, ProductID: 1},
		{Sequence: 1, Kind: workload.InventoryWrite, ProductID: 1},
		{Sequence: 2, Kind: workload.InventoryWrite, ProductID: 2},
		{Sequence: 3, Kind: workload.OrderWrite, CustomerID: 1},
	}
	var wg sync.WaitGroup
	for _, op := range ops {
		op := op
		wg.Add(1)
		go func() {
			defer wg.Done()
			if result := s.Submit(context.Background(), op); result.Err != nil {
				t.Errorf("submit %d: %v", op.Sequence, result.Err)
			}
		}()
	}
	wg.Wait()
	if violations.Load() != 0 {
		t.Fatalf("same-token concurrent dispatches=%d", violations.Load())
	}
	if concurrent.Load() == 0 {
		t.Fatal("independent tokens never ran concurrently")
	}
	m := s.ConflictMetrics()
	if m.RequestsBlocked == 0 || m.DoubleReleases != 0 || m.OwnershipLeaks != 0 {
		t.Fatalf("unexpected conflict metrics: %+v", m)
	}
}

func TestFailureReleasesOwnershipAndAgentMailboxIsBounded(t *testing.T) {
	s := NewAgentic(nil, 128, 8, 8, 0)
	s.EnableTrace()
	defer s.Close()
	var calls atomic.Int64
	s.executeOne = func(context.Context, workload.Operation) error {
		if calls.Add(1) == 1 {
			return errors.New("injected failure")
		}
		return nil
	}
	s.executeBatch = func(context.Context, []workload.Operation) error { return nil }
	first := make(chan Result, 1)
	go func() {
		first <- s.Submit(context.Background(), workload.Operation{Sequence: 0, Kind: workload.InventoryWrite, ProductID: 1})
	}()
	time.Sleep(time.Millisecond)
	second := s.Submit(context.Background(), workload.Operation{Sequence: 1, Kind: workload.InventoryWrite, ProductID: 1})
	if got := <-first; got.Err == nil {
		t.Fatal("injected failure was not returned")
	}
	if second.Err != nil {
		t.Fatalf("ownership was not released after failure: %v", second.Err)
	}
	m := s.ConflictMetrics()
	if m.MailboxOverflows != 0 || m.PeakMailboxOccupancy > mailboxCapacity || m.DoubleReleases != 0 {
		t.Fatalf("bounded mailbox/release invariant failed: %+v", m)
	}
	trace := s.Trace()
	if len(trace) < 4 {
		t.Fatalf("missing causal trace: %+v", trace)
	}
}

func TestCancellationCompletionReleasesOwnership(t *testing.T) {
	s := NewConflictAware(nil, 128, 8, 8, 0)
	defer s.Close()
	started := make(chan struct{}, 1)
	s.executeOne = func(ctx context.Context, _ workload.Operation) error {
		started <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	s.executeBatch = func(context.Context, []workload.Operation) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan Result, 1)
	go func() {
		result <- s.Submit(ctx, workload.Operation{Sequence: 0, Kind: workload.InventoryWrite, ProductID: 1})
	}()
	<-started
	cancel()
	if got := <-result; !errors.Is(got.Err, context.Canceled) {
		t.Fatalf("submit error=%v, want cancellation", got.Err)
	}
	deadline := time.Now().Add(time.Second)
	for s.InUse() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.InUse() != 0 || s.ConflictMetrics().OwnershipLeaks != 0 {
		t.Fatalf("cancelled request leaked ownership/capacity: in_use=%d metrics=%+v", s.InUse(), s.ConflictMetrics())
	}
}

func TestMailboxFIFOAndOverflowPolicy(t *testing.T) {
	a := newRequestAgent()
	if !a.send(AgentMessage{Kind: MessageWake, Sender: 10}) || !a.send(AgentMessage{Kind: MessageCompletion, Sender: 11}) {
		t.Fatal("mailbox rejected within static capacity")
	}
	if a.send(AgentMessage{Kind: MessageWake, Sender: 12}) {
		t.Fatal("mailbox accepted overflow")
	}
	first, _ := a.receive()
	second, _ := a.receive()
	if first.Sender != 10 || second.Sender != 11 {
		t.Fatalf("mailbox order=%d,%d", first.Sender, second.Sender)
	}
}

func TestOneTokenPerRequestPreventsWaitCycles(t *testing.T) {
	plan := staticPlanLookup{plan: &staticExecutionPlan}
	for _, op := range []workload.Operation{{Kind: workload.OrderWrite, CustomerID: 1, ProductID: 2}, {Kind: workload.InventoryWrite, CustomerID: 1, ProductID: 2}} {
		token, ok := conflictToken(plan, op)
		if !ok || !token.valid() {
			t.Fatalf("missing token for %s", op.Kind)
		}
		// The projection returns a scalar token, not a resource set. A request
		// therefore cannot hold one token while waiting for another.
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
