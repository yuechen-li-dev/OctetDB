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

func TestGeneratedAdmissionPolicyDecisions(t *testing.T) {
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
}

func TestPersistentFairControllerRetainsMinimumCommitAndHysteresis(t *testing.T) {
	controller := fn_Scheduler_PersistentFairController().(*__octFlow_Scheduler_PersistentFairController)
	controller.board.OldestEligible = true
	controller.board.OldestScore = 100
	controller.__octStep()
	if controller.board.Chosen != actionOldest {
		t.Fatalf("first choice=%d, want oldest", controller.board.Chosen)
	}
	controller.board.HighEligible = true
	controller.board.HighScore = 200
	controller.__octStep()
	if controller.board.Chosen != actionOldest {
		t.Fatalf("minimum commit choice=%d, want retained oldest", controller.board.Chosen)
	}
	controller.board.HighScore = 120
	controller.__octStep()
	if controller.board.Chosen != actionOldest {
		t.Fatalf("hysteresis choice=%d, want retained oldest", controller.board.Chosen)
	}
	controller.board.HighScore = 200
	controller.__octStep()
	if controller.board.Chosen != actionHighPriority || controller.board.Decisions != 4 {
		t.Fatalf("persistent switch choice=%d decisions=%d", controller.board.Chosen, controller.board.Decisions)
	}
}

func TestPersistentControllersAllocateNothingPerDecision(t *testing.T) {
	parity := fn_Scheduler_PersistentParityController().(*__octFlow_Scheduler_PersistentParityController)
	parity.board.OldestEligible = true
	if allocs := testing.AllocsPerRun(1000, parity.__octStep); allocs != 0 {
		t.Fatalf("parity allocations/decision=%v, want 0", allocs)
	}
	fair := fn_Scheduler_PersistentFairController().(*__octFlow_Scheduler_PersistentFairController)
	fair.board.OldestEligible, fair.board.HighEligible = true, true
	fair.board.OldestScore, fair.board.HighScore = 100, 300
	if allocs := testing.AllocsPerRun(1000, fair.__octStep); allocs != 0 {
		t.Fatalf("fair allocations/decision=%v, want 0", allocs)
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

func TestPriorityAgingBoundsStrictPriorityStarvation(t *testing.T) {
	for _, constructor := range []struct {
		name string
		new  func() *Scheduler
	}{
		{"H", func() *Scheduler { return NewPriority(nil, 128, 8, 8, 2*time.Millisecond) }},
		{"F1", func() *Scheduler { return NewUtility(nil, 128, 8, 8, 2*time.Millisecond) }},
	} {
		t.Run(constructor.name, func(t *testing.T) {
			s := constructor.new()
			defer s.Close()
			base := time.Now()
			low := &request{op: workload.Operation{Sequence: 1, Kind: workload.PointRead}, queuedAt: base}
			high := &request{op: workload.Operation{Sequence: 2, Kind: workload.InventoryWrite, ProductID: 2}, queuedAt: base.Add(time.Millisecond)}
			high.token, _ = conflictToken(s.plan, high.op)
			pending := []*request{low, high}
			if got := s.selectRequest(pending, map[ConflictToken]owner{}, base.Add(2*time.Millisecond)); got != 1 {
				t.Fatalf("before starvation bound selected=%d, want high priority", got)
			}
			if got := s.selectRequest(pending, map[ConflictToken]owner{}, base.Add(11*time.Millisecond)); got != 0 {
				t.Fatalf("after 10ms starvation bound selected=%d, want aged low priority", got)
			}
			if s.ConflictMetrics().FairnessOverrides == 0 {
				t.Fatal("bounded starvation dispatch did not record a fairness override")
			}
		})
	}
}

func TestPolicyCannotSelectUnsafeHighPriorityCandidate(t *testing.T) {
	for _, s := range []*Scheduler{
		NewPriority(nil, 128, 8, 8, 0),
		NewUtility(nil, 128, 8, 8, 0),
	} {
		func() {
			defer s.Close()
			now := time.Now()
			blocked := &request{op: workload.Operation{Sequence: 1, Kind: workload.InventoryWrite, ProductID: 1}, queuedAt: now.Add(-time.Millisecond)}
			blocked.token, _ = conflictToken(s.plan, blocked.op)
			legal := &request{op: workload.Operation{Sequence: 2, Kind: workload.PointRead}, queuedAt: now.Add(-10 * time.Millisecond)}
			owned := map[ConflictToken]owner{blocked.token: {request: &request{op: workload.Operation{Sequence: 0}}, acquiredAt: now}}
			if got := s.selectRequest([]*request{blocked, legal}, owned, now); got != 1 {
				t.Fatalf("strategy=%d selected unsafe index %d", s.strategy, got)
			}
		}()
	}
}

func TestAgedSingletonOverridesBatchOpportunity(t *testing.T) {
	for _, s := range []*Scheduler{NewPriority(nil, 128, 8, 8, 2*time.Millisecond), NewUtility(nil, 128, 8, 8, 2*time.Millisecond)} {
		func() {
			defer s.Close()
			now := time.Now()
			pending := []*request{
				{op: workload.Operation{Sequence: 1, Kind: workload.RangeRead}, queuedAt: now.Add(-11 * time.Millisecond)},
				{op: workload.Operation{Sequence: 2, Kind: workload.PointRead}, queuedAt: now.Add(-3 * time.Millisecond)},
				{op: workload.Operation{Sequence: 3, Kind: workload.PointRead}, queuedAt: now.Add(-3 * time.Millisecond)},
				{op: workload.Operation{Sequence: 4, Kind: workload.PointRead}, queuedAt: now.Add(-3 * time.Millisecond)},
			}
			if got := s.selectRequest(pending, map[ConflictToken]owner{}, now); got != 0 {
				t.Fatalf("strategy=%d selected=%d, want aged singleton", s.strategy, got)
			}
		}()
	}
}

func TestStaticPlanExcludesCrossPriorityTokenInversion(t *testing.T) {
	plan := staticPlanLookup{plan: &staticExecutionPlan}
	ownerPriority := -1
	for kind := workload.PointRead; kind <= workload.InventoryWrite; kind++ {
		d, _ := plan.command(kind)
		if d.Transaction == 0 {
			continue
		}
		if ownerPriority < 0 {
			ownerPriority = d.Priority
		} else if d.Priority != ownerPriority {
			t.Fatalf("token owners span priorities %d and %d; inversion policy needs characterization", ownerPriority, d.Priority)
		}
	}
	if ownerPriority != 1 {
		t.Fatalf("exclusive token owner priority=%d, want static high priority class 1", ownerPriority)
	}
}

func TestOneControllerPerSchedulerAndRepeatedDecisions(t *testing.T) {
	s := NewUtility(nil, 128, 8, 8, 0)
	defer s.Close()
	now := time.Now()
	r := &request{op: workload.Operation{Sequence: 1, Kind: workload.InventoryWrite, ProductID: 1}, queuedAt: now}
	r.token, _ = conflictToken(s.plan, r.op)
	for i := 0; i < 4; i++ {
		if got := s.selectRequest([]*request{r}, map[ConflictToken]owner{}, now.Add(time.Duration(i)*time.Millisecond)); got != 0 {
			t.Fatalf("decision %d selected=%d", i, got)
		}
	}
	m := s.ConflictMetrics()
	if m.ControllerConstructions != 1 || m.UtilityDecisions != 4 || s.fairPolicy.board.Decisions != 4 {
		t.Fatalf("controller lifetime metrics=%+v board decisions=%d", m, s.fairPolicy.board.Decisions)
	}
}

var benchmarkPolicyChoice int

func BenchmarkPersistentOctParityPolicy(b *testing.B) {
	controller := fn_Scheduler_PersistentParityController().(*__octFlow_Scheduler_PersistentParityController)
	controller.board.OldestEligible = true
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		controller.__octStep()
	}
	benchmarkPolicyChoice = controller.board.Chosen
}

func BenchmarkPersistentOctFairPolicy(b *testing.B) {
	controller := fn_Scheduler_PersistentFairController().(*__octFlow_Scheduler_PersistentFairController)
	controller.board.OldestEligible, controller.board.HighEligible = true, true
	controller.board.OldestScore, controller.board.HighScore = 100, 300
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		controller.__octStep()
	}
	benchmarkPolicyChoice = controller.board.Chosen
}

func BenchmarkConventionalFairPolicy(b *testing.B) {
	var controller conventionalPolicy
	var counters conflictCounters
	eligible := func(action int) bool { return action == actionOldest || action == actionHighPriority }
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkPolicyChoice = controller.selectAction(actionHighPriority, 300, false, eligible, &counters)
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
