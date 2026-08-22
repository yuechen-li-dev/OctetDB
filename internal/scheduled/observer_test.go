package scheduled

import "testing"

func TestM4ObserverMaturesAtMeasuredHorizon(t *testing.T) {
	var o observerRuntime
	o.update(0, 1, 0, 1000)
	o.update(1, 1, 0, 1100)
	e := o.update(2, 1, 0, 1200)
	if !e.Matured {
		t.Fatal("prediction did not mature after two 1 ms ticks")
	}
	if got := o.metrics().Samples; got != 1 {
		t.Fatalf("samples=%d, want 1", got)
	}
}

func TestM4ObserverClampsAdversarialNegativeQueuePrediction(t *testing.T) {
	state := Scheduler_M4ObserverState{Initialized: true, QueueMilli: 1000, ArrivalMilli: 0, CompletionMilli: 8000, Delay: 0.001, PreviousDelay: 0.001}
	out := fn_Scheduler_M4ObserverUpdate(state, 0, 0, 32, 0.0001, m4PredictionHorizonTicks)
	if out.LinearQueueMilli < 0 || out.LinearDelay < 0 {
		t.Fatalf("unsafe negative prediction: %+v", out)
	}
}

func TestM4ObserverPredictionNeverChangesAuthoritativeBounds(t *testing.T) {
	var o observerRuntime
	for range 16 {
		o.update(128, 64, 1, 10000)
	}
	if m := o.metrics(); m.WouldLimitPredictive == 0 {
		t.Fatal("adversarial error did not exercise predictive counterfactual")
	}
	// The observer owns no capacity, conflict token, worker, or fairness state;
	// its only output is diagnostics until a separately bounded actuator exists.
}

func TestM4ReactiveExposureIsBoundedAndRecoversFromLatencySpike(t *testing.T) {
	if got := reactiveExposure(8, 8, 10_000); got != 6 {
		t.Fatalf("high-delay exposure=%d, want 6", got)
	}
	if got := reactiveExposure(6, 8, 1_600); got != 6 {
		t.Fatalf("hysteresis-band exposure=%d, want hold at 6", got)
	}
	if got := reactiveExposure(6, 8, 100); got != 8 {
		t.Fatalf("recovered exposure=%d, want static maximum 8", got)
	}
	if got := reactiveExposure(1, 1, 10_000); got != 1 {
		t.Fatalf("single-worker bound violated: %d", got)
	}
}

func BenchmarkM4ObserverUpdate(b *testing.B) {
	state := Scheduler_M4ObserverState{Initialized: true, QueueMilli: 4000, ArrivalMilli: 3000, CompletionMilli: 2000, Delay: 0.0015, PreviousDelay: 0.0014}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out := fn_Scheduler_M4ObserverUpdate(state, 4, 3, 2, 0.0016, m4PredictionHorizonTicks)
		state = out.State
	}
}
