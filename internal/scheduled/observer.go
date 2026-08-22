package scheduled

const m4PredictionHorizonTicks = 2

type ObserverMetrics struct {
	Samples                int64   `json:"samples"`
	QueuePersistenceMAE    float64 `json:"queue_persistence_mae"`
	QueueLinearMAE         float64 `json:"queue_linear_mae"`
	QueuePersistenceBias   float64 `json:"queue_persistence_bias"`
	QueueLinearBias        float64 `json:"queue_linear_bias"`
	QueueLinearWins        int64   `json:"queue_linear_wins"`
	DelaySamples           int64   `json:"delay_samples"`
	DelayPersistenceMAEUS  float64 `json:"delay_persistence_mae_us"`
	DelayLinearMAEUS       float64 `json:"delay_linear_mae_us"`
	DelayPersistenceBiasUS float64 `json:"delay_persistence_bias_us"`
	DelayLinearBiasUS      float64 `json:"delay_linear_bias_us"`
	DelayLinearWins        int64   `json:"delay_linear_wins"`
	RawQueueVariance       float64 `json:"raw_queue_variance"`
	FilteredQueueVariance  float64 `json:"filtered_queue_variance"`
	WouldLimitReactive     int64   `json:"would_limit_reactive"`
	WouldLimitPredictive   int64   `json:"would_limit_predictive"`
	ControlActions         int64   `json:"control_actions"`
	TargetChanges          int64   `json:"target_changes"`
	LimitedTicks           int64   `json:"limited_ticks"`
	MinimumExposure        int     `json:"minimum_exposure"`
	MaximumExposure        int     `json:"maximum_exposure"`
	HorizonTicks           int     `json:"horizon_ticks"`
	TickMicros             int     `json:"tick_micros"`
}

type ObserverEvent struct {
	Tick                          int64 `json:"tick"`
	AtNS                          int64 `json:"at_ns"`
	RawQueue                      int   `json:"raw_queue"`
	FilteredQueueMilli            int   `json:"filtered_queue_milli"`
	Arrivals                      int   `json:"arrivals"`
	Completions                   int   `json:"completions"`
	MeanDispatchDelayMicros       int   `json:"mean_dispatch_delay_micros"`
	PersistenceQueueMilli         int   `json:"persistence_queue_milli"`
	LinearQueueMilli              int   `json:"linear_queue_milli"`
	PersistenceDelayMicros        int   `json:"persistence_delay_micros"`
	LinearDelayMicros             int   `json:"linear_delay_micros"`
	ActualQueueMilli              int   `json:"actual_queue_milli,omitempty"`
	ActualDelayMicros             int   `json:"actual_delay_micros,omitempty"`
	MaturedPersistenceQueueMilli  int   `json:"matured_persistence_queue_milli,omitempty"`
	MaturedLinearQueueMilli       int   `json:"matured_linear_queue_milli,omitempty"`
	MaturedPersistenceDelayMicros int   `json:"matured_persistence_delay_micros,omitempty"`
	MaturedLinearDelayMicros      int   `json:"matured_linear_delay_micros,omitempty"`
	Matured                       bool  `json:"matured"`
	ReactiveWouldLimit            bool  `json:"reactive_would_limit"`
	PredictiveWouldLimit          bool  `json:"predictive_would_limit"`
}

type observerPrediction struct {
	tick, persistenceQueue, linearQueue int
	persistenceDelay, linearDelay       int
}

type observerRuntime struct {
	state                                              Scheduler_M4ObserverState
	tick                                               int
	ring                                               [m4PredictionHorizonTicks + 1]observerPrediction
	samples, queuePersistAbs, queueLinearAbs           int64
	queuePersistBias, queueLinearBias                  int64
	queueLinearWins                                    int64
	delaySamples, delayPersistAbs, delayLinearAbs      int64
	delayPersistBias, delayLinearBias, delayLinearWins int64
	rawSum, rawSquares, filteredSum, filteredSquares   int64
	wouldReactive, wouldPredictive                     int64
	controlActions, targetChanges, limitedTicks        int64
	minimumExposure, maximumExposure                   int
}

func absInt(value int) int {
	if value < 0 {
		return -value
	}
	return value
}

func (o *observerRuntime) update(rawQueue, arrivals, completions int, delayMicros int) ObserverEvent {
	if completions == 0 {
		delayMicros = int(o.state.Delay * 1_000_000)
	}
	out := fn_Scheduler_M4ObserverUpdate(o.state, rawQueue, arrivals, completions, float64(delayMicros)/1_000_000, m4PredictionHorizonTicks)
	o.state = out.State
	o.tick++
	e := ObserverEvent{Tick: int64(o.tick), RawQueue: rawQueue, FilteredQueueMilli: out.State.QueueMilli, Arrivals: arrivals, Completions: completions, MeanDispatchDelayMicros: delayMicros, PersistenceQueueMilli: out.PersistenceQueueMilli, LinearQueueMilli: out.LinearQueueMilli, PersistenceDelayMicros: int(out.PersistenceDelay * 1_000_000), LinearDelayMicros: int(out.LinearDelay * 1_000_000)}
	e.ReactiveWouldLimit = e.PersistenceDelayMicros >= 1800
	e.PredictiveWouldLimit = e.LinearDelayMicros >= 1800
	if e.ReactiveWouldLimit {
		o.wouldReactive++
	}
	if e.PredictiveWouldLimit {
		o.wouldPredictive++
	}
	o.rawSum += int64(rawQueue * 1000)
	o.rawSquares += int64(rawQueue*1000) * int64(rawQueue*1000)
	o.filteredSum += int64(out.State.QueueMilli)
	o.filteredSquares += int64(out.State.QueueMilli) * int64(out.State.QueueMilli)
	matureIndex := (o.tick - m4PredictionHorizonTicks) % len(o.ring)
	matured := observerPrediction{}
	if matureIndex >= 0 {
		matured = o.ring[matureIndex]
	}
	if matured.tick != 0 && o.tick-matured.tick == m4PredictionHorizonTicks {
		actualQueue := rawQueue * 1000
		persistErr := matured.persistenceQueue - actualQueue
		linearErr := matured.linearQueue - actualQueue
		o.samples++
		o.queuePersistAbs += int64(absInt(persistErr))
		o.queueLinearAbs += int64(absInt(linearErr))
		o.queuePersistBias += int64(persistErr)
		o.queueLinearBias += int64(linearErr)
		if absInt(linearErr) < absInt(persistErr) {
			o.queueLinearWins++
		}
		e.ActualQueueMilli, e.Matured = actualQueue, true
		e.MaturedPersistenceQueueMilli = matured.persistenceQueue
		e.MaturedLinearQueueMilli = matured.linearQueue
		if completions > 0 {
			persistDelayErr := matured.persistenceDelay - delayMicros
			linearDelayErr := matured.linearDelay - delayMicros
			o.delaySamples++
			o.delayPersistAbs += int64(absInt(persistDelayErr))
			o.delayLinearAbs += int64(absInt(linearDelayErr))
			o.delayPersistBias += int64(persistDelayErr)
			o.delayLinearBias += int64(linearDelayErr)
			if absInt(linearDelayErr) < absInt(persistDelayErr) {
				o.delayLinearWins++
			}
			e.ActualDelayMicros = delayMicros
			e.MaturedPersistenceDelayMicros = matured.persistenceDelay
			e.MaturedLinearDelayMicros = matured.linearDelay
		}
	}
	writeIndex := o.tick % len(o.ring)
	o.ring[writeIndex] = observerPrediction{tick: o.tick, persistenceQueue: out.PersistenceQueueMilli, linearQueue: out.LinearQueueMilli, persistenceDelay: e.PersistenceDelayMicros, linearDelay: e.LinearDelayMicros}
	return e
}

func ratio(n, d int64) float64 {
	if d == 0 {
		return 0
	}
	return float64(n) / float64(d)
}

func variance(sum, squares, count int64) float64 {
	if count == 0 {
		return 0
	}
	mean := float64(sum) / float64(count)
	return float64(squares)/float64(count) - mean*mean
}

func (o *observerRuntime) metrics() ObserverMetrics {
	return ObserverMetrics{Samples: o.samples, QueuePersistenceMAE: ratio(o.queuePersistAbs, o.samples) / 1000, QueueLinearMAE: ratio(o.queueLinearAbs, o.samples) / 1000, QueuePersistenceBias: ratio(o.queuePersistBias, o.samples) / 1000, QueueLinearBias: ratio(o.queueLinearBias, o.samples) / 1000, QueueLinearWins: o.queueLinearWins, DelaySamples: o.delaySamples, DelayPersistenceMAEUS: ratio(o.delayPersistAbs, o.delaySamples), DelayLinearMAEUS: ratio(o.delayLinearAbs, o.delaySamples), DelayPersistenceBiasUS: ratio(o.delayPersistBias, o.delaySamples), DelayLinearBiasUS: ratio(o.delayLinearBias, o.delaySamples), DelayLinearWins: o.delayLinearWins, RawQueueVariance: variance(o.rawSum, o.rawSquares, int64(o.tick)) / 1e6, FilteredQueueVariance: variance(o.filteredSum, o.filteredSquares, int64(o.tick)) / 1e6, WouldLimitReactive: o.wouldReactive, WouldLimitPredictive: o.wouldPredictive, ControlActions: o.controlActions, TargetChanges: o.targetChanges, LimitedTicks: o.limitedTicks, MinimumExposure: o.minimumExposure, MaximumExposure: o.maximumExposure, HorizonTicks: m4PredictionHorizonTicks, TickMicros: 1000}
}

func (o *observerRuntime) recordControl(exposure, maximum int, changed bool) {
	o.controlActions++
	if changed {
		o.targetChanges++
	}
	if exposure < maximum {
		o.limitedTicks++
	}
	if o.minimumExposure == 0 || exposure < o.minimumExposure {
		o.minimumExposure = exposure
	}
	if exposure > o.maximumExposure {
		o.maximumExposure = exposure
	}
}

// reactiveExposure is the only M4 actuator. It cannot exceed the static worker
// bound or fall below one, and the 1.4-1.8 ms band prevents tick-to-tick chatter.
func reactiveExposure(current, maximum, filteredDelayMicros int) int {
	if filteredDelayMicros >= 1800 {
		target := maximum - 2
		if target < 1 {
			return 1
		}
		return target
	}
	if filteredDelayMicros <= 1400 {
		return maximum
	}
	return current
}

func (s *Scheduler) EnableObserverTrace() { s.observerTraceEnabled.Store(true) }

func (s *Scheduler) ObserverTrace() []ObserverEvent {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	return append([]ObserverEvent(nil), s.observerEvents...)
}

func (s *Scheduler) ObserverMetrics() ObserverMetrics {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	return s.observerSummary
}

func (s *Scheduler) recordObserver(event ObserverEvent, summary ObserverMetrics) {
	s.observerMu.Lock()
	defer s.observerMu.Unlock()
	s.observerSummary = summary
	if s.observerTraceEnabled.Load() && len(s.observerEvents) < 16384 {
		s.observerEvents = append(s.observerEvents, event)
	}
}
