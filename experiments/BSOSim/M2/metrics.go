package bsosim

import "sync"

// metricStore is deliberately the entire metrics concurrency abstraction.
// It keeps M2 counters race-safe without introducing a telemetry framework.
type metricStore struct {
	mu sync.Mutex
	m  Metrics
}

func (s *metricStore) add(fn func(*Metrics)) {
	s.mu.Lock()
	fn(&s.m)
	s.mu.Unlock()
}

func (s *metricStore) snapshot() Metrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m
}
