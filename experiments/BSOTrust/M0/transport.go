package bsosim

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"
)

type transportEvent struct {
	At                   int
	Order                string
	TransferID           string
	WorkerID, Generation int
	Data                 []byte
}

// transport is the sole logical-time and fault authority. Concurrent workers
// only submit; one mutex protects its bounded queue and deterministic attempt
// counters. Fault choices are keyed by stable message identity, not goroutine
// arrival order.
type transport struct {
	mu       sync.Mutex
	seed     int64
	faults   FaultProfile
	now      int
	queue    []transportEvent
	attempts map[string]int
	metrics  *metricStore
}

func newTransport(seed int64, f FaultProfile, m *metricStore) *transport {
	return &transport{seed: seed, faults: f, attempts: map[string]int{}, metrics: m}
}

func (t *transport) sample(key string) float64 {
	h := sha256.Sum256([]byte(fmt.Sprintf("%d/%s", t.seed, key)))
	return float64(binary.BigEndian.Uint64(h[:8])>>11) / float64(uint64(1)<<53)
}

func (t *transport) send(workerID, generation int, e ProtocolEnvelopeV1) {
	start := time.Now()
	data, err := EncodeEnvelope(e)
	encodeNS := time.Since(start).Nanoseconds()
	if err != nil {
		return
	}
	t.mu.Lock()
	attempt := t.attempts[e.MessageID]
	t.attempts[e.MessageID] = attempt + 1
	base := fmt.Sprintf("%s/%06d", e.MessageID, attempt)
	now := t.now
	t.mu.Unlock()
	t.metrics.add(func(m *Metrics) {
		m.OctagonEncodeNanoseconds += encodeNS
		if len(data) > m.OctagonBytes {
			m.OctagonBytes = len(data)
		}
		m.MessagesSent++
	})
	if t.sample(base+"/drop") < t.faults.DropRate {
		t.metrics.add(func(m *Metrics) { m.MessagesDropped++ })
		return
	}
	t.enqueue(now, workerID, generation, e.TransferID, data, base+"/0", false)
	if t.sample(base+"/duplicate") < t.faults.DuplicateRate {
		t.metrics.add(func(m *Metrics) { m.DuplicatesInjected++ })
		t.enqueue(now, workerID, generation, e.TransferID, data, base+"/1", true)
	}
}

func (t *transport) enqueue(now, workerID, generation int, transferID string, data []byte, order string, duplicate bool) {
	delay := 0
	if t.faults.MaxDelay > 0 && t.sample(order+"/delay-enabled") < .05 {
		delay = 1 + int(t.sample(order+"/delay")*float64(t.faults.MaxDelay))
	}
	reorder := 0
	if t.faults.ReorderWindow > 0 && t.sample(order+"/reorder-enabled") < .05 {
		reorder = 1 + int(t.sample(order+"/reorder")*float64(t.faults.ReorderWindow))
	}
	if delay+reorder > 0 {
		t.metrics.add(func(m *Metrics) { m.DelayedOrReordered++ })
	}
	t.mu.Lock()
	t.queue = append(t.queue, transportEvent{At: now + delay + reorder, Order: order, TransferID: transferID, WorkerID: workerID, Generation: generation, Data: append([]byte(nil), data...)})
	t.mu.Unlock()
}

func (t *transport) drain(deliver func(int, int, ProtocolEnvelopeV1) error) error {
	t.mu.Lock()
	sort.SliceStable(t.queue, func(i, j int) bool {
		if t.queue[i].At == t.queue[j].At {
			return t.queue[i].Order < t.queue[j].Order
		}
		return t.queue[i].At < t.queue[j].At
	})
	events := t.queue
	t.queue = nil
	t.mu.Unlock()
	for _, ev := range events {
		t.mu.Lock()
		t.now = ev.At
		t.mu.Unlock()
		start := time.Now()
		e, err := DecodeEnvelope(ev.Data)
		decodeNS := time.Since(start).Nanoseconds()
		if err != nil {
			return err
		}
		t.metrics.add(func(m *Metrics) { m.OctagonDecodeNanoseconds += decodeNS; m.MessagesDelivered++ })
		if err = deliver(ev.WorkerID, ev.Generation, e); err != nil {
			return err
		}
	}
	return nil
}

func (t *transport) retarget(transferID string, workerID, generation int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := range t.queue {
		if t.queue[i].TransferID == transferID {
			t.queue[i].WorkerID = workerID
			t.queue[i].Generation = generation
		}
	}
}
