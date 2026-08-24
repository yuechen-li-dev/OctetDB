package bsosim

import (
	"math/rand"
	"sort"
	"time"
)

type transportEvent struct {
	At    int
	Order int64
	Data  []byte
}
type transport struct {
	rng     *rand.Rand
	faults  FaultProfile
	now     int
	next    int64
	queue   []transportEvent
	metrics *Metrics
}

func newTransport(seed int64, f FaultProfile, m *Metrics) *transport {
	return &transport{rng: rand.New(rand.NewSource(seed)), faults: f, metrics: m}
}
func (t *transport) send(e ProtocolEnvelopeV1) {
	start := time.Now()
	data, err := EncodeEnvelope(e)
	t.metrics.OctagonEncodeNanoseconds += time.Since(start).Nanoseconds()
	if err != nil {
		return
	}
	if len(data) > t.metrics.OctagonBytes {
		t.metrics.OctagonBytes = len(data)
	}
	t.metrics.MessagesSent++
	if t.rng.Float64() < t.faults.DropRate {
		t.metrics.MessagesDropped++
		return
	}
	t.enqueue(data, false)
	if t.rng.Float64() < t.faults.DuplicateRate {
		t.metrics.DuplicatesInjected++
		t.enqueue(data, true)
	}
}
func (t *transport) enqueue(data []byte, dup bool) {
	delay := 0
	if t.faults.MaxDelay > 0 && t.rng.Float64() < .05 {
		delay = 1 + t.rng.Intn(t.faults.MaxDelay)
	}
	reorder := 0
	if t.faults.ReorderWindow > 0 && t.rng.Float64() < .05 {
		reorder = 1 + t.rng.Intn(t.faults.ReorderWindow)
	}
	if delay+reorder > 0 {
		t.metrics.DelayedOrReordered++
	}
	t.next++
	order := t.next
	if dup {
		order += int64(t.faults.ReorderWindow + 1)
	}
	t.queue = append(t.queue, transportEvent{At: t.now + delay + reorder, Order: order, Data: append([]byte(nil), data...)})
}
func (t *transport) drain(deliver func(ProtocolEnvelopeV1) error) error {
	sort.SliceStable(t.queue, func(i, j int) bool {
		if t.queue[i].At == t.queue[j].At {
			return t.queue[i].Order < t.queue[j].Order
		}
		return t.queue[i].At < t.queue[j].At
	})
	events := t.queue
	t.queue = nil
	for _, ev := range events {
		t.now = ev.At
		start := time.Now()
		e, err := DecodeEnvelope(ev.Data)
		t.metrics.OctagonDecodeNanoseconds += time.Since(start).Nanoseconds()
		if err != nil {
			return err
		}
		t.metrics.MessagesDelivered++
		if err = deliver(e); err != nil {
			return err
		}
	}
	return nil
}
