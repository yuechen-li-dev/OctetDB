package bsosim

import (
	"math/rand"
	"sort"
)

type transportEvent struct {
	At       int
	Order    int64
	Envelope Envelope
}

type transport struct {
	rng     *rand.Rand
	faults  FaultProfile
	now     int
	next    int64
	queue   []transportEvent
	metrics *Metrics
}

func newTransport(seed int64, faults FaultProfile, metrics *Metrics) *transport {
	return &transport{rng: rand.New(rand.NewSource(seed)), faults: faults, metrics: metrics}
}

func (t *transport) send(e Envelope) {
	t.metrics.MessagesSent++
	if t.rng.Float64() < t.faults.DropRate {
		t.metrics.MessagesDropped++
		return
	}
	t.enqueue(e, false)
	if t.rng.Float64() < t.faults.DuplicateRate {
		t.metrics.DuplicatesInjected++
		t.enqueue(e, true)
	}
}

func (t *transport) enqueue(e Envelope, duplicate bool) {
	delay := 0
	if t.faults.MaxDelay > 0 && t.rng.Float64() < 0.05 {
		delay = 1 + t.rng.Intn(t.faults.MaxDelay)
	}
	reorder := 0
	if t.faults.ReorderWindow > 0 && t.rng.Float64() < 0.05 {
		reorder = 1 + t.rng.Intn(t.faults.ReorderWindow)
	}
	if delay+reorder > 0 {
		t.metrics.DelayedOrReordered++
	}
	t.next++
	order := t.next
	if duplicate {
		order += int64(t.faults.ReorderWindow + 1)
	}
	t.queue = append(t.queue, transportEvent{At: t.now + delay + reorder, Order: order, Envelope: e})
}

func (t *transport) drain(deliver func(Envelope) error) error {
	for len(t.queue) > 0 {
		sort.SliceStable(t.queue, func(i, j int) bool {
			if t.queue[i].At == t.queue[j].At {
				return t.queue[i].Order < t.queue[j].Order
			}
			return t.queue[i].At < t.queue[j].At
		})
		event := t.queue[0]
		t.queue = t.queue[1:]
		t.now = event.At
		t.metrics.MessagesDelivered++
		if err := deliver(event.Envelope); err != nil {
			return err
		}
	}
	return nil
}
