package bsosim

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

type workItem struct {
	transferID string
	envelope   *ProtocolEnvelopeV1
	generation int
}

type workerRound struct {
	round int
	done  chan<- workerResult
}
type workerResult struct {
	workerID int
	err      error
}

// SchedulerWorker owns its queue and agent map. Only its single run goroutine
// steps agents; the mutex is solely for admission and round-barrier delivery.
type SchedulerWorker struct {
	ID          int
	Alive       bool
	mu          sync.Mutex
	queue       []workItem
	queued      map[string]bool
	agents      map[string]*TransactionAgent
	Steps, Peak int
	QueuePeak   int
	rounds      chan workerRound
	stop        chan struct{}
	done        chan struct{}
	once        sync.Once
	sim         *simulation
}

func newWorker(id int) *SchedulerWorker {
	return &SchedulerWorker{ID: id, Alive: true, queued: map[string]bool{}, agents: map[string]*TransactionAgent{}, rounds: make(chan workerRound), stop: make(chan struct{}), done: make(chan struct{})}
}
func (w *SchedulerWorker) start(s *simulation) {
	w.sim = s
	s.metrics.add(func(m *Metrics) { m.WorkerGoroutinesStarted++ })
	go func() {
		defer close(w.done)
		defer s.metrics.add(func(m *Metrics) { m.WorkerGoroutinesStopped++ })
		for {
			select {
			case task := <-w.rounds:
				task.done <- workerResult{workerID: w.ID, err: w.runRound(task.round)}
			case <-w.stop:
				return
			}
		}
	}()
}
func (w *SchedulerWorker) shutdown() { w.once.Do(func() { close(w.stop) }); <-w.done }
func (w *SchedulerWorker) enqueue(id string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.enqueueLocked(workItem{transferID: id})
}
func (w *SchedulerWorker) enqueueEnvelope(e ProtocolEnvelopeV1, generation int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	copy := e
	w.queue = append(w.queue, workItem{transferID: e.TransferID, envelope: &copy, generation: generation})
	if len(w.queue) > w.QueuePeak {
		w.QueuePeak = len(w.queue)
	}
}
func (w *SchedulerWorker) enqueueLocked(item workItem) {
	if !w.Alive || w.queued[item.transferID] {
		return
	}
	w.queue = append(w.queue, item)
	if len(w.queue) > w.QueuePeak {
		w.QueuePeak = len(w.queue)
	}
	w.queued[item.transferID] = true
	if len(w.agents) > w.Peak {
		w.Peak = len(w.agents)
	}
}
func (w *SchedulerWorker) runRound(round int) error {
	w.mu.Lock()
	if !w.Alive {
		w.mu.Unlock()
		return nil
	}
	items := append([]workItem(nil), w.queue...)
	w.queue = nil
	for _, item := range items {
		if item.envelope == nil {
			delete(w.queued, item.transferID)
		}
	}
	w.mu.Unlock()
	if len(items) == 0 {
		return nil
	}
	w.sim.metrics.add(func(m *Metrics) {
		m.WorkerActiveCurrent++
		if m.WorkerActiveCurrent > m.WorkerActivePeak {
			m.WorkerActivePeak = m.WorkerActiveCurrent
		}
	})
	defer w.sim.metrics.add(func(m *Metrics) { m.WorkerActiveCurrent-- })
	for _, item := range items {
		w.mu.Lock()
		a := w.agents[item.transferID]
		w.mu.Unlock()
		if a == nil || a.Phase.terminal() {
			continue
		}
		if item.envelope != nil {
			if item.generation != a.PlacementGeneration {
				return fmt.Errorf("worker %d received stale generation %d for %s at generation %d", w.ID, item.generation, a.TransferID, a.PlacementGeneration)
			}
			if err := w.sim.deliver(w, a, *item.envelope, round); err != nil {
				return err
			}
			continue
		}
		w.Steps++
		w.sim.metrics.add(func(m *Metrics) { m.AgentSteps++ })
		if err := w.sim.step(w, a, round); err != nil {
			return err
		}
		if a.Phase.terminal() {
			w.sim.coordinator.complete(w.ID, a.TransferID)
		} else if a.Phase != PhaseAwaitAccept && a.Phase != PhaseAwaitAcknowledge {
			w.enqueue(a.TransferID)
		}
	}
	return nil
}

type SchedulerCoordinator struct {
	mu        sync.Mutex
	workers   []*SchedulerWorker
	placement map[string]int
	metrics   *metricStore
	transport *transport
}

func newCoordinator(n int, m *metricStore) *SchedulerCoordinator {
	c := &SchedulerCoordinator{placement: map[string]int{}, metrics: m}
	for i := 0; i < n; i++ {
		c.workers = append(c.workers, newWorker(i))
	}
	return c
}
func (c *SchedulerCoordinator) start(s *simulation) {
	for _, w := range c.workers {
		w.start(s)
	}
}
func (c *SchedulerCoordinator) assign(a *TransactionAgent) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	best, load := -1, int(^uint(0)>>1)
	for _, w := range c.workers {
		w.mu.Lock()
		alive, n := w.Alive, len(w.agents)
		w.mu.Unlock()
		if alive && (n < load || n == load && (best < 0 || w.ID < best)) {
			best, load = w.ID, n
		}
	}
	if best < 0 {
		return fmt.Errorf("no live scheduler worker")
	}
	a.PlacementGeneration++
	w := c.workers[best]
	w.mu.Lock()
	w.agents[a.TransferID] = a
	w.enqueueLocked(workItem{transferID: a.TransferID})
	w.mu.Unlock()
	c.placement[a.TransferID] = best
	c.metrics.add(func(m *Metrics) { m.NewAgentPlacements++; m.CoordinatorOps++ })
	return nil
}
func (c *SchedulerCoordinator) kill(id int) error {
	c.mu.Lock()
	if id < 0 || id >= len(c.workers) {
		c.mu.Unlock()
		return nil
	}
	dead := c.workers[id]
	dead.mu.Lock()
	if !dead.Alive {
		dead.mu.Unlock()
		c.mu.Unlock()
		return nil
	}
	dead.Alive = false
	ids := make([]string, 0, len(dead.agents))
	checkpoints := make(map[string][]byte, len(dead.agents))
	for transferID, a := range dead.agents {
		checkpoint, err := EncodeCheckpoint(*a)
		if err != nil {
			dead.mu.Unlock()
			c.mu.Unlock()
			return err
		}
		ids = append(ids, transferID)
		checkpoints[transferID] = checkpoint
		delete(c.placement, transferID)
	}
	dead.queue = nil
	dead.agents = map[string]*TransactionAgent{}
	dead.queued = map[string]bool{}
	dead.mu.Unlock()
	c.mu.Unlock()
	sort.Strings(ids)
	dead.shutdown()
	c.metrics.add(func(m *Metrics) { m.WorkerFailures++; m.CoordinatorOps++; m.AgentsAffected += len(ids) })
	for _, transferID := range ids {
		restored, err := DecodeCheckpoint(checkpoints[transferID])
		if err != nil {
			return err
		}
		if err = c.assign(&restored); err != nil {
			return err
		}
		_, newWorker, ok := c.agent(transferID)
		if !ok {
			return fmt.Errorf("migrated agent %s missing", transferID)
		}
		c.transport.retarget(transferID, newWorker, restored.PlacementGeneration)
		c.metrics.add(func(m *Metrics) { m.AgentsMigrated++; m.CoordinatorOps++ })
	}
	return nil
}
func (c *SchedulerCoordinator) complete(worker int, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if current, ok := c.placement[id]; !ok || current != worker {
		return
	}
	w := c.workers[worker]
	w.mu.Lock()
	delete(w.agents, id)
	w.mu.Unlock()
	delete(c.placement, id)
}
func (c *SchedulerCoordinator) agent(id string) (*TransactionAgent, int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	wid, ok := c.placement[id]
	if !ok {
		return nil, 0, false
	}
	w := c.workers[wid]
	w.mu.Lock()
	a, ok := w.agents[id]
	w.mu.Unlock()
	return a, wid, ok
}
func (c *SchedulerCoordinator) shutdown() {
	for _, w := range c.workers {
		w.mu.Lock()
		alive := w.Alive
		w.mu.Unlock()
		if alive {
			w.shutdown()
		}
	}
}
func sendRound(ctx context.Context, w *SchedulerWorker, round int, done chan<- workerResult) error {
	select {
	case w.rounds <- workerRound{round: round, done: done}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
