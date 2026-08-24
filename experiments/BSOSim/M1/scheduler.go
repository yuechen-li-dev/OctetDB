package bsosim

import "fmt"

type SchedulerWorker struct {
	ID          int
	Alive       bool
	queue       []string
	queued      map[string]bool
	agents      map[string]*TransactionAgent
	Steps, Peak int
}

func newWorker(id int) *SchedulerWorker {
	return &SchedulerWorker{ID: id, Alive: true, queued: map[string]bool{}, agents: map[string]*TransactionAgent{}}
}
func (w *SchedulerWorker) enqueue(id string) {
	if !w.Alive || w.queued[id] {
		return
	}
	w.queue = append(w.queue, id)
	w.queued[id] = true
	if len(w.agents) > w.Peak {
		w.Peak = len(w.agents)
	}
}
func (w *SchedulerWorker) pop() (string, bool) {
	if !w.Alive || len(w.queue) == 0 {
		return "", false
	}
	id := w.queue[0]
	w.queue = w.queue[1:]
	delete(w.queued, id)
	return id, true
}

type SchedulerCoordinator struct {
	workers   []*SchedulerWorker
	placement map[string]int
	metrics   *Metrics
}

func newCoordinator(n int, m *Metrics) *SchedulerCoordinator {
	c := &SchedulerCoordinator{placement: map[string]int{}, metrics: m}
	for i := 0; i < n; i++ {
		c.workers = append(c.workers, newWorker(i))
	}
	return c
}
func (c *SchedulerCoordinator) assign(a *TransactionAgent) error {
	best := -1
	load := int(^uint(0) >> 1)
	for _, w := range c.workers {
		if w.Alive && (len(w.agents) < load || len(w.agents) == load && w.ID < best) {
			best, load = w.ID, len(w.agents)
		}
	}
	if best < 0 {
		return fmt.Errorf("no live scheduler worker")
	}
	a.PlacementGeneration++
	w := c.workers[best]
	w.agents[a.TransferID] = a
	w.enqueue(a.TransferID)
	c.placement[a.TransferID] = best
	c.metrics.NewAgentPlacements++
	c.metrics.CoordinatorOps++
	return nil
}
func (c *SchedulerCoordinator) kill(id int) error {
	if id < 0 || id >= len(c.workers) || !c.workers[id].Alive {
		return nil
	}
	dead := c.workers[id]
	dead.Alive = false
	c.metrics.WorkerFailures++
	c.metrics.CoordinatorOps++
	ids := append([]string(nil), dead.queue...)
	for transferID := range dead.agents {
		found := false
		for _, x := range ids {
			if x == transferID {
				found = true
				break
			}
		}
		if !found {
			ids = append(ids, transferID)
		}
	}
	c.metrics.AgentsAffected += len(ids)
	for _, transferID := range ids {
		a := dead.agents[transferID]
		checkpoint, err := EncodeCheckpoint(*a)
		if err != nil {
			return err
		}
		restored, err := DecodeCheckpoint(checkpoint)
		if err != nil {
			return err
		}
		delete(c.placement, transferID)
		if err = c.assign(&restored); err != nil {
			return err
		}
		c.metrics.AgentsMigrated++
		c.metrics.CoordinatorOps++
	}
	dead.queue = nil
	dead.agents = map[string]*TransactionAgent{}
	dead.queued = map[string]bool{}
	return nil
}
func (c *SchedulerCoordinator) complete(worker int, id string) {
	delete(c.workers[worker].agents, id)
	delete(c.placement, id)
}
func (c *SchedulerCoordinator) agent(id string) (*TransactionAgent, int, bool) {
	wid, ok := c.placement[id]
	if !ok {
		return nil, 0, false
	}
	a, ok := c.workers[wid].agents[id]
	return a, wid, ok
}
