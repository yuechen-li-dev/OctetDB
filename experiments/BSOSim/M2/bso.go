package bsosim

import (
	"context"
	"errors"
	"path/filepath"
	"strings"

	"github.com/yuechen-li-dev/octetdb"
)

const stateKey = "state"

type durableBSO struct {
	id, path string
	db       *octetdb.Database
	state    *octetdb.Dataset
	metrics  *metricStore
}
type mutationResult struct {
	State TransferState `json:"state"`
	Send  bool          `json:"send"`
}

func openBSO(ctx context.Context, root, id string, balance Money, m *metricStore) (*durableBSO, error) {
	b := &durableBSO{id: id, path: filepath.Join(root, strings.ReplaceAll(id, ":", "_")), metrics: m}
	if err := b.open(ctx); err != nil {
		return nil, err
	}
	var old BSOState
	found, err := b.state.Get(ctx, stateKey, &old)
	if err != nil {
		return nil, err
	}
	if found {
		return b, nil
	}
	initial := BSOState{ID: id, Balance: balance, Outgoing: map[string]Transfer{}, Incoming: map[string]Transfer{}, ProtocolVersion: 1}
	_, err = b.db.Mutate(ctx, octetdb.KeyedCommand{ID: "initialize/" + id}, func(tx *octetdb.Tx) (any, error) { return initial, tx.Put(b.state, stateKey, initial) })
	if err == nil {
		m.add(func(metrics *Metrics) { metrics.LocalDurableMutations++; metrics.BSOMutations++ })
	}
	return b, err
}
func (b *durableBSO) open(ctx context.Context) error {
	db, err := octetdb.OpenCatalog(ctx, b.path, octetdb.DefaultKeyedOptions())
	if err != nil {
		return err
	}
	bucket, err := db.Bucket(ctx, "bso")
	if err != nil {
		_ = db.Close()
		return err
	}
	state, err := bucket.Dataset(ctx, "state", octetdb.DatasetOptions{TypeIdentity: "bso-sim-m2.BSOState/v1"})
	if err != nil {
		_ = db.Close()
		return err
	}
	b.db, b.state = db, state
	return nil
}
func (b *durableBSO) close() error {
	if b.db == nil {
		return nil
	}
	err := b.db.Close()
	b.db, b.state = nil, nil
	return err
}
func (b *durableBSO) restart(ctx context.Context) error {
	if err := b.close(); err != nil {
		return err
	}
	return b.open(ctx)
}
func (b *durableBSO) load(ctx context.Context) (BSOState, error) {
	var s BSOState
	ok, err := b.state.Get(ctx, stateKey, &s)
	if err == nil && !ok {
		err = errors.New("durable BSO state missing")
	}
	return s, err
}
func (b *durableBSO) mutate(ctx context.Context, id string, fn func(*BSOState) mutationResult) (mutationResult, error) {
	d, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: id}, func(tx *octetdb.Tx) (any, error) {
		var s BSOState
		ok, e := tx.Get(b.state, stateKey, &s)
		if e != nil || !ok {
			return nil, e
		}
		r := fn(&s)
		return r, tx.Put(b.state, stateKey, s)
	})
	if err != nil {
		return mutationResult{}, err
	}
	var r mutationResult
	if err = octetdb.DecodeResult(d, &r); err != nil {
		return r, err
	}
	if d.Duplicate {
		b.metrics.add(func(m *Metrics) { m.DuplicatesSuppressed++; m.BSOMutations++ })
	} else {
		b.metrics.add(func(m *Metrics) { m.LocalDurableMutations++; m.BSOMutations++ })
	}
	return r, nil
}

func (b *durableBSO) reserve(ctx context.Context, a Attempt, round int) (TransferState, error) {
	r, err := b.mutate(ctx, "reserve/"+a.ID, func(s *BSOState) mutationResult {
		if t, ok := s.Outgoing[a.ID]; ok {
			return mutationResult{State: t.State}
		}
		t := Transfer{ID: a.ID, From: a.From, To: a.To, Amount: a.Amount, CreatedRound: round}
		if a.From != s.ID || a.To == s.ID || a.Amount <= 0 || s.Balance < a.Amount {
			t.State = StateRejected
			s.Outgoing[a.ID] = t
			return mutationResult{State: t.State}
		}
		s.Balance -= a.Amount
		s.Reserved += a.Amount
		t.State = StateReserved
		s.Outgoing[a.ID] = t
		return mutationResult{State: t.State}
	})
	return r.State, err
}
func (b *durableBSO) offer(ctx context.Context, e ProtocolEnvelopeV1) (TransferState, error) {
	r, err := b.mutate(ctx, "message/"+e.MessageID, func(s *BSOState) mutationResult {
		t, ok := s.Incoming[e.TransferID]
		if ok && (t.From != e.From || t.Amount != e.Amount) {
			return mutationResult{State: StateRejected}
		}
		if !ok {
			t = Transfer{ID: e.TransferID, From: e.From, To: e.To, Amount: e.Amount, State: StateAccepted}
			s.Incoming[e.TransferID] = t
		}
		return mutationResult{State: t.State}
	})
	return r.State, err
}
func (b *durableBSO) commitSender(ctx context.Context, e ProtocolEnvelopeV1) (TransferState, error) {
	r, err := b.mutate(ctx, "message/"+e.MessageID, func(s *BSOState) mutationResult {
		t, ok := s.Outgoing[e.TransferID]
		if !ok || t.To != e.From || t.Amount != e.Amount {
			return mutationResult{State: StateRejected}
		}
		if t.State == StateReserved || t.State == StateAccepted {
			t.State = StateCommitted
			if !t.DebitApplied {
				t.DebitApplied = true
				s.Reserved -= t.Amount
				s.Audit = append(s.Audit, AuditEntry{TransferID: t.ID, State: StateCommitted, Delta: -t.Amount})
			}
			s.Outgoing[t.ID] = t
		}
		return mutationResult{State: t.State}
	})
	return r.State, err
}
func (b *durableBSO) commitReceiver(ctx context.Context, e ProtocolEnvelopeV1) (TransferState, error) {
	r, err := b.mutate(ctx, "message/"+e.MessageID, func(s *BSOState) mutationResult {
		t, ok := s.Incoming[e.TransferID]
		if !ok || t.From != e.From || t.Amount != e.Amount {
			return mutationResult{State: StateRejected}
		}
		if t.State == StateAccepted {
			t.State = StateCommitted
			if !t.CreditApplied {
				t.CreditApplied = true
				s.Balance += t.Amount
				s.Audit = append(s.Audit, AuditEntry{TransferID: t.ID, State: StateCommitted, Delta: t.Amount})
			}
			s.Incoming[t.ID] = t
		}
		return mutationResult{State: t.State}
	})
	return r.State, err
}
func (b *durableBSO) ackSender(ctx context.Context, e ProtocolEnvelopeV1) (TransferState, error) {
	r, err := b.mutate(ctx, "message/"+e.MessageID, func(s *BSOState) mutationResult {
		t, ok := s.Outgoing[e.TransferID]
		if ok && t.To == e.From && t.Amount == e.Amount && t.State == StateCommitted {
			t.State = StateAcknowledged
			s.Outgoing[t.ID] = t
		}
		return mutationResult{State: t.State}
	})
	return r.State, err
}
func (b *durableBSO) expire(ctx context.Context, id string) (TransferState, error) {
	r, err := b.mutate(ctx, "expire/"+id, func(s *BSOState) mutationResult {
		t := s.Outgoing[id]
		if t.State == StateReserved || t.State == StateAccepted {
			s.Balance += t.Amount
			s.Reserved -= t.Amount
			t.State = StateExpired
			s.Outgoing[id] = t
		}
		return mutationResult{State: t.State}
	})
	return r.State, err
}
func (b *durableBSO) pending(ctx context.Context) ([]string, error) {
	s, err := b.load(ctx)
	if err != nil {
		return nil, err
	}
	ids := []string{}
	for id, t := range s.Outgoing {
		if t.State != StateAcknowledged && t.State != StateRejected && t.State != StateExpired {
			ids = append(ids, id)
		}
	}
	for id, t := range s.Incoming {
		if t.State == StateAccepted {
			ids = append(ids, id)
		}
	}
	return ids, nil
}
