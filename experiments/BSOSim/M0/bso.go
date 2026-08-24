package bsosim

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yuechen-li-dev/octetdb"
)

const stateKey = "state"

type durableBSO struct {
	id      string
	path    string
	db      *octetdb.Database
	state   *octetdb.Dataset
	metrics *Metrics
}

type messageOutcome struct {
	Send  bool          `json:"send"`
	Kind  MessageKind   `json:"kind,omitempty"`
	State TransferState `json:"state,omitempty"`
}

func openBSO(ctx context.Context, root, id string, balance Money, metrics *Metrics) (*durableBSO, error) {
	b := &durableBSO{id: id, path: filepath.Join(root, strings.ReplaceAll(id, ":", "_")), metrics: metrics}
	if err := b.open(ctx); err != nil {
		return nil, err
	}
	var existing BSOState
	found, err := b.state.Get(ctx, stateKey, &existing)
	if err != nil {
		_ = b.close()
		return nil, err
	}
	if found {
		return b, nil
	}
	initial := BSOState{ID: id, Balance: balance, Outgoing: map[string]Transfer{}, Incoming: map[string]Transfer{}, SeenMessages: map[string]bool{}, ProtocolVersion: 1}
	_, err = b.db.Mutate(ctx, octetdb.KeyedCommand{ID: "initialize/" + id}, func(tx *octetdb.Tx) (any, error) {
		return initial, tx.Put(b.state, stateKey, initial)
	})
	if err != nil {
		_ = b.close()
		return nil, err
	}
	b.metrics.LocalDurableMutations++
	return b, nil
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
	state, err := bucket.Dataset(ctx, "state", octetdb.DatasetOptions{TypeIdentity: "bso-sim.BSOState/v1"})
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
	var state BSOState
	found, err := b.state.Get(ctx, stateKey, &state)
	if err != nil {
		return state, err
	}
	if !found {
		return state, errors.New("durable BSO state is missing")
	}
	return state, nil
}

func (b *durableBSO) mutate(ctx context.Context, commandID string, fn func(*BSOState) (messageOutcome, error)) (messageOutcome, bool, error) {
	decision, err := b.db.Mutate(ctx, octetdb.KeyedCommand{ID: commandID}, func(tx *octetdb.Tx) (any, error) {
		var state BSOState
		found, err := tx.Get(b.state, stateKey, &state)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errors.New("durable BSO state is missing")
		}
		outcome, err := fn(&state)
		if err != nil {
			return nil, err
		}
		if err := tx.Put(b.state, stateKey, state); err != nil {
			return nil, err
		}
		return outcome, nil
	})
	if err != nil {
		return messageOutcome{}, false, err
	}
	var outcome messageOutcome
	if err := octetdb.DecodeResult(decision, &outcome); err != nil {
		return outcome, decision.Duplicate, err
	}
	if decision.Duplicate {
		b.metrics.DuplicatesSuppressed++
	} else {
		b.metrics.LocalDurableMutations++
	}
	return outcome, decision.Duplicate, nil
}

func (b *durableBSO) reserve(ctx context.Context, attempt Attempt, round int) (Envelope, bool, error) {
	outcome, _, err := b.mutate(ctx, "reserve/"+attempt.ID, func(state *BSOState) (messageOutcome, error) {
		if existing, ok := state.Outgoing[attempt.ID]; ok {
			return messageOutcome{Send: existing.State == StateReserved, Kind: MessageOffer, State: existing.State}, nil
		}
		if attempt.From != state.ID || attempt.To == state.ID || attempt.Amount <= 0 || state.Balance < attempt.Amount {
			state.Outgoing[attempt.ID] = Transfer{ID: attempt.ID, From: attempt.From, To: attempt.To, Amount: attempt.Amount, State: StateRejected, CreatedRound: round}
			return messageOutcome{}, nil
		}
		state.Balance -= attempt.Amount
		state.Reserved += attempt.Amount
		state.Outgoing[attempt.ID] = Transfer{ID: attempt.ID, From: attempt.From, To: attempt.To, Amount: attempt.Amount, State: StateReserved, CreatedRound: round}
		return messageOutcome{Send: true, Kind: MessageOffer, State: StateReserved}, nil
	})
	if err != nil || !outcome.Send {
		return Envelope{}, false, err
	}
	return newEnvelope(attempt.ID, attempt.From, attempt.To, attempt.Amount, MessageOffer, outcome.State), true, nil
}

func (b *durableBSO) handle(ctx context.Context, envelope Envelope) (*Envelope, error) {
	if err := validateEnvelope(envelope, b.id); err != nil {
		b.metrics.AuthenticationFailures++
		return nil, nil
	}
	outcome, _, err := b.mutate(ctx, "message/"+envelope.MessageID, func(state *BSOState) (messageOutcome, error) {
		state.SeenMessages[envelope.MessageID] = true
		switch envelope.Kind {
		case MessageOffer:
			return receiveOffer(state, envelope), nil
		case MessageAccept:
			return receiveAccept(state, envelope), nil
		case MessageReject:
			return receiveReject(state, envelope), nil
		case MessageCommit:
			return receiveCommit(state, envelope), nil
		case MessageAck:
			return receiveAck(state, envelope), nil
		case MessageReconcile:
			return receiveReconcile(state, envelope), nil
		default:
			return messageOutcome{}, nil
		}
	})
	if err != nil || !outcome.Send {
		return nil, err
	}
	response := newEnvelope(envelope.TransferID, b.id, envelope.From, envelope.Amount, outcome.Kind, outcome.State)
	return &response, nil
}

func receiveOffer(state *BSOState, e Envelope) messageOutcome {
	if e.To != state.ID || e.From == state.ID || e.Amount <= 0 {
		return messageOutcome{}
	}
	transfer, ok := state.Incoming[e.TransferID]
	if ok && (transfer.From != e.From || transfer.To != e.To || transfer.Amount != e.Amount) {
		return messageOutcome{}
	}
	if !ok {
		transfer = Transfer{ID: e.TransferID, From: e.From, To: e.To, Amount: e.Amount, State: StateAccepted}
		state.Incoming[e.TransferID] = transfer
	}
	switch transfer.State {
	case StateAccepted:
		return messageOutcome{Send: true, Kind: MessageAccept, State: StateAccepted}
	case StateCommitted:
		return messageOutcome{Send: true, Kind: MessageAck, State: StateCommitted}
	default:
		return messageOutcome{Send: true, Kind: MessageReject, State: transfer.State}
	}
}

func receiveAccept(state *BSOState, e Envelope) messageOutcome {
	transfer, ok := state.Outgoing[e.TransferID]
	if !ok || transfer.From != state.ID || transfer.To != e.From || transfer.Amount != e.Amount {
		return messageOutcome{}
	}
	if transfer.State == StateReserved || transfer.State == StateAccepted {
		transfer.State = StateCommitted
		if !transfer.DebitApplied {
			transfer.DebitApplied = true
			state.Reserved -= transfer.Amount
			state.Audit = append(state.Audit, AuditEntry{TransferID: transfer.ID, State: StateCommitted, Delta: -transfer.Amount})
		}
		state.Outgoing[e.TransferID] = transfer
	}
	if transfer.State == StateCommitted || transfer.State == StateAcknowledged {
		return messageOutcome{Send: true, Kind: MessageCommit, State: transfer.State}
	}
	return messageOutcome{Send: true, Kind: MessageReject, State: transfer.State}
}

func receiveReject(state *BSOState, e Envelope) messageOutcome {
	transfer, ok := state.Outgoing[e.TransferID]
	if !ok || transfer.To != e.From || transfer.Amount != e.Amount {
		return messageOutcome{}
	}
	if transfer.State == StateReserved || transfer.State == StateAccepted {
		state.Balance += transfer.Amount
		state.Reserved -= transfer.Amount
		transfer.State = StateRejected
		state.Outgoing[e.TransferID] = transfer
	}
	return messageOutcome{}
}

func receiveCommit(state *BSOState, e Envelope) messageOutcome {
	transfer, ok := state.Incoming[e.TransferID]
	if !ok || transfer.From != e.From || transfer.To != state.ID || transfer.Amount != e.Amount {
		return messageOutcome{}
	}
	if transfer.State == StateAccepted {
		transfer.State = StateCommitted
		if !transfer.CreditApplied {
			transfer.CreditApplied = true
			state.Balance += transfer.Amount
			state.Audit = append(state.Audit, AuditEntry{TransferID: transfer.ID, State: StateCommitted, Delta: transfer.Amount})
		}
		state.Incoming[e.TransferID] = transfer
	}
	if transfer.State == StateCommitted {
		return messageOutcome{Send: true, Kind: MessageAck, State: StateCommitted}
	}
	return messageOutcome{Send: true, Kind: MessageReject, State: transfer.State}
}

func receiveAck(state *BSOState, e Envelope) messageOutcome {
	transfer, ok := state.Outgoing[e.TransferID]
	if ok && transfer.To == e.From && transfer.Amount == e.Amount && transfer.State == StateCommitted {
		transfer.State = StateAcknowledged
		state.Outgoing[e.TransferID] = transfer
	}
	return messageOutcome{}
}

func receiveReconcile(state *BSOState, e Envelope) messageOutcome {
	transfer, ok := state.Incoming[e.TransferID]
	if !ok || transfer.From != e.From || transfer.Amount != e.Amount {
		return messageOutcome{}
	}
	if e.State == StateExpired && transfer.State == StateAccepted {
		transfer.State = StateExpired
		state.Incoming[e.TransferID] = transfer
		return messageOutcome{Send: true, Kind: MessageReject, State: StateExpired}
	}
	if transfer.State == StateCommitted {
		return messageOutcome{Send: true, Kind: MessageAck, State: StateCommitted}
	}
	if transfer.State == StateAccepted {
		return messageOutcome{Send: true, Kind: MessageAccept, State: StateAccepted}
	}
	return messageOutcome{Send: true, Kind: MessageReject, State: transfer.State}
}

func newEnvelope(transferID, from, to string, amount Money, kind MessageKind, state TransferState) Envelope {
	e := Envelope{MessageID: fmt.Sprintf("%s/%s/%s", transferID, kind, from), TransferID: transferID, From: from, To: to, Kind: kind, Amount: amount, State: state}
	e.Auth = envelopeAuth(e)
	return e
}

func envelopeAuth(e Envelope) string {
	payload := fmt.Sprintf("bso-sim-m0|%s|%s|%s|%s|%s|%d|%s", e.MessageID, e.TransferID, e.From, e.To, e.Kind, e.Amount, e.State)
	sum := sha256.Sum256([]byte(payload + "|identity-secret/" + e.From))
	return hex.EncodeToString(sum[:])
}

func validateEnvelope(e Envelope, routedTo string) error {
	if e.To != routedTo || e.From == "" || e.To == "" || e.From == e.To || e.TransferID == "" || e.MessageID == "" || e.Amount <= 0 {
		return errors.New("invalid envelope identity or route")
	}
	if envelopeAuth(e) != e.Auth {
		return errors.New("envelope authentication failed")
	}
	return nil
}
