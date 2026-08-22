package m7write

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	generated "github.com/yuechen-li-dev/database-scheduler/internal/m7generated"
)

type queued struct {
	envelope Envelope
	result   chan outcome
}

type outcome struct {
	result Result
	err    error
}

type agentEntry struct {
	id         AccountID
	mailbox    chan queued
	machine    *generated.AccountAgent
	checkpoint []byte
	started    bool
	mu         sync.Mutex
}

type Engine struct {
	cfg               Config
	store             *Store
	locks             *lockTable
	log               *commitLog
	registryMu        sync.Mutex
	registry          map[AccountID]*agentEntry
	commitMu          sync.Mutex
	results           map[string]Result
	sequence          atomic.Uint64
	closed            atomic.Bool
	recoveryTruncated bool
}

func Open(cfg Config) (*Engine, error) {
	if cfg.MailboxCapacity <= 0 {
		cfg.MailboxCapacity = 64
	}
	log, records, truncated, err := openCommitLog(cfg.LogPath, cfg.Durability, cfg.BatchSize)
	if err != nil {
		return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: err}
	}
	e := &Engine{cfg: cfg, store: NewStore(), locks: newLockTable(), log: log, registry: make(map[AccountID]*agentEntry), results: make(map[string]Result), recoveryTruncated: truncated}
	for _, record := range records {
		if record.Sequence <= e.sequence.Load() {
			log.close()
			return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: fmt.Errorf("non-monotonic sequence %d", record.Sequence)}
		}
		checkpoint, err := generated.ParseAccountAgentCheckpoint(record.Checkpoint)
		if err != nil {
			log.close()
			return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
		}
		machine, err := generated.RestoreAccountAgent(checkpoint)
		if err != nil {
			log.close()
			return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
		}
		e.store.apply(record)
		entry := e.entry(record.AgentID)
		entry.machine = machine
		entry.checkpoint = append(entry.checkpoint[:0], record.Checkpoint...)
		e.results[record.CommandID] = record.Result
		e.sequence.Store(record.Sequence)
	}
	return e, nil
}

func (e *Engine) Store() *Store               { return e.store }
func (e *Engine) RecoveryTruncatedTail() bool { return e.recoveryTruncated }
func (e *Engine) AgentCount() int {
	e.registryMu.Lock()
	defer e.registryMu.Unlock()
	return len(e.registry)
}

func (e *Engine) entry(id AccountID) *agentEntry {
	e.registryMu.Lock()
	defer e.registryMu.Unlock()
	entry := e.registry[id]
	if entry == nil {
		entry = &agentEntry{id: id, mailbox: make(chan queued, e.cfg.MailboxCapacity)}
		e.registry[id] = entry
	}
	return entry
}

func (e *Engine) Submit(ctx context.Context, command Command) (Result, error) {
	if err := ValidateCommand(command); err != nil {
		return Result{}, err
	}
	if e.closed.Load() {
		return Result{}, &RuntimeError{Kind: EngineClosed, Err: errClosed}
	}
	entry := e.entry(command.Account)
	entry.mu.Lock()
	if !entry.started {
		entry.started = true
		go e.runAgent(entry)
	}
	entry.mu.Unlock()
	response := make(chan outcome, 1)
	item := queued{envelope: Envelope{Command: command, Submitted: time.Now()}, result: response}
	select {
	case entry.mailbox <- item:
	default:
		return Result{}, &RuntimeError{Kind: MailboxFull, Err: errors.New("bounded agent mailbox is full")}
	}
	select {
	case got := <-response:
		return got.result, got.err
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}

func (e *Engine) runAgent(entry *agentEntry) {
	for item := range entry.mailbox {
		item.result <- e.process(entry, item.envelope)
	}
}

func (e *Engine) process(entry *agentEntry, envelope Envelope) outcome {
	command := envelope.Command
	tokens := tokensFor(command)
	e.trace(TraceEvent{CommandID: command.ID, Agent: command.Account, Phase: "mailbox_dequeue", ConflictTokens: tokens})
	release := e.locks.acquire(tokens)
	defer release()
	e.trace(TraceEvent{CommandID: command.ID, Agent: command.Account, Phase: "ownership_acquired", ConflictTokens: tokens})
	e.commitMu.Lock()
	if prior, ok := e.results[command.ID]; ok {
		e.commitMu.Unlock()
		prior.Duplicate = true
		return outcome{result: prior}
	}
	e.commitMu.Unlock()
	a, aok, b, bok := e.store.view(command.Account, command.Other)
	e.trace(TraceEvent{CommandID: command.ID, Agent: command.Account, Phase: "state_view", VersionA: a.Version, VersionB: b.Version})
	if entry.machine == nil {
		entry.machine = generated.NewAccountAgent(int(command.Account))
	}
	machine := entry.machine
	rollback := func() error {
		if len(entry.checkpoint) == 0 {
			entry.machine = generated.NewAccountAgent(int(command.Account))
			return nil
		}
		checkpoint, err := generated.ParseAccountAgentCheckpoint(entry.checkpoint)
		if err != nil {
			return err
		}
		restored, err := generated.RestoreAccountAgent(checkpoint)
		if err != nil {
			return err
		}
		entry.machine = restored
		return nil
	}
	turn, err := machine.Step(toContext(command, a, aok, b, bok))
	if err != nil {
		_ = rollback()
		return outcome{err: &RuntimeError{Kind: FlowStepFailed, Err: err}}
	}
	decision, err := turn.Yielded()
	if err != nil {
		_ = rollback()
		return outcome{err: &RuntimeError{Kind: FlowStepFailed, Err: err}}
	}
	if err := validateDecision(command, a, aok, b, bok, decision); err != nil {
		_ = rollback()
		return outcome{err: &RuntimeError{Kind: InvalidDecision, Err: err}}
	}
	e.trace(TraceEvent{CommandID: command.ID, Agent: command.Account, Phase: "flow_yield", VersionA: a.Version, VersionB: b.Version, Accepted: decision.Accepted, ReasonTag: decision.Reason.Tag, EffectTag: decision.Effect.Tag, FlowState: turn.Active()})
	checkpoint, err := machine.Checkpoint()
	if err != nil {
		_ = rollback()
		return outcome{err: &RuntimeError{Kind: CheckpointExportFailed, Err: err}}
	}
	cpBytes := checkpoint.Bytes()
	e.commitMu.Lock()
	defer e.commitMu.Unlock()
	if prior, ok := e.results[command.ID]; ok {
		_ = rollback()
		prior.Duplicate = true
		return outcome{result: prior}
	}
	sequence := e.sequence.Add(1)
	result := Result{Sequence: sequence, CommandID: command.ID, Accepted: decision.Accepted, ReasonTag: decision.Reason.Tag, EffectTag: decision.Effect.Tag, TransitionCount: decision.TransitionCount}
	record := logRecord{Version: 1, Sequence: sequence, AgentID: command.Account, CommandID: command.ID, CommandKind: command.Kind.Tag, AccountA: command.Account, AccountB: command.Other, Amount: command.Amount, Result: result, EffectTag: decision.Effect.Tag, NewBalanceA: decision.NewBalanceA, NewBalanceB: decision.NewBalanceB, NewStatusTag: decision.NewStatus.Tag, ExpectedA: uint64(decision.ExpectedVersionA), ExpectedB: uint64(decision.ExpectedVersionB), Checkpoint: cpBytes}
	e.trace(TraceEvent{Sequence: sequence, CommandID: command.ID, Agent: command.Account, Phase: "wal_append", Accepted: decision.Accepted, ReasonTag: decision.Reason.Tag, EffectTag: decision.Effect.Tag, Detail: fmt.Sprintf("record_version=1 checkpoint_bytes=%d", len(cpBytes))})
	if err := e.log.append(record); err != nil {
		if rollbackErr := rollback(); rollbackErr != nil {
			return outcome{err: &RuntimeError{Kind: RecoveryIncompatible, Err: rollbackErr}}
		}
		return outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
	}
	e.store.apply(record)
	entry.checkpoint = append(entry.checkpoint[:0], cpBytes...)
	e.results[command.ID] = result
	hash := sha256.Sum256(cpBytes)
	newA, _, newB, _ := e.store.view(command.Account, command.Other)
	e.trace(TraceEvent{Sequence: sequence, CommandID: command.ID, Agent: command.Account, Phase: "committed", VersionA: uint64(decision.ExpectedVersionA), VersionB: uint64(decision.ExpectedVersionB), NewVersionA: newA.Version, NewVersionB: newB.Version, Accepted: decision.Accepted, ReasonTag: decision.Reason.Tag, EffectTag: decision.Effect.Tag, CheckpointHash: hex.EncodeToString(hash[:])})
	return outcome{result: result}
}

func toContext(command Command, a Account, aok bool, b Account, bok bool) generated.Main_CommandContext {
	return generated.Main_CommandContext{Kind: command.Kind, AccountA: int(command.Account), AccountB: int(command.Other), Amount: command.Amount, ExistsA: aok, ExistsB: bok, BalanceA: a.Balance, BalanceB: b.Balance, StatusA: statusToGenerated(a, aok), StatusB: statusToGenerated(b, bok), VersionA: int(a.Version), VersionB: int(b.Version)}
}

func statusToGenerated(account Account, exists bool) generated.Main_AccountStatus {
	if !exists {
		return generated.NewAccountStatusMissing()
	}
	if account.Status == StatusFrozen {
		return generated.NewAccountStatusFrozen()
	}
	return generated.NewAccountStatusOpen()
}

func validateDecision(command Command, a Account, aok bool, b Account, bok bool, d generated.Main_TransitionDecision) error {
	if d.AccountA != int(command.Account) || d.AccountB != int(command.Other) || d.Amount != command.Amount {
		return errors.New("decision identity differs from input")
	}
	if d.ExpectedVersionA != int(a.Version) || d.ExpectedVersionB != int(b.Version) {
		return errors.New("stale expected version")
	}
	if d.Accepted && d.NewBalanceA < 0 {
		return errors.New("negative accepted balance")
	}
	if d.Accepted && d.Effect.Tag == generated.NewEffectKindTransfer().Tag && (!aok || !bok) {
		return errors.New("transfer effect references missing account")
	}
	return nil
}

func (e *Engine) trace(event TraceEvent) {
	if e.cfg.Trace != nil {
		e.cfg.Trace(event)
	}
}
func (e *Engine) Flush() error                      { return e.log.flush() }
func (e *Engine) InjectDurabilityFailure(err error) { e.log.injectFailure(err) }
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	return e.log.close()
}
