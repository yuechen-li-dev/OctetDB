package m7write

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
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
	machine    *generated.DurableAccountAgent
	checkpoint []byte
	started    bool
	mu         sync.Mutex
	goPending  pendingTransfer
	goTurns    int
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
	m1                *m1Storage
	authority         chan any
	recoveryStats     RecoveryStats
	dedupeOrder       []string
	dedupeHead        int
	poisoned          error
}

type m1Commit struct {
	record     logRecord
	entry      *agentEntry
	checkpoint []byte
	response   chan outcome
	install    func()
}

type snapshotRequest struct{ response chan snapshotOutcome }
type snapshotOutcome struct {
	path  string
	bytes int64
	err   error
}
type closeAuthority struct{ response chan error }

func Open(cfg Config) (*Engine, error) {
	if cfg.MailboxCapacity <= 0 {
		cfg.MailboxCapacity = 64
	}
	if cfg.StorageDir != "" {
		return openM1Engine(cfg)
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
		machine, err := generated.RestoreDurableAccountAgent(checkpoint)
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

func openM1Engine(cfg Config) (*Engine, error) {
	started := time.Now()
	if cfg.MailboxCapacity <= 0 {
		cfg.MailboxCapacity = 64
	}
	if cfg.GroupMax <= 0 {
		cfg.GroupMax = 16
	}
	if cfg.GroupWait < 0 {
		cfg.GroupWait = 200 * time.Microsecond
	}
	if cfg.DedupeWindow <= 0 {
		cfg.DedupeWindow = 100_000
	}
	e := &Engine{cfg: cfg, store: NewStore(), locks: newLockTable(), registry: make(map[AccountID]*agentEntry), results: make(map[string]Result), authority: make(chan any, cfg.GroupMax*2+16)}
	snapshot, snapshotBytes, decodeTime, err := loadLatestSnapshot(cfg.StorageDir, expectedProgramID(cfg))
	if err != nil {
		return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: err}
	}
	var frontier uint64
	if snapshot != nil {
		frontier = snapshot.Sequence
		e.store.restoreState(snapshot.Accounts, snapshot.Ledger)
		publication, hash := e.store.CanonicalOctagon()
		if string(publication) != string(snapshot.Octagon) || hash != snapshot.OctHash {
			return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: errors.New("snapshot authoritative state/publication mismatch")}
		}
		dedupeStarted := time.Now()
		dedupe := snapshot.Dedupe
		if snapshot.DedupeFormat == "compact-v1" {
			decoded, _, err := decodeCompactDedupe(snapshot.DedupeCompact, cfg.DedupeWindow)
			if err != nil {
				return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: err}
			}
			dedupe = decoded
		} else if snapshot.DedupeFormat != "json-v1" {
			return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: fmt.Errorf("unknown dedupe format %q", snapshot.DedupeFormat)}
		}
		if err := validateDedupeEntries(dedupe, snapshot.DedupeHorizon, cfg.DedupeWindow, snapshot.Sequence); err != nil {
			return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: err}
		}
		for _, item := range dedupe {
			e.results[item.CommandID] = item.Result
			e.dedupeOrder = append(e.dedupeOrder, item.CommandID)
		}
		e.recoveryStats.DedupeDecode = time.Since(dedupeStarted)
		agentStarted := time.Now()
		for _, agent := range snapshot.Agents {
			if cfg.GoBehavioralControl {
				var state goAgentCheckpoint
				if err := json.Unmarshal(agent.Checkpoint, &state); err != nil {
					return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
				}
				entry := e.entry(agent.ID)
				entry.goPending, entry.goTurns = state.Pending, state.Turns
				entry.checkpoint = append([]byte(nil), agent.Checkpoint...)
				continue
			}
			checkpoint, err := generated.ParseAccountAgentCheckpoint(agent.Checkpoint)
			if err != nil {
				return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
			}
			machine, err := generated.RestoreDurableAccountAgent(checkpoint)
			if err != nil {
				return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
			}
			entry := e.entry(agent.ID)
			entry.machine = machine
			entry.checkpoint = append([]byte(nil), agent.Checkpoint...)
		}
		e.recoveryStats.AgentRestore = time.Since(agentStarted)
		e.sequence.Store(frontier)
		e.recoveryStats.SnapshotSequence = frontier
		e.recoveryStats.SnapshotBytes = snapshotBytes
		e.recoveryStats.SnapshotDecode = decodeTime
		e.recoveryStats.AgentsRestored = len(snapshot.Agents)
	}
	storage, records, stats, err := openM1Storage(cfg, frontier)
	if err != nil {
		return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: err}
	}
	e.m1 = storage
	e.m1.stats.DedupeDecodeNanos = uint64(e.recoveryStats.DedupeDecode)
	e.recoveryStats.WALScan = stats.WALScan
	e.recoveryStats.WALBytesScanned = stats.WALBytesScanned
	e.recoveryStats.RecordsReplayed = len(records)
	for _, record := range records {
		if record.Sequence != e.sequence.Load()+1 {
			storage.close()
			return nil, &RuntimeError{Kind: RecoveryCorrupt, Err: fmt.Errorf("non-monotonic sequence %d", record.Sequence)}
		}
		entry := e.entry(record.AgentID)
		deltaStarted := time.Now()
		if cfg.GoBehavioralControl {
			var state goAgentCheckpoint
			if err := json.Unmarshal(record.Checkpoint, &state); err != nil {
				storage.close()
				return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
			}
			entry.goPending, entry.goTurns = state.Pending, state.Turns
		} else {
			checkpoint, err := recoverAccountAgentCheckpoint(entry, record)
			if err != nil {
				storage.close()
				return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
			}
			machine, err := generated.RestoreDurableAccountAgent(checkpoint)
			if err != nil {
				storage.close()
				return nil, &RuntimeError{Kind: RecoveryIncompatible, Err: err}
			}
			entry.machine = machine
			record.Checkpoint = checkpoint.Bytes()
		}
		e.recoveryStats.FlowDeltaApply += time.Since(deltaStarted)
		e.store.apply(record)
		entry.checkpoint = append(entry.checkpoint[:0], record.Checkpoint...)
		e.rememberResult(record.CommandID, record.Result)
		e.sequence.Store(record.Sequence)
	}
	e.recoveryStats.AgentsRestored = e.AgentCount()
	e.recoveryStats.TotalReady = time.Since(started)
	go e.runCommitAuthority()
	return e, nil
}

func (e *Engine) Store() *Store               { return e.store }
func (e *Engine) RecoveryTruncatedTail() bool { return e.recoveryTruncated }
func (e *Engine) AgentCount() int {
	e.registryMu.Lock()
	defer e.registryMu.Unlock()
	return len(e.registry)
}

func (e *Engine) RecoveryMetrics() RecoveryStats { return e.recoveryStats }
func (e *Engine) StorageMetrics() StorageStats {
	if e.m1 == nil {
		return StorageStats{}
	}
	return e.m1.statsSnapshot()
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
	if e.m1 == nil {
		e.commitMu.Lock()
		if prior, ok := e.results[command.ID]; ok {
			e.commitMu.Unlock()
			prior.Duplicate = true
			return outcome{result: prior}
		}
		e.commitMu.Unlock()
	}
	a, aok, b, bok := e.store.view(command.Account, command.Other)
	e.trace(TraceEvent{CommandID: command.ID, Agent: command.Account, Phase: "state_view", VersionA: a.Version, VersionB: b.Version})
	if e.m1 != nil && e.cfg.GoBehavioralControl {
		return e.processGoM1(entry, command, a, aok, b, bok)
	}
	if entry.machine == nil {
		entry.machine = generated.NewDurableAccountAgent(int(command.Account))
	}
	machine := entry.machine
	rollback := func() error {
		if len(entry.checkpoint) == 0 {
			entry.machine = generated.NewDurableAccountAgent(int(command.Account))
			return nil
		}
		checkpoint, err := generated.ParseAccountAgentCheckpoint(entry.checkpoint)
		if err != nil {
			return err
		}
		restored, err := generated.RestoreDurableAccountAgent(checkpoint)
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
	if e.m1 != nil {
		if err := e.m1.inject(AfterStepBeforeDeltaExport); err != nil {
			_ = rollback()
			return outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
	}
	e.trace(TraceEvent{CommandID: command.ID, Agent: command.Account, Phase: "flow_yield", VersionA: a.Version, VersionB: b.Version, Accepted: decision.Accepted, ReasonTag: decision.Reason.Tag, EffectTag: decision.Effect.Tag, FlowState: turn.Active()})
	checkpoint, err := machine.Checkpoint()
	if err != nil {
		_ = rollback()
		return outcome{err: &RuntimeError{Kind: CheckpointExportFailed, Err: err}}
	}
	cpBytes := checkpoint.Bytes()
	if e.m1 != nil {
		result := Result{CommandID: command.ID, Accepted: decision.Accepted, ReasonTag: decision.Reason.Tag, EffectTag: decision.Effect.Tag, TransitionCount: decision.TransitionCount}
		record := logRecord{Version: m1WALVersion, SchemaID: m1SchemaID, ProgramID: m1ProgramID, AgentID: command.Account, CommandID: command.ID, CommandKind: command.Kind.Tag, AccountA: command.Account, AccountB: command.Other, Amount: command.Amount, Result: result, EffectTag: decision.Effect.Tag, NewBalanceA: decision.NewBalanceA, NewBalanceB: decision.NewBalanceB, NewStatusTag: decision.NewStatus.Tag, ExpectedA: uint64(decision.ExpectedVersionA), ExpectedB: uint64(decision.ExpectedVersionB)}
		if e.cfg.FullCheckpointWAL {
			record.Checkpoint = cpBytes
		} else {
			delta, err := machine.ExportDelta()
			if err != nil {
				_ = rollback()
				return outcome{err: &RuntimeError{Kind: CheckpointExportFailed, Err: err}}
			}
			record.FlowDelta = delta.Bytes()
		}
		if err := e.m1.inject(AfterDeltaExportBeforeWALAppend); err != nil {
			_ = rollback()
			return outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		response := make(chan outcome, 1)
		e.authority <- &m1Commit{record: record, entry: entry, checkpoint: cpBytes, response: response, install: func() { _ = machine.AcceptCommitted(checkpoint) }}
		got := <-response
		if got.err != nil || got.result.Duplicate {
			_ = rollback()
		}
		return got
	}
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
func (e *Engine) Flush() error { return e.log.flush() }
func (e *Engine) InjectDurabilityFailure(err error) {
	if e.m1 != nil {
		e.m1.failpoint = func(point FailurePoint) error {
			if point == BeforeWALAppend {
				return err
			}
			return nil
		}
		return
	}
	e.log.injectFailure(err)
}
func (e *Engine) Close() error {
	if !e.closed.CompareAndSwap(false, true) {
		return nil
	}
	if e.m1 != nil {
		response := make(chan error, 1)
		e.authority <- &closeAuthority{response: response}
		return <-response
	}
	return e.log.close()
}

func (e *Engine) rememberResult(id string, result Result) {
	e.results[id] = result
	e.dedupeOrder = append(e.dedupeOrder, id)
	if len(e.dedupeOrder)-e.dedupeHead > e.cfg.DedupeWindow {
		delete(e.results, e.dedupeOrder[e.dedupeHead])
		e.dedupeHead++
	}
	if e.dedupeHead >= e.cfg.DedupeWindow && e.dedupeHead*2 >= len(e.dedupeOrder) {
		active := copy(e.dedupeOrder, e.dedupeOrder[e.dedupeHead:])
		clear(e.dedupeOrder[active:])
		e.dedupeOrder = e.dedupeOrder[:active]
		e.dedupeHead = 0
	}
}

func recoverAccountAgentCheckpoint(entry *agentEntry, record logRecord) (generated.AccountAgentCheckpoint, error) {
	if len(record.FlowDelta) > 0 && len(record.Checkpoint) > 0 {
		return generated.AccountAgentCheckpoint{}, errors.New("WAL record has both FLOW delta and checkpoint")
	}
	if len(record.FlowDelta) == 0 {
		if len(record.Checkpoint) == 0 {
			return generated.AccountAgentCheckpoint{}, errors.New("WAL record has no FLOW state")
		}
		return generated.ParseAccountAgentCheckpoint(record.Checkpoint)
	}
	delta, err := generated.ParseAccountAgentDelta(record.FlowDelta)
	if err != nil {
		return generated.AccountAgentCheckpoint{}, err
	}
	var previous *generated.AccountAgentCheckpoint
	if len(entry.checkpoint) > 0 {
		parsed, err := generated.ParseAccountAgentCheckpoint(entry.checkpoint)
		if err != nil {
			return generated.AccountAgentCheckpoint{}, err
		}
		previous = &parsed
	}
	return generated.ApplyAccountAgentDelta(previous, int(record.AgentID), delta)
}

func validateDedupeEntries(entries []snapshotResult, horizon, configured int, frontier uint64) error {
	if horizon != configured || horizon <= 0 || len(entries) > horizon {
		return fmt.Errorf("dedupe count/horizon mismatch: %d/%d configured %d", len(entries), horizon, configured)
	}
	seen := make(map[string]struct{}, len(entries))
	var prior uint64
	for _, entry := range entries {
		if entry.CommandID == "" || entry.Result.CommandID != entry.CommandID {
			return errors.New("dedupe command identity mismatch")
		}
		if _, ok := seen[entry.CommandID]; ok {
			return fmt.Errorf("duplicate command ID %q in snapshot", entry.CommandID)
		}
		seen[entry.CommandID] = struct{}{}
		if entry.Result.Sequence <= prior {
			return fmt.Errorf("out-of-order dedupe sequence %d", entry.Result.Sequence)
		}
		if entry.Result.Sequence > frontier {
			return fmt.Errorf("dedupe sequence %d exceeds frontier", entry.Result.Sequence)
		}
		prior = entry.Result.Sequence
	}
	return nil
}

func (e *Engine) runCommitAuthority() {
	for {
		message := <-e.authority
		switch first := message.(type) {
		case *m1Commit:
			group := []*m1Commit{first}
			max := e.cfg.GroupMax
			if e.cfg.Durability != BatchSync {
				max = 1
			}
			var deferred []any
			if max > 1 {
				if e.cfg.GroupWait == 0 {
				drain:
					for len(group) < max {
						select {
						case next := <-e.authority:
							if commit, ok := next.(*m1Commit); ok {
								group = append(group, commit)
							} else {
								deferred = append(deferred, next)
							}
						default:
							break drain
						}
					}
				} else {
					timer := time.NewTimer(e.cfg.GroupWait)
				collect:
					for len(group) < max {
						select {
						case next := <-e.authority:
							if commit, ok := next.(*m1Commit); ok {
								group = append(group, commit)
							} else {
								deferred = append(deferred, next)
							}
						case <-timer.C:
							break collect
						}
					}
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}
			}
			e.commitGroup(group)
			for _, item := range deferred {
				e.handleAuthorityControl(item)
			}
		case *snapshotRequest, *closeAuthority:
			if e.handleAuthorityControl(first) {
				return
			}
		}
	}
}

func (e *Engine) commitGroup(group []*m1Commit) {
	if e.poisoned != nil {
		for _, request := range group {
			request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: e.poisoned}}
		}
		return
	}
	unique := make([]*m1Commit, 0, len(group))
	type repeated struct{ request, original *m1Commit }
	var repeats []repeated
	seen := make(map[string]*m1Commit)
	for _, request := range group {
		if prior, ok := e.results[request.record.CommandID]; ok {
			prior.Duplicate = true
			request.response <- outcome{result: prior}
			continue
		}
		if original, ok := seen[request.record.CommandID]; ok {
			repeats = append(repeats, repeated{request: request, original: original})
			continue
		}
		seen[request.record.CommandID] = request
		sequence := e.sequence.Load() + uint64(len(unique)) + 1
		request.record.Sequence = sequence
		request.record.Result.Sequence = sequence
		unique = append(unique, request)
	}
	records := make([]logRecord, len(unique))
	for i, request := range unique {
		records[i] = request.record
	}
	if len(records) == 0 {
		return
	}
	if err := e.m1.appendGroup(records); err != nil {
		e.poisoned = err
		for _, request := range unique {
			request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		for _, repeat := range repeats {
			repeat.request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		return
	}
	if err := e.m1.inject(AfterWALSyncBeforeApply); err != nil {
		e.poisoned = err
		for _, request := range unique {
			request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		for _, repeat := range repeats {
			repeat.request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		return
	}
	if err := e.m1.inject(AfterSyncBeforeDirtyClear); err != nil {
		e.poisoned = err
		for _, request := range unique {
			request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		for _, repeat := range repeats {
			repeat.request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		return
	}
	for _, request := range unique {
		e.store.apply(request.record)
		if err := e.m1.inject(AfterStateApplyBeforeDirtyClear); err != nil {
			e.poisoned = err
			for _, pending := range unique {
				pending.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
			}
			for _, repeat := range repeats {
				repeat.request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
			}
			return
		}
		request.entry.checkpoint = append(request.entry.checkpoint[:0], request.checkpoint...)
		if request.install != nil {
			request.install()
		}
		e.rememberResult(request.record.CommandID, request.record.Result)
		e.sequence.Store(request.record.Sequence)
	}
	if err := e.m1.inject(AfterStateApplyBeforeAck); err != nil {
		e.poisoned = err
		for _, request := range unique {
			request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		for _, repeat := range repeats {
			repeat.request.response <- outcome{err: &RuntimeError{Kind: DurabilityWriteFailed, Err: err}}
		}
		return
	}
	for _, request := range unique {
		request.response <- outcome{result: request.record.Result}
	}
	for _, repeat := range repeats {
		result := repeat.original.record.Result
		result.Duplicate = true
		repeat.request.response <- outcome{result: result}
	}
	if e.cfg.SnapshotEvery > 0 && e.sequence.Load()%e.cfg.SnapshotEvery == 0 {
		_, _, _ = e.snapshotAtFrontier()
	}
}

func (e *Engine) handleAuthorityControl(message any) bool {
	switch request := message.(type) {
	case *snapshotRequest:
		path, size, err := e.snapshotAtFrontier()
		request.response <- snapshotOutcome{path: path, bytes: size, err: err}
	case *closeAuthority:
		request.response <- e.m1.close()
		return true
	}
	return false
}

func (e *Engine) snapshotAtFrontier() (string, int64, error) {
	if e.poisoned != nil {
		return "", 0, e.poisoned
	}
	if err := e.m1.closeSegment(true); err != nil {
		return "", 0, err
	}
	accounts, ledger := e.store.snapshotState()
	octagon, hash := e.store.CanonicalOctagon()
	snapshot := durableSnapshot{Version: m1SnapshotVersion, Sequence: e.sequence.Load(), SchemaID: m1SchemaID, ProgramID: expectedProgramID(e.cfg), Accounts: accounts, Ledger: ledger, Octagon: octagon, OctHash: hash, DedupeHorizon: e.cfg.DedupeWindow}
	var dedupe []snapshotResult
	for _, id := range e.dedupeOrder[e.dedupeHead:] {
		dedupe = append(dedupe, snapshotResult{CommandID: id, Result: e.results[id]})
	}
	if e.cfg.JSONDedupeSnapshot {
		snapshot.DedupeFormat, snapshot.Dedupe = "json-v1", dedupe
	} else {
		if err := e.m1.inject(DuringCompactDedupeEncoding); err != nil {
			return "", 0, err
		}
		compact, err := encodeCompactDedupe(dedupe, e.cfg.DedupeWindow)
		if err != nil {
			return "", 0, err
		}
		snapshot.DedupeFormat, snapshot.DedupeCompact = "compact-v1", compact
	}
	e.registryMu.Lock()
	if err := e.m1.inject(DuringSnapshotFlowCheckpoint); err != nil {
		e.registryMu.Unlock()
		return "", 0, err
	}
	ids := make([]int, 0, len(e.registry))
	for id, entry := range e.registry {
		if len(entry.checkpoint) > 0 {
			ids = append(ids, int(id))
		}
	}
	sort.Ints(ids)
	for _, raw := range ids {
		entry := e.registry[AccountID(raw)]
		snapshot.Agents = append(snapshot.Agents, snapshotAgent{ID: entry.id, Checkpoint: append([]byte(nil), entry.checkpoint...)})
	}
	e.registryMu.Unlock()
	logicalBytes, _ := json.Marshal(struct {
		Accounts []Account     `json:"accounts"`
		Ledger   []LedgerEntry `json:"ledger"`
	}{accounts, ledger})
	dedupeBytes := snapshot.DedupeCompact
	if snapshot.DedupeFormat == "json-v1" {
		dedupeBytes, _ = json.Marshal(snapshot.Dedupe)
	}
	e.m1.stats.LogicalStateBytes = uint64(len(logicalBytes))
	e.m1.stats.DedupeBytes = uint64(len(dedupeBytes))
	e.m1.stats.PublicationBytes = uint64(len(octagon))
	e.m1.stats.FlowCheckpointBytes = 0
	for _, agent := range snapshot.Agents {
		e.m1.stats.FlowCheckpointBytes += uint64(len(agent.Checkpoint))
	}
	path, size, err := e.m1.installSnapshot(snapshot)
	if err != nil {
		return "", 0, err
	}
	if err := e.m1.retireThrough(snapshot.Sequence); err != nil {
		return path, size, err
	}
	return path, size, nil
}

func (e *Engine) Snapshot() (string, int64, error) {
	if e.m1 == nil {
		return "", 0, errors.New("snapshots require StorageDir")
	}
	response := make(chan snapshotOutcome, 1)
	e.authority <- &snapshotRequest{response: response}
	got := <-response
	return got.path, got.bytes, got.err
}

type goAgentCheckpoint struct {
	Pending pendingTransfer `json:"pending"`
	Turns   int             `json:"turns"`
}

func (e *Engine) processGoM1(entry *agentEntry, command Command, a Account, aok bool, b Account, bok bool) outcome {
	pending := entry.goPending
	d := decideGo(command, a, aok, b, bok, pending)
	next := pending
	if isKind(command.Kind, BeginTransfer) && d.accepted {
		next = pendingTransfer{Active: true, Target: command.Other, Amount: command.Amount}
	}
	if isKind(command.Kind, Confirm) || isKind(command.Kind, Cancel) {
		next = pendingTransfer{}
	}
	turns := entry.goTurns + 1
	checkpoint, err := json.Marshal(goAgentCheckpoint{Pending: next, Turns: turns})
	if err != nil {
		return outcome{err: err}
	}
	result := Result{CommandID: command.ID, Accepted: d.accepted, ReasonTag: d.reason, EffectTag: d.effect, TransitionCount: turns}
	record := logRecord{Version: m1WALVersion, SchemaID: m1SchemaID, ProgramID: expectedProgramID(e.cfg), AgentID: command.Account, CommandID: command.ID, CommandKind: command.Kind.Tag, AccountA: command.Account, AccountB: command.Other, Amount: command.Amount, Result: result, EffectTag: d.effect, NewBalanceA: d.balanceA, NewBalanceB: d.balanceB, NewStatusTag: d.status, ExpectedA: a.Version, ExpectedB: b.Version, Checkpoint: checkpoint}
	response := make(chan outcome, 1)
	e.authority <- &m1Commit{record: record, entry: entry, checkpoint: checkpoint, response: response, install: func() { entry.goPending, entry.goTurns = next, turns }}
	return <-response
}

func OpenGoM1Baseline(cfg Config) (*Engine, error) {
	cfg.GoBehavioralControl = true
	return Open(cfg)
}
