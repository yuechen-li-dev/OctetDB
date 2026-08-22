package m7write

import (
	"encoding/json"
	"sync"
)

type GoEngine struct {
	store    *Store
	locks    *lockTable
	log      *commitLog
	mu       sync.Mutex
	results  map[string]Result
	pending  map[AccountID]pendingTransfer
	sequence uint64
}

type pendingTransfer struct {
	Active bool      `json:"active"`
	Target AccountID `json:"target"`
	Amount int       `json:"amount"`
}

func OpenGoBaseline(cfg Config) (*GoEngine, error) {
	log, records, _, err := openCommitLog(cfg.LogPath, cfg.Durability, cfg.BatchSize)
	if err != nil {
		return nil, err
	}
	e := &GoEngine{store: NewStore(), locks: newLockTable(), log: log, results: make(map[string]Result), pending: make(map[AccountID]pendingTransfer)}
	for _, record := range records {
		e.store.apply(record)
		e.results[record.CommandID] = record.Result
		e.sequence = record.Sequence
		if len(record.Checkpoint) > 0 {
			var p pendingTransfer
			if err := json.Unmarshal(record.Checkpoint, &p); err != nil {
				log.close()
				return nil, err
			}
			e.pending[record.AgentID] = p
		}
	}
	return e, nil
}

func (e *GoEngine) Store() *Store { return e.store }

func (e *GoEngine) Execute(command Command) (Result, error) {
	if err := ValidateCommand(command); err != nil {
		return Result{}, err
	}
	release := e.locks.acquire(tokensFor(command))
	defer release()
	e.mu.Lock()
	defer e.mu.Unlock()
	if prior, ok := e.results[command.ID]; ok {
		prior.Duplicate = true
		return prior, nil
	}
	a, aok, b, bok := e.store.view(command.Account, command.Other)
	pending := e.pending[command.Account]
	d := decideGo(command, a, aok, b, bok, pending)
	nextPending := pending
	if isKind(command.Kind, BeginTransfer) && d.accepted {
		nextPending = pendingTransfer{Active: true, Target: command.Other, Amount: command.Amount}
	}
	if isKind(command.Kind, Confirm) || isKind(command.Kind, Cancel) {
		nextPending = pendingTransfer{}
	}
	checkpoint, err := json.Marshal(nextPending)
	if err != nil {
		return Result{}, err
	}
	e.sequence++
	result := Result{Sequence: e.sequence, CommandID: command.ID, Accepted: d.accepted, ReasonTag: d.reason, EffectTag: d.effect}
	record := logRecord{Version: 1, Sequence: e.sequence, AgentID: command.Account, CommandID: command.ID, CommandKind: command.Kind.Tag, AccountA: command.Account, AccountB: command.Other, Amount: command.Amount, Result: result, EffectTag: d.effect, NewBalanceA: d.balanceA, NewBalanceB: d.balanceB, NewStatusTag: d.status, ExpectedA: a.Version, ExpectedB: b.Version, Checkpoint: checkpoint}
	if err := e.log.append(record); err != nil {
		return Result{}, err
	}
	e.store.apply(record)
	e.results[command.ID] = result
	e.pending[command.Account] = nextPending
	return result, nil
}

func (e *GoEngine) Close() error { return e.log.close() }

type goDecision struct {
	accepted                                   bool
	reason, effect, balanceA, balanceB, status int
}

func decideGo(c Command, a Account, aok bool, b Account, bok bool, pending pendingTransfer) goDecision {
	none := func(reason int) goDecision {
		return goDecision{reason: reason, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	}
	applied := 0
	invalidAmount, missing, exists, frozen, insufficient, invalidWorkflow := 3, 4, 5, 6, 7, 8
	switch {
	case isKind(c.Kind, Create):
		if aok {
			return none(exists)
		}
		if c.Amount < 0 {
			return none(invalidAmount)
		}
		return goDecision{accepted: true, reason: applied, effect: 1, balanceA: c.Amount, balanceB: b.Balance, status: StatusOpen}
	case isKind(c.Kind, Deposit):
		if c.Amount <= 0 {
			return none(invalidAmount)
		}
		if !aok {
			return none(missing)
		}
		return goDecision{accepted: true, reason: applied, effect: 2, balanceA: a.Balance + c.Amount, balanceB: b.Balance, status: a.Status}
	case isKind(c.Kind, Withdraw):
		if c.Amount <= 0 {
			return none(invalidAmount)
		}
		if !aok {
			return none(missing)
		}
		if a.Status == StatusFrozen {
			return none(frozen)
		}
		if a.Balance < c.Amount {
			return none(insufficient)
		}
		return goDecision{accepted: true, reason: applied, effect: 2, balanceA: a.Balance - c.Amount, balanceB: b.Balance, status: a.Status}
	case isKind(c.Kind, Transfer) || isKind(c.Kind, Confirm):
		if isKind(c.Kind, Confirm) && (!pending.Active || pending.Target != c.Other || pending.Amount != c.Amount) {
			return none(invalidWorkflow)
		}
		if c.Amount <= 0 {
			return none(invalidAmount)
		}
		if !aok || !bok {
			return none(missing)
		}
		if a.Status == StatusFrozen {
			return none(frozen)
		}
		if a.Balance < c.Amount {
			return none(insufficient)
		}
		return goDecision{accepted: true, reason: applied, effect: 3, balanceA: a.Balance - c.Amount, balanceB: b.Balance + c.Amount, status: a.Status}
	case isKind(c.Kind, Freeze):
		if !aok {
			return none(missing)
		}
		return goDecision{accepted: true, reason: applied, effect: 4, balanceA: a.Balance, balanceB: b.Balance, status: StatusFrozen}
	case isKind(c.Kind, Unfreeze):
		if !aok {
			return none(missing)
		}
		return goDecision{accepted: true, reason: applied, effect: 4, balanceA: a.Balance, balanceB: b.Balance, status: StatusOpen}
	case isKind(c.Kind, BeginTransfer):
		if c.Amount <= 0 {
			return none(invalidAmount)
		}
		return goDecision{accepted: true, reason: 1, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	case isKind(c.Kind, Cancel):
		return goDecision{accepted: true, reason: 2, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	default:
		return goDecision{reason: invalidWorkflow, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	}
}
