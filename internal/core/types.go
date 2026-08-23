// Package core implements the canonical, bounded OctetDB account engine.
package core

import (
	"errors"
	"fmt"
	"time"
)

type AccountID uint64

type Account struct {
	ID      AccountID `json:"id"`
	Balance int       `json:"balance"`
	Status  int       `json:"status"`
	Version uint64    `json:"version"`
}

const (
	StatusOpen   = 1
	StatusFrozen = 2
)

type CommandKind struct{ Tag int }

var (
	Create        = CommandKind{Tag: 0}
	Deposit       = CommandKind{Tag: 1}
	Withdraw      = CommandKind{Tag: 2}
	Transfer      = CommandKind{Tag: 3}
	Freeze        = CommandKind{Tag: 4}
	Unfreeze      = CommandKind{Tag: 5}
	BeginTransfer = CommandKind{Tag: 6}
	Confirm       = CommandKind{Tag: 7}
	Cancel        = CommandKind{Tag: 8}
)

type Command struct {
	ID      string
	Kind    CommandKind
	Account AccountID
	Other   AccountID
	Amount  int
}

type Result struct {
	Sequence        uint64
	CommandID       string
	Accepted        bool
	ReasonTag       int
	EffectTag       int
	TransitionCount int
	Duplicate       bool
}

type DurabilityMode uint8

const (
	MemoryOnly DurabilityMode = iota
	BatchSync
	SyncEach
)

type ErrorKind string

const (
	CapacityExceeded      ErrorKind = "capacity_exceeded"
	EngineClosed          ErrorKind = "engine_closed"
	EnginePoisoned        ErrorKind = "engine_poisoned"
	InvalidCommand        ErrorKind = "invalid_command"
	DurabilityWriteFailed ErrorKind = "durability_write_failed"
	RecoveryCorrupt       ErrorKind = "recovery_corrupt"
	RecoveryIncompatible  ErrorKind = "recovery_incompatible"
)

type RuntimeError struct {
	Kind ErrorKind
	Err  error
}

func (e *RuntimeError) Error() string { return fmt.Sprintf("%s: %v", e.Kind, e.Err) }
func (e *RuntimeError) Unwrap() error { return e.Err }

var errClosed = errors.New("engine closed")

func isKind(kind, expected CommandKind) bool { return kind.Tag == expected.Tag }

func ValidateCommand(command Command) error {
	if command.ID == "" || command.Account == 0 {
		return &RuntimeError{Kind: InvalidCommand, Err: errors.New("command ID and account are required")}
	}
	if (isKind(command.Kind, Transfer) || isKind(command.Kind, BeginTransfer) || isKind(command.Kind, Confirm)) && (command.Other == 0 || command.Other == command.Account) {
		return &RuntimeError{Kind: InvalidCommand, Err: errors.New("multi-account command requires a distinct destination")}
	}
	return nil
}

type LedgerEntry struct {
	Sequence  uint64
	CommandID string
	From      AccountID
	To        AccountID
	Amount    int
	EffectTag int
}

type RecoveryStats struct {
	SnapshotSequence uint64
	SnapshotBytes    int64
	SnapshotDecode   time.Duration
	WALScan          time.Duration
	WALBytesScanned  int64
	RecordsReplayed  int
	TotalReady       time.Duration
}

type StorageStats struct {
	WALBytesWritten      uint64
	SnapshotBytesWritten uint64
	Syncs                uint64
	Committed            uint64
}

type pendingTransfer struct {
	Active bool
	Target AccountID
	Amount int
}

type decision struct {
	accepted                                   bool
	reason, effect, balanceA, balanceB, status int
}

func decideGo(c Command, a Account, aok bool, b Account, bok bool, pending pendingTransfer) decision {
	none := func(reason int) decision {
		return decision{reason: reason, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	}
	const applied = 0
	const invalidAmount, missing, exists, frozen, insufficient, invalidWorkflow = 3, 4, 5, 6, 7, 8
	switch {
	case isKind(c.Kind, Create):
		if aok {
			return none(exists)
		}
		if c.Amount < 0 {
			return none(invalidAmount)
		}
		return decision{accepted: true, reason: applied, effect: 1, balanceA: c.Amount, balanceB: b.Balance, status: StatusOpen}
	case isKind(c.Kind, Deposit):
		if c.Amount <= 0 {
			return none(invalidAmount)
		}
		if !aok {
			return none(missing)
		}
		return decision{accepted: true, reason: applied, effect: 2, balanceA: a.Balance + c.Amount, balanceB: b.Balance, status: a.Status}
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
		return decision{accepted: true, reason: applied, effect: 2, balanceA: a.Balance - c.Amount, balanceB: b.Balance, status: a.Status}
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
		return decision{accepted: true, reason: applied, effect: 3, balanceA: a.Balance - c.Amount, balanceB: b.Balance + c.Amount, status: a.Status}
	case isKind(c.Kind, Freeze):
		if !aok {
			return none(missing)
		}
		return decision{accepted: true, reason: applied, effect: 4, balanceA: a.Balance, balanceB: b.Balance, status: StatusFrozen}
	case isKind(c.Kind, Unfreeze):
		if !aok {
			return none(missing)
		}
		return decision{accepted: true, reason: applied, effect: 4, balanceA: a.Balance, balanceB: b.Balance, status: StatusOpen}
	case isKind(c.Kind, BeginTransfer):
		if c.Amount <= 0 {
			return none(invalidAmount)
		}
		return decision{accepted: true, reason: 1, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	case isKind(c.Kind, Cancel):
		return decision{accepted: true, reason: 2, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	default:
		return decision{reason: invalidWorkflow, balanceA: a.Balance, balanceB: b.Balance, status: a.Status}
	}
}
