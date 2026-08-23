package octetdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/yuechen-li-dev/octetdb/internal/core"
)

const (
	// FormatVersion is the on-disk database format written by this release.
	FormatVersion = 1
	// Version is the pre-release module version for diagnostic provenance.
	Version = "0.1.0-dev"
)

const formatContents = "OCTETDB\nformat=1\nmodel=accounts-v1\nengine=safe-go\noct-revision=309da01b60ec0f7917d4fd5efd1707bd71d2d40f\n"

// CommandKind selects one operation in the v0.1 account domain.
type CommandKind uint8

const (
	// Create creates an account with Amount as its non-negative opening balance.
	Create CommandKind = iota + 1
	// Deposit adds a positive Amount to an existing account.
	Deposit
	// Withdraw removes a positive Amount when the account is open and funded.
	Withdraw
	// Transfer moves a positive Amount from AccountID to OtherAccountID.
	Transfer
	// Freeze prevents withdrawals and transfers from an account.
	Freeze
	// Unfreeze restores withdrawal and transfer eligibility.
	Unfreeze
	// BeginTransfer records a pending transfer for later confirmation.
	BeginTransfer
	// Confirm applies the matching pending transfer.
	Confirm
	// Cancel clears a pending transfer without changing balances.
	Cancel
)

// Command is a uniquely identified account operation. Command IDs provide
// exact idempotency while retained in the configured dedupe horizon.
type Command struct {
	// ID is the application-assigned idempotency key.
	ID string
	// Kind selects the account operation.
	Kind CommandKind
	// AccountID is the primary account or transfer source.
	AccountID uint64
	// OtherAccountID is the destination for multi-account commands.
	OtherAccountID uint64
	// Amount is the opening balance or amount operated on, depending on Kind.
	Amount int64
}

// Result is the durable decision for one command.
type Result struct {
	// Sequence is the durable decision's monotonic database sequence.
	Sequence uint64
	// CommandID is the submitted idempotency key.
	CommandID string
	// Accepted reports whether the domain applied the requested transition.
	Accepted bool
	// Reason explains the domain decision.
	Reason Reason
	// Duplicate reports that a retained prior result was returned.
	Duplicate bool
}

// Reason explains an accepted or rejected domain decision.
type Reason string

const (
	// ReasonApplied means the requested state change was applied.
	ReasonApplied Reason = "applied"
	// ReasonAwaitingConfirmation means a pending transfer was recorded.
	ReasonAwaitingConfirmation Reason = "awaiting_confirmation"
	// ReasonCancelled means a pending transfer was cleared.
	ReasonCancelled Reason = "cancelled"
	// ReasonInvalidAmount means the domain rejected the amount.
	ReasonInvalidAmount Reason = "invalid_amount"
	// ReasonAccountMissing means a referenced account does not exist.
	ReasonAccountMissing Reason = "account_missing"
	// ReasonAccountExists means Create named an existing account.
	ReasonAccountExists Reason = "account_exists"
	// ReasonAccountFrozen means a frozen account cannot perform the operation.
	ReasonAccountFrozen Reason = "account_frozen"
	// ReasonInsufficientFunds means the source balance was too small.
	ReasonInsufficientFunds Reason = "insufficient_funds"
	// ReasonInvalidWorkflow means a pending-transfer transition did not match.
	ReasonInvalidWorkflow Reason = "invalid_workflow"
)

// Account is the authoritative state for one account.
type Account struct {
	// ID is the account's application-assigned key.
	ID uint64
	// Balance is the current authoritative integer balance.
	Balance int64
	// Frozen reports whether outgoing value operations are disabled.
	Frozen bool
	// Version increments for each applied state change.
	Version uint64
}

// Options configures a bounded durable database.
type Options struct {
	// Path is the required database directory.
	Path string
	// MaxAccounts bounds dense account slots; zero selects 100,000.
	MaxAccounts int
	// DedupeHorizon bounds retained exact command results; zero selects 100,000.
	DedupeHorizon int
	// BatchMax bounds commands in one SubmitBatch call; zero selects 512.
	BatchMax int
}

// Stats is a consistent snapshot of the smallest reliable operational counters.
type Stats struct {
	// CommittedSequence is the latest durable decision sequence.
	CommittedSequence uint64
	// WALBytesWritten is WAL write volume in the current process.
	WALBytesWritten uint64
	// SnapshotSequence is the installed snapshot sequence, or zero if absent.
	SnapshotSequence uint64
	// DedupeEntries is the number of currently retained command results.
	DedupeEntries int
	// AccountCount is the number of allocated account identities.
	AccountCount int
}

// DB is an open OctetDB handle. A database directory must have at most one
// open handle across all processes; v0.1 does not yet provide a lock file.
type DB struct {
	core      *core.Core
	admission chan struct{}
	closed    atomic.Bool
}

// Open creates or recovers a durable database. Context cancellation is checked
// before recovery begins; once recovery starts, Open completes it or returns a
// storage/recovery error so the caller never receives a partially recovered DB.
func Open(ctx context.Context, options Options) (*DB, error) {
	if ctx == nil {
		return nil, &Error{Kind: ErrorInvalidInput, Op: "open", Err: errors.New("nil context")}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	if err := ensureFormat(options.Path); err != nil {
		return nil, err
	}
	engineCore, err := core.OpenCore(core.CoreConfig{
		StorageDir:   options.Path,
		Durability:   core.BatchSync,
		DedupeWindow: options.DedupeHorizon,
		MaxAccounts:  options.MaxAccounts,
		BatchMax:     options.BatchMax,
		AccountHint:  min(options.MaxAccounts, 1024),
	})
	if err != nil {
		return nil, wrapError("open", err)
	}
	db := &DB{core: engineCore, admission: make(chan struct{}, 1)}
	db.admission <- struct{}{}
	return db, nil
}

// Submit submits one command and returns its durable decision.
func (db *DB) Submit(ctx context.Context, command Command) (Result, error) {
	results, err := db.SubmitBatch(ctx, []Command{command})
	if err != nil {
		return Result{}, err
	}
	return results[0], nil
}

// SubmitBatch evaluates commands in order and commits all new decisions in one
// WAL frame and one synchronization. Cancellation can abort while waiting for
// admission. After admission, the operation runs to a definitive durable result.
func (db *DB) SubmitBatch(ctx context.Context, commands []Command) ([]Result, error) {
	if err := db.enter(ctx, "submit"); err != nil {
		return nil, err
	}
	defer db.leave()
	if len(commands) == 0 {
		return []Result{}, nil
	}
	internal := make([]core.Command, len(commands))
	for i, command := range commands {
		converted, err := convertCommand(command)
		if err != nil {
			return nil, err
		}
		internal[i] = converted
	}
	results, err := db.core.SubmitBatch(internal)
	if err != nil {
		return nil, wrapError("submit", err)
	}
	out := make([]Result, len(results))
	for i, result := range results {
		out[i] = convertResult(result)
	}
	return out, nil
}

// Get reads the current authoritative account state. It returns false for a
// missing account and after the DB is closed.
func (db *DB) Get(id uint64) (Account, bool) {
	if db == nil || db.closed.Load() {
		return Account{}, false
	}
	account, ok := db.core.Account(core.AccountID(id))
	if !ok {
		return Account{}, false
	}
	return Account{ID: uint64(account.ID), Balance: int64(account.Balance), Frozen: account.Status == core.StatusFrozen, Version: account.Version}, true
}

// Snapshot atomically installs a snapshot and starts a fresh WAL. Cancellation
// is honored while waiting for admission, not after snapshot installation starts.
func (db *DB) Snapshot(ctx context.Context) error {
	if err := db.enter(ctx, "snapshot"); err != nil {
		return err
	}
	defer db.leave()
	_, _, err := db.core.Snapshot()
	return wrapError("snapshot", err)
}

// Stats returns reliable counters without creating a metrics subsystem.
func (db *DB) Stats() Stats {
	if db == nil {
		return Stats{}
	}
	stats := db.core.Stats()
	return Stats{CommittedSequence: stats.CommittedSequence, WALBytesWritten: stats.WALBytes, SnapshotSequence: stats.SnapshotSequence, DedupeEntries: stats.DedupeEntries, AccountCount: stats.AccountCount}
}

// Close stops the handle and closes its WAL. Close is idempotent.
func (db *DB) Close() error {
	if db == nil || !db.closed.CompareAndSwap(false, true) {
		return nil
	}
	<-db.admission
	defer db.leave()
	return wrapError("close", db.core.Close())
}

func (db *DB) enter(ctx context.Context, op string) error {
	if db == nil || db.closed.Load() {
		return &Error{Kind: ErrorClosed, Op: op, Err: errors.New("database is closed")}
	}
	if ctx == nil {
		return &Error{Kind: ErrorInvalidInput, Op: op, Err: errors.New("nil context")}
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-db.admission:
	}
	if db.closed.Load() {
		db.leave()
		return &Error{Kind: ErrorClosed, Op: op, Err: errors.New("database is closed")}
	}
	return nil
}

func (db *DB) leave() { db.admission <- struct{}{} }

func normalizeOptions(options Options) (Options, error) {
	if strings.TrimSpace(options.Path) == "" {
		return Options{}, &Error{Kind: ErrorInvalidInput, Op: "open", Err: errors.New("Path is required")}
	}
	if options.MaxAccounts < 0 || options.DedupeHorizon < 0 || options.BatchMax < 0 {
		return Options{}, &Error{Kind: ErrorInvalidInput, Op: "open", Err: errors.New("bounds cannot be negative")}
	}
	if options.MaxAccounts == 0 {
		options.MaxAccounts = 100_000
	}
	if options.DedupeHorizon == 0 {
		options.DedupeHorizon = 100_000
	}
	if options.BatchMax == 0 {
		options.BatchMax = 512
	}
	return options, nil
}

func ensureFormat(path string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return &Error{Kind: ErrorStorage, Op: "open", Err: err}
	}
	formatPath := filepath.Join(path, "FORMAT")
	data, err := os.ReadFile(formatPath)
	if err == nil {
		if string(data) != formatContents {
			return &Error{Kind: ErrorIncompatible, Op: "open", Err: fmt.Errorf("unsupported FORMAT contents")}
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return &Error{Kind: ErrorStorage, Op: "open", Err: err}
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "open", Err: err}
	}
	if len(entries) == 1 && entries[0].Name() == "FORMAT.tmp" {
		if err := os.Remove(filepath.Join(path, entries[0].Name())); err != nil {
			return &Error{Kind: ErrorStorage, Op: "open", Err: err}
		}
		entries = nil
	}
	if len(entries) != 0 {
		return &Error{Kind: ErrorIncompatible, Op: "open", Err: errors.New("non-empty directory has no OctetDB FORMAT marker")}
	}
	tmp := formatPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "open", Err: err}
	}
	_, writeErr := f.WriteString(formatContents)
	if writeErr == nil {
		writeErr = f.Sync()
	}
	if closeErr := f.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmp, formatPath)
	}
	if writeErr == nil && runtime.GOOS != "windows" {
		directory, openErr := os.Open(path)
		if openErr == nil {
			writeErr = directory.Sync()
			if closeErr := directory.Close(); writeErr == nil {
				writeErr = closeErr
			}
		} else {
			writeErr = openErr
		}
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return &Error{Kind: ErrorStorage, Op: "open", Err: writeErr}
	}
	return nil
}

func convertCommand(command Command) (core.Command, error) {
	if command.ID == "" || command.AccountID == 0 {
		return core.Command{}, &Error{Kind: ErrorInvalidInput, Op: "submit", Err: errors.New("command ID and AccountID are required")}
	}
	if command.Amount > int64(^uint(0)>>1) || command.Amount < -int64(^uint(0)>>1)-1 {
		return core.Command{}, &Error{Kind: ErrorInvalidInput, Op: "submit", Err: errors.New("amount is outside the platform integer range")}
	}
	kinds := map[CommandKind]core.CommandKind{
		Create: core.Create, Deposit: core.Deposit, Withdraw: core.Withdraw,
		Transfer: core.Transfer, Freeze: core.Freeze, Unfreeze: core.Unfreeze,
		BeginTransfer: core.BeginTransfer, Confirm: core.Confirm, Cancel: core.Cancel,
	}
	kind, ok := kinds[command.Kind]
	if !ok {
		return core.Command{}, &Error{Kind: ErrorInvalidInput, Op: "submit", Err: errors.New("unknown command kind")}
	}
	return core.Command{ID: command.ID, Kind: kind, Account: core.AccountID(command.AccountID), Other: core.AccountID(command.OtherAccountID), Amount: int(command.Amount)}, nil
}

func convertResult(result core.Result) Result {
	reasons := [...]Reason{ReasonApplied, ReasonAwaitingConfirmation, ReasonCancelled, ReasonInvalidAmount, ReasonAccountMissing, ReasonAccountExists, ReasonAccountFrozen, ReasonInsufficientFunds, ReasonInvalidWorkflow}
	reason := ReasonInvalidWorkflow
	if result.ReasonTag >= 0 && result.ReasonTag < len(reasons) {
		reason = reasons[result.ReasonTag]
	}
	return Result{Sequence: result.Sequence, CommandID: result.CommandID, Accepted: result.Accepted, Reason: reason, Duplicate: result.Duplicate}
}
