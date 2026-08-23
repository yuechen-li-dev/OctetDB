package octetdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// KeyedOptions configures the conventional application-defined keyed-state
// path. Zero values select bounded product defaults.
type KeyedOptions struct {
	// MaxRecords bounds live keys. Zero selects 100,000.
	MaxRecords int
	// DedupeHorizon bounds retained exact command decisions. Zero selects 100,000.
	DedupeHorizon int
	// MaxValueBytes bounds one encoded Go value. Zero selects 1 MiB.
	MaxValueBytes int
	// MaxTransactionBytes bounds all encoded writes in one command. Zero selects 4 MiB.
	MaxTransactionBytes int
}

// DefaultKeyedOptions returns the documented bounded defaults. It exists to
// make the default path explicit; the zero KeyedOptions value is equivalent.
func DefaultKeyedOptions() KeyedOptions { return KeyedOptions{} }

// KeyedCommand identifies an application mutation. ID must be stable across
// retries; OctetDB never silently generates retry identity.
type KeyedCommand struct {
	ID string
}

// KeyedDecision is the durable outcome of one keyed command. Result is the
// JSON encoding returned by the mutation function or by RejectWithResult.
type KeyedDecision struct {
	Sequence  uint64
	CommandID string
	Applied   bool
	Code      string
	Result    json.RawMessage
	Duplicate bool
}

// DecodeResult decodes a decision result into an application-defined Go value.
func DecodeResult(decision KeyedDecision, destination any) error {
	if destination == nil {
		return &Error{Kind: ErrorInvalidInput, Op: "decode_result", err: errors.New("destination is required")}
	}
	if len(decision.Result) == 0 {
		return &Error{Kind: ErrorInvalidInput, Op: "decode_result", err: errors.New("decision has no result")}
	}
	if err := json.Unmarshal(decision.Result, destination); err != nil {
		return &Error{Kind: ErrorInvalidInput, Op: "decode_result", err: err}
	}
	return nil
}

// KeyedMutation atomically reads and writes application-defined records. A nil
// error applies all writes. Reject or RejectWithResult records an exact durable
// rejection and discards all writes. Other errors abort without recording an ID.
//
// Deprecated: use Mutation with OpenCatalog.
type KeyedMutation func(*KeyedTx) (any, error)

// KeyedRejection is an application-domain rejection persisted for exact retry.
type KeyedRejection struct {
	Code   string
	result json.RawMessage
}

func (r *KeyedRejection) Error() string { return "octetdb command rejected: " + r.Code }

// Reject returns an error that makes SubmitKeyed durably reject a command.
func Reject(code string) error { return &KeyedRejection{Code: code} }

// RejectWithResult is Reject with an application-defined JSON result.
func RejectWithResult(code string, result any) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return &Error{Kind: ErrorInvalidInput, Op: "reject", err: err}
	}
	return &KeyedRejection{Code: code, result: encoded}
}

// KeyedDB is a durable, single-process database for application-defined JSON
// records. Mutations are serialized and atomic across all keys they touch.
//
// Deprecated: use Database with OpenCatalog. KeyedDB retains the distinct
// pre-v0.2 global-key format only for compatibility.
type KeyedDB struct {
	path         string
	options      KeyedOptions
	records      map[string][]byte
	dedupe       map[string]keyedWALRecord
	dedupeIDs    []string
	sequence     uint64
	wal          keyedWAL
	admission    chan struct{}
	closed       atomic.Bool
	poisoned     atomic.Bool
	format       string
	afterApply   func(keyedWALRecord)
	beforeAppend func(keyedWALRecord) error
	beforeSync   func() error
	afterSync    func()
	commit       keyedCommitCoordinator
	commitStats  keyedCommitStats
}

const keyedMaxCommitGroup = 64

type keyedCommitRequest struct {
	ctx      context.Context
	command  KeyedCommand
	mutation KeyedMutation
	result   chan keyedCommitResult
}

type keyedCommitResult struct {
	decision   KeyedDecision
	err        error
	panicValue any
}

type keyedCommitCoordinator struct {
	mu       sync.Mutex
	queue    []*keyedCommitRequest
	wake     chan struct{}
	done     chan struct{}
	disabled bool // internal benchmark/test control: one request per Sync
}

type keyedCommitStats struct {
	syncCalls    atomic.Uint64
	framesSynced atomic.Uint64
	groups       atomic.Uint64
	maxGroup     atomic.Uint64
	groupSizes   [keyedMaxCommitGroup + 1]atomic.Uint64
}

// KeyedTx is the application-facing transaction passed to a KeyedMutation.
// It is valid only during that callback.
//
// Deprecated: use Tx with Database.Mutate.
type KeyedTx struct {
	db      *KeyedDB
	writes  map[string]*[]byte
	invalid bool
}

// OpenKeyed creates or recovers a conventional keyed-state database beneath
// path. OctetDB owns the product files in that directory.
//
// Deprecated: use OpenCatalog. OpenKeyed exists only for the distinct
// pre-v0.2 global-key development format and never opens a catalog database.
func OpenKeyed(ctx context.Context, path string, options KeyedOptions) (*KeyedDB, error) {
	return openKeyed(ctx, path, options, keyedFormatContents)
}

func openKeyed(ctx context.Context, path string, options KeyedOptions, format string) (*KeyedDB, error) {
	if ctx == nil {
		return nil, &Error{Kind: ErrorInvalidInput, Op: "open_keyed", err: errors.New("nil context")}
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	normalized, err := normalizeKeyedOptions(options)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(path) == "" {
		return nil, &Error{Kind: ErrorInvalidInput, Op: "open_keyed", err: errors.New("path is required")}
	}
	db := &KeyedDB{
		path: path, options: normalized, records: make(map[string][]byte),
		dedupe: make(map[string]keyedWALRecord), admission: make(chan struct{}, 1), format: format,
	}
	if err := db.openStorage(); err != nil {
		return nil, err
	}
	db.admission <- struct{}{}
	db.commit.wake = make(chan struct{}, 1)
	db.commit.done = make(chan struct{})
	go db.runCommitCoordinator()
	return db, nil
}

// SubmitKeyed executes one atomic, durable, exactly deduplicated mutation.
//
// Deprecated: use Database.Mutate.
func (db *KeyedDB) SubmitKeyed(ctx context.Context, command KeyedCommand, mutation KeyedMutation) (KeyedDecision, error) {
	if db == nil || db.closed.Load() {
		return KeyedDecision{}, &Error{Kind: ErrorClosed, Op: "submit_keyed", err: errors.New("database is closed")}
	}
	if ctx == nil {
		return KeyedDecision{}, &Error{Kind: ErrorInvalidInput, Op: "submit_keyed", err: errors.New("nil context")}
	}
	if db.poisoned.Load() {
		return KeyedDecision{}, &Error{Kind: ErrorPoisoned, Op: "submit_keyed", err: errors.New("an earlier durability failure made the database unsafe; close and reopen the database")}
	}
	request := &keyedCommitRequest{ctx: ctx, command: command, mutation: mutation, result: make(chan keyedCommitResult, 1)}
	db.commit.mu.Lock()
	if db.closed.Load() {
		db.commit.mu.Unlock()
		return KeyedDecision{}, &Error{Kind: ErrorClosed, Op: "submit_keyed", err: errors.New("database is closed")}
	}
	db.commit.queue = append(db.commit.queue, request)
	db.commit.mu.Unlock()
	select {
	case db.commit.wake <- struct{}{}:
	default:
	}
	result := <-request.result
	if result.panicValue != nil {
		panic(result.panicValue)
	}
	return result.decision, result.err
}

func (db *KeyedDB) safelyExecuteKeyed(request *keyedCommitRequest) (record keyedWALRecord, decision KeyedDecision, err error, persist bool, panicValue any) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicValue = recovered
			record, decision, err, persist = keyedWALRecord{}, KeyedDecision{}, nil, false
		}
	}()
	record, decision, err, persist = db.executeKeyed(request)
	return
}

func (db *KeyedDB) executeKeyed(request *keyedCommitRequest) (keyedWALRecord, KeyedDecision, error, bool) {
	command, mutation := request.command, request.mutation
	if strings.TrimSpace(command.ID) == "" {
		return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorInvalidInput, Op: "submit_keyed", err: errors.New("command ID is required")}, false
	}
	if len(command.ID) > keyedMaxIdentityBytes {
		return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("command ID exceeds 4 KiB")}, false
	}
	if mutation == nil {
		return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorInvalidInput, Op: "submit_keyed", err: errors.New("mutation is required")}, false
	}
	if prior, ok := db.dedupe[command.ID]; ok {
		decision := prior.decision()
		decision.Duplicate = true
		return keyedWALRecord{}, decision, nil, false
	}

	tx := &KeyedTx{db: db, writes: make(map[string]*[]byte)}
	result, mutationErr := runKeyedMutation(tx, mutation)
	record := keyedWALRecord{Sequence: db.sequence + 1, CommandID: command.ID, Applied: true}
	var rejection *KeyedRejection
	if errors.As(mutationErr, &rejection) {
		if strings.TrimSpace(rejection.Code) == "" {
			return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorInvalidInput, Op: "submit_keyed", err: errors.New("rejection code is required")}, false
		}
		if len(rejection.Code) > keyedMaxCodeBytes {
			return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("rejection code exceeds 1 KiB")}, false
		}
		record.Applied = false
		record.Code = rejection.Code
		record.Result = append([]byte(nil), rejection.result...)
	} else if mutationErr != nil {
		return keyedWALRecord{}, KeyedDecision{}, mutationErr, false
	} else {
		encoded, err := json.Marshal(result)
		if err != nil {
			return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorInvalidInput, Op: "submit_keyed", err: fmt.Errorf("encode result: %w", err)}, false
		}
		if len(encoded) > db.options.MaxValueBytes {
			return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("result exceeds MaxValueBytes")}, false
		}
		record.Result = encoded
		mutations, err := tx.finalize()
		if err != nil {
			return keyedWALRecord{}, KeyedDecision{}, err, false
		}
		record.Mutations = mutations
	}
	if len(record.Result) > db.options.MaxValueBytes {
		return keyedWALRecord{}, KeyedDecision{}, &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("rejection result exceeds MaxValueBytes")}, false
	}
	return record, record.decision(), nil, true
}

func (db *KeyedDB) runCommitCoordinator() {
	defer close(db.commit.done)
	for {
		<-db.commit.wake
		for {
			db.commit.mu.Lock()
			if len(db.commit.queue) == 0 {
				closed := db.closed.Load()
				db.commit.mu.Unlock()
				if closed {
					return
				}
				break
			}
			limit := keyedMaxCommitGroup
			if db.commit.disabled {
				limit = 1
			}
			if limit > len(db.commit.queue) {
				limit = len(db.commit.queue)
			}
			group := db.commit.queue[:limit]
			if limit == len(db.commit.queue) {
				db.commit.queue = nil
			} else {
				db.commit.queue = db.commit.queue[limit:len(db.commit.queue):len(db.commit.queue)]
			}
			db.commit.mu.Unlock()
			db.processCommitGroup(group)
		}
	}
}

// processCommitGroup holds the pre-existing single database authority while it
// executes callbacks in queue order. Applying frames before the shared Sync is
// authority-private staging: no read can enter, and any storage failure poisons
// every operation until recovery rebuilds state from the WAL's durable prefix.
func (db *KeyedDB) processCommitGroup(group []*keyedCommitRequest) {
	<-db.admission
	defer db.leaveKeyed()
	var resultStorage [keyedMaxCommitGroup]keyedCommitResult
	var durableStorage [keyedMaxCommitGroup]bool
	results := resultStorage[:len(group)]
	durable := durableStorage[:len(group)]
	var staged map[string]int
	if len(group) > 1 {
		staged = make(map[string]int, len(group))
	}
	frames := uint64(0)
	var storageErr error
	for index, request := range group {
		if db.poisoned.Load() {
			results[index].err = &Error{Kind: ErrorPoisoned, Op: "submit_keyed", err: errors.New("an earlier durability failure made the database unsafe; close and reopen the database")}
			continue
		}
		select {
		case <-request.ctx.Done():
			results[index].err = request.ctx.Err()
			continue
		default:
		}
		if original, ok := staged[request.command.ID]; ok {
			if _, retained := db.dedupe[request.command.ID]; retained {
				decision := results[original].decision
				decision.Duplicate = true
				results[index].decision = decision
				durable[index] = true // depends on the original frame's shared Sync
				continue
			}
		}
		record, decision, err, persist, panicValue := db.safelyExecuteKeyed(request)
		results[index] = keyedCommitResult{decision: decision, err: err, panicValue: panicValue}
		if !persist {
			continue
		}
		if err := db.appendKeyedFrame(record); err != nil {
			storageErr = err
			db.poisoned.Store(true)
			results[index].err = err
			continue
		}
		db.applyKeyed(record)
		if staged != nil {
			staged[record.CommandID] = index
		}
		durable[index] = true
		frames++
	}
	if frames > 0 && storageErr == nil {
		storageErr = db.syncKeyed()
		if storageErr != nil {
			db.poisoned.Store(true)
		} else if db.afterSync != nil {
			db.afterSync()
		}
	}
	if storageErr != nil {
		for index := range results {
			if durable[index] {
				results[index] = keyedCommitResult{err: storageErr}
			}
		}
	} else if frames > 0 {
		db.commitStats.syncCalls.Add(1)
		db.commitStats.framesSynced.Add(frames)
		db.commitStats.groups.Add(1)
		db.commitStats.groupSizes[frames].Add(1)
		for {
			old := db.commitStats.maxGroup.Load()
			if old >= frames || db.commitStats.maxGroup.CompareAndSwap(old, frames) {
				break
			}
		}
	}
	for index, request := range group {
		request.result <- results[index]
	}
}

func runKeyedMutation(tx *KeyedTx, mutation KeyedMutation) (result any, err error) {
	defer func() { tx.invalid = true }()
	return mutation(tx)
}

// GetKeyed decodes one current record into destination.
//
// Deprecated: use Dataset.Get.
func (db *KeyedDB) GetKeyed(ctx context.Context, key string, destination any) (bool, error) {
	if err := db.enterKeyed(ctx, "get_keyed"); err != nil {
		return false, err
	}
	defer db.leaveKeyed()
	if destination == nil || key == "" {
		return false, &Error{Kind: ErrorInvalidInput, Op: "get_keyed", err: errors.New("key and destination are required")}
	}
	if len(key) > keyedMaxKeyBytes {
		return false, &Error{Kind: ErrorCapacity, Op: "get_keyed", err: errors.New("key exceeds 4 KiB")}
	}
	value, ok := db.records[key]
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return false, &Error{Kind: ErrorCorruption, Op: "get_keyed", err: err}
	}
	return true, nil
}

// SnapshotKeyed deterministically installs a current snapshot and resets the WAL.
//
// Deprecated: use Database.Snapshot.
func (db *KeyedDB) SnapshotKeyed(ctx context.Context) error {
	if err := db.enterKeyed(ctx, "snapshot_keyed"); err != nil {
		return err
	}
	defer db.leaveKeyed()
	if err := db.snapshotKeyed(); err != nil {
		db.poisoned.Store(true)
		return err
	}
	return nil
}

// Close snapshots current keyed state, closes storage, and is idempotent.
func (db *KeyedDB) Close() error {
	if db == nil || !db.closed.CompareAndSwap(false, true) {
		return nil
	}
	select {
	case db.commit.wake <- struct{}{}:
	default:
	}
	<-db.commit.done
	<-db.admission
	defer db.leaveKeyed()
	if db.poisoned.Load() {
		return db.wal.close()
	}
	if err := db.snapshotKeyed(); err != nil {
		_ = db.wal.close()
		return err
	}
	return db.wal.close()
}

// Get decodes a record visible to the current mutation, including its writes.
func (tx *KeyedTx) Get(key string, destination any) (bool, error) {
	if err := tx.valid("get"); err != nil {
		return false, err
	}
	if key == "" || destination == nil {
		return false, &Error{Kind: ErrorInvalidInput, Op: "keyed_tx_get", err: errors.New("key and destination are required")}
	}
	if len(key) > keyedMaxKeyBytes {
		return false, &Error{Kind: ErrorCapacity, Op: "keyed_tx_get", err: errors.New("key exceeds 4 KiB")}
	}
	if value, ok := tx.writes[key]; ok {
		if value == nil {
			return false, nil
		}
		if err := json.Unmarshal(*value, destination); err != nil {
			return false, &Error{Kind: ErrorInvalidInput, Op: "keyed_tx_get", err: err}
		}
		return true, nil
	}
	value, ok := tx.db.records[key]
	if !ok {
		return false, nil
	}
	if err := json.Unmarshal(value, destination); err != nil {
		return false, &Error{Kind: ErrorCorruption, Op: "keyed_tx_get", err: err}
	}
	return true, nil
}

// Put JSON-encodes and writes a value when the mutation commits.
func (tx *KeyedTx) Put(key string, value any) error {
	if err := tx.valid("put"); err != nil {
		return err
	}
	if key == "" {
		return &Error{Kind: ErrorInvalidInput, Op: "keyed_tx_put", err: errors.New("key is required")}
	}
	if len(key) > keyedMaxKeyBytes {
		return &Error{Kind: ErrorCapacity, Op: "keyed_tx_put", err: errors.New("key exceeds 4 KiB")}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return &Error{Kind: ErrorInvalidInput, Op: "keyed_tx_put", err: err}
	}
	if len(encoded) > tx.db.options.MaxValueBytes {
		return &Error{Kind: ErrorCapacity, Op: "keyed_tx_put", err: errors.New("value exceeds MaxValueBytes")}
	}
	copyValue := append([]byte(nil), encoded...)
	tx.writes[key] = &copyValue
	return nil
}

// Delete removes a key when the mutation commits.
func (tx *KeyedTx) Delete(key string) error {
	if err := tx.valid("delete"); err != nil {
		return err
	}
	if key == "" {
		return &Error{Kind: ErrorInvalidInput, Op: "keyed_tx_delete", err: errors.New("key is required")}
	}
	if len(key) > keyedMaxKeyBytes {
		return &Error{Kind: ErrorCapacity, Op: "keyed_tx_delete", err: errors.New("key exceeds 4 KiB")}
	}
	tx.writes[key] = nil
	return nil
}

func (tx *KeyedTx) valid(op string) error {
	if tx == nil || tx.db == nil || tx.invalid {
		return &Error{Kind: ErrorInvalidInput, Op: "keyed_tx_" + op, err: errors.New("transaction is no longer active")}
	}
	return nil
}

func (tx *KeyedTx) finalize() ([]keyedMutation, error) {
	keys := make([]string, 0, len(tx.writes))
	for key := range tx.writes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	count := len(tx.db.records)
	total := 0
	mutations := make([]keyedMutation, 0, len(keys))
	for _, key := range keys {
		value := tx.writes[key]
		_, existed := tx.db.records[key]
		if value == nil {
			if existed {
				count--
			}
			mutations = append(mutations, keyedMutation{Key: key, Delete: true})
			continue
		}
		if !existed {
			count++
		}
		total += len(key) + len(*value)
		mutations = append(mutations, keyedMutation{Key: key, Value: append([]byte(nil), (*value)...)})
	}
	if count > tx.db.options.MaxRecords {
		return nil, &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("record capacity exceeded")}
	}
	if total > tx.db.options.MaxTransactionBytes {
		return nil, &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("transaction exceeds MaxTransactionBytes")}
	}
	return mutations, nil
}

func normalizeKeyedOptions(options KeyedOptions) (KeyedOptions, error) {
	if options.MaxRecords < 0 || options.DedupeHorizon < 0 || options.MaxValueBytes < 0 || options.MaxTransactionBytes < 0 {
		return KeyedOptions{}, &Error{Kind: ErrorInvalidInput, Op: "open_keyed", err: errors.New("bounds cannot be negative")}
	}
	if options.MaxRecords == 0 {
		options.MaxRecords = 100_000
	}
	if options.DedupeHorizon == 0 {
		options.DedupeHorizon = 100_000
	}
	if options.MaxValueBytes == 0 {
		options.MaxValueBytes = 1 << 20
	}
	if options.MaxTransactionBytes == 0 {
		options.MaxTransactionBytes = 4 << 20
	}
	if options.MaxTransactionBytes < options.MaxValueBytes {
		return KeyedOptions{}, &Error{Kind: ErrorInvalidInput, Op: "open_keyed", err: errors.New("MaxTransactionBytes must be at least MaxValueBytes")}
	}
	return options, nil
}

func (db *KeyedDB) enterKeyed(ctx context.Context, op string) error {
	if db == nil || db.closed.Load() {
		return &Error{Kind: ErrorClosed, Op: op, err: errors.New("database is closed")}
	}
	if ctx == nil {
		return &Error{Kind: ErrorInvalidInput, Op: op, err: errors.New("nil context")}
	}
	if db.poisoned.Load() {
		return &Error{Kind: ErrorPoisoned, Op: op, err: errors.New("an earlier durability failure made the database unsafe; close and reopen the database")}
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
		db.leaveKeyed()
		return &Error{Kind: ErrorClosed, Op: op, err: errors.New("database is closed")}
	}
	if db.poisoned.Load() {
		db.leaveKeyed()
		return &Error{Kind: ErrorPoisoned, Op: op, err: errors.New("an earlier durability failure made writes unsafe; close and reopen the database")}
	}
	return nil
}

func (db *KeyedDB) leaveKeyed() { db.admission <- struct{}{} }

func (record keyedWALRecord) decision() KeyedDecision {
	return KeyedDecision{Sequence: record.Sequence, CommandID: record.CommandID, Applied: record.Applied, Code: record.Code, Result: append([]byte(nil), record.Result...)}
}
