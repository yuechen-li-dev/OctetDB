package engine

// This file is the bounded OCTETDB-LAYOUT-M0 alternate lane. It deliberately
// does not modify Engine, Store, the Oct lane, or the M2 WAL format.

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	c2WALVersion      = uint32(1)
	c2SnapshotVersion = uint32(1)
	c2HeaderSize      = 24
	c2SnapshotHeader  = 52
	c2MaxFrameBytes   = 64 << 20
)

var (
	c2WALMagic      = [8]byte{'O', 'C', 'T', 'C', '2', 'W', 'A', 'L'}
	c2SnapshotMagic = [8]byte{'O', 'C', 'T', 'C', '2', 'S', 'N', 'P'}
)

// SpecializedConfig is intentionally separate from Config so the experimental
// format cannot be selected accidentally by the frozen M2 constructors.
type SpecializedConfig struct {
	StorageDir   string
	Durability   DurabilityMode
	DedupeWindow int
	AccountHint  int
	RecordHint   int
}

// SpecializedEngine is a single-owner, safe-Go batch engine. External account
// IDs remain arbitrary; the map only resolves them to stable dense slots.
// Authoritative records and behavioral state are stored in contiguous slices.
type SpecializedEngine struct {
	mu sync.Mutex

	cfg       SpecializedConfig
	ids       map[AccountID]uint32
	accounts  []Account
	present   []bool
	agents    []c2AgentState
	ledger    []LedgerEntry
	scratch   []Account
	marks     []uint32
	epoch     uint32
	results   map[string]Result
	order     []string
	orderHead int
	sequence  uint64
	recordBuf []c2Record
	batchIDs  map[string]int

	wal           *c2WAL
	closed        bool
	poisoned      error
	recoveryStats RecoveryStats
}

type c2AgentState struct {
	Pending pendingTransfer
	Turns   int
}

type c2Record struct {
	sequence                 uint64
	commandID                string
	kind                     int
	a, b                     AccountID
	amount                   int
	accepted                 bool
	reason, effect, turns    int
	newBalanceA, newBalanceB int
	newStatus                int
	expectedA, expectedB     uint64
	pendingActive            bool
	pendingTarget            AccountID
	pendingAmount            int
}

type c2WAL struct {
	file  *os.File
	mode  DurabilityMode
	path  string
	buf   []byte
	stats StorageStats
}

func OpenSpecialized(cfg SpecializedConfig) (*SpecializedEngine, error) {
	started := time.Now()
	if cfg.DedupeWindow <= 0 {
		cfg.DedupeWindow = 100_000
	}
	if cfg.AccountHint < 0 || cfg.RecordHint < 0 {
		return nil, errors.New("negative C2 capacity hint")
	}
	if cfg.RecordHint == 0 {
		cfg.RecordHint = cfg.AccountHint
	}
	if cfg.Durability == SyncEach {
		return nil, errors.New("C2 supports MemoryOnly or one durable Sync per offered batch")
	}
	e := &SpecializedEngine{
		cfg:       cfg,
		ids:       make(map[AccountID]uint32, cfg.AccountHint),
		accounts:  make([]Account, 0, cfg.AccountHint),
		present:   make([]bool, 0, cfg.AccountHint),
		agents:    make([]c2AgentState, 0, cfg.AccountHint),
		scratch:   make([]Account, 0, cfg.AccountHint),
		marks:     make([]uint32, 0, cfg.AccountHint),
		ledger:    make([]LedgerEntry, 0, cfg.RecordHint),
		results:   make(map[string]Result, cfg.DedupeWindow),
		order:     make([]string, 0, cfg.DedupeWindow),
		recordBuf: make([]c2Record, 0, 512),
		batchIDs:  make(map[string]int, 512),
	}
	if cfg.StorageDir != "" {
		if err := os.MkdirAll(cfg.StorageDir, 0o755); err != nil {
			return nil, err
		}
		if err := e.loadSnapshot(); err != nil {
			return nil, err
		}
	}
	scanStarted := time.Now()
	w, records, scanned, err := openC2WAL(cfg.StorageDir, cfg.Durability, e.sequence)
	if err != nil {
		return nil, err
	}
	e.wal = w
	e.recoveryStats.WALScan = time.Since(scanStarted)
	e.recoveryStats.WALBytesScanned = scanned
	e.recoveryStats.RecordsReplayed = len(records)
	for _, record := range records {
		if record.sequence != e.sequence+1 {
			_ = w.close()
			return nil, fmt.Errorf("C2 non-monotonic sequence %d after %d", record.sequence, e.sequence)
		}
		if err := e.applyRecovered(record); err != nil {
			_ = w.close()
			return nil, err
		}
	}
	e.recoveryStats.TotalReady = time.Since(started)
	return e, nil
}

// SubmitBatch preserves offered batch identity through decision, one binary WAL
// frame, one Sync, apply, and result production. Commands are evaluated in
// order, so overlapping keys retain ordinary serial transfer semantics.
func (e *SpecializedEngine) SubmitBatch(commands []Command) ([]Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, &RuntimeError{Kind: EngineClosed, Err: errClosed}
	}
	if e.poisoned != nil {
		return nil, &RuntimeError{Kind: DurabilityWriteFailed, Err: e.poisoned}
	}
	if len(commands) == 0 {
		return []Result{}, nil
	}
	for _, command := range commands {
		if err := ValidateCommand(command); err != nil {
			return nil, err
		}
	}
	if e.epoch == ^uint32(0) {
		clear(e.marks)
		e.epoch = 1
	} else {
		e.epoch++
	}
	records := e.recordBuf[:0]
	if cap(records) < len(commands) {
		records = make([]c2Record, 0, len(commands))
	}
	results := make([]Result, len(commands))
	clear(e.batchIDs)
	batchIDs := e.batchIDs
	for i, command := range commands {
		if prior, ok := e.results[command.ID]; ok {
			prior.Duplicate = true
			results[i] = prior
			continue
		}
		if priorIndex, ok := batchIDs[command.ID]; ok {
			prior := results[priorIndex]
			prior.Duplicate = true
			results[i] = prior
			continue
		}
		aSlot, aok := e.slot(command.Account, isKind(command.Kind, Create))
		var bSlot uint32
		var bok bool
		if command.Other != 0 {
			bSlot, bok = e.slot(command.Other, false)
		}
		a := e.accountFor(aSlot, aok)
		b := e.accountFor(bSlot, bok)
		state := c2AgentState{}
		if aok {
			state = e.agents[aSlot]
		}
		aExists := aok && (e.present[aSlot] || e.epoch != 0 && e.marks[aSlot] == e.epoch)
		bExists := bok && (e.present[bSlot] || e.epoch != 0 && e.marks[bSlot] == e.epoch)
		d := decideGo(command, a, aExists, b, bExists, state.Pending)
		next := state.Pending
		if isKind(command.Kind, BeginTransfer) && d.accepted {
			next = pendingTransfer{Active: true, Target: command.Other, Amount: command.Amount}
		}
		if isKind(command.Kind, Confirm) || isKind(command.Kind, Cancel) {
			next = pendingTransfer{}
		}
		seq := e.sequence + uint64(len(records)) + 1
		turns := state.Turns + 1
		result := Result{Sequence: seq, CommandID: command.ID, Accepted: d.accepted, ReasonTag: d.reason, EffectTag: d.effect, TransitionCount: turns}
		record := c2Record{sequence: seq, commandID: command.ID, kind: command.Kind.Tag, a: command.Account, b: command.Other, amount: command.Amount, accepted: d.accepted, reason: d.reason, effect: d.effect, turns: turns, newBalanceA: d.balanceA, newBalanceB: d.balanceB, newStatus: d.status, expectedA: a.Version, expectedB: b.Version, pendingActive: next.Active, pendingTarget: next.Target, pendingAmount: next.Amount}
		records = append(records, record)
		results[i] = result
		batchIDs[command.ID] = i
		if d.accepted {
			e.stageEffect(aSlot, bSlot, record)
		}
		if aok {
			e.agents[aSlot] = c2AgentState{Pending: next, Turns: turns}
		}
	}
	if len(records) == 0 {
		return results, nil
	}
	if err := e.wal.appendBatch(records); err != nil {
		e.recordBuf = records[:0]
		e.poisoned = err
		// Agent state is small and was staged eagerly. Rebuild it from durable
		// state on a write failure rather than permitting the poisoned engine to
		// expose or commit further work.
		return nil, &RuntimeError{Kind: DurabilityWriteFailed, Err: err}
	}
	for _, record := range records {
		e.applyDurable(record)
	}
	e.recordBuf = records[:0]
	return results, nil
}

func (e *SpecializedEngine) Submit(command Command) (Result, error) {
	results, err := e.SubmitBatch([]Command{command})
	if err != nil {
		return Result{}, err
	}
	return results[0], nil
}

func (e *SpecializedEngine) Account(id AccountID) (Account, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	slot, ok := e.ids[id]
	if !ok || !e.present[slot] {
		return Account{}, false
	}
	return e.accounts[slot], true
}

func (e *SpecializedEngine) StorageMetrics() StorageStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal == nil {
		return StorageStats{}
	}
	return e.wal.stats
}

func (e *SpecializedEngine) RecoveryMetrics() RecoveryStats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.recoveryStats
}

func (e *SpecializedEngine) PopulationBytes() (accounts, agents, mappingEstimate uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	// Slice payloads are exact. Map bucket overhead is runtime-specific, so the
	// returned mapping number is explicitly a key/value lower-bound estimate.
	return uint64(cap(e.accounts)) * 32, uint64(cap(e.agents)) * 32, uint64(len(e.ids)) * 12
}

func (e *SpecializedEngine) LedgerLen() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.ledger)
}

func (e *SpecializedEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.wal.close()
}

func (e *SpecializedEngine) slot(id AccountID, create bool) (uint32, bool) {
	if slot, ok := e.ids[id]; ok {
		return slot, true
	}
	if !create {
		return 0, false
	}
	slot := uint32(len(e.accounts))
	e.ids[id] = slot
	e.accounts = append(e.accounts, Account{ID: id})
	e.present = append(e.present, false)
	e.agents = append(e.agents, c2AgentState{})
	e.scratch = append(e.scratch, Account{})
	e.marks = append(e.marks, 0)
	return slot, true
}

func (e *SpecializedEngine) accountFor(slot uint32, ok bool) Account {
	if !ok {
		return Account{}
	}
	if e.epoch != 0 && e.marks[slot] == e.epoch {
		return e.scratch[slot]
	}
	return e.accounts[slot]
}

func (e *SpecializedEngine) stageEffect(aSlot, bSlot uint32, r c2Record) {
	switch r.effect {
	case 1:
		e.scratch[aSlot] = Account{ID: r.a, Balance: r.newBalanceA, Status: StatusOpen, Version: 1}
		e.marks[aSlot] = e.epoch
	case 2:
		a := e.accountFor(aSlot, true)
		a.Balance, a.Version = r.newBalanceA, a.Version+1
		e.scratch[aSlot], e.marks[aSlot] = a, e.epoch
	case 3:
		a, b := e.accountFor(aSlot, true), e.accountFor(bSlot, true)
		a.Balance, a.Version = r.newBalanceA, a.Version+1
		b.Balance, b.Version = r.newBalanceB, b.Version+1
		e.scratch[aSlot], e.marks[aSlot] = a, e.epoch
		e.scratch[bSlot], e.marks[bSlot] = b, e.epoch
	case 4:
		a := e.accountFor(aSlot, true)
		a.Status, a.Version = r.newStatus, a.Version+1
		e.scratch[aSlot], e.marks[aSlot] = a, e.epoch
	}
}

func (e *SpecializedEngine) applyDurable(r c2Record) {
	aSlot, _ := e.slot(r.a, true)
	var bSlot uint32
	if r.b != 0 {
		bSlot, _ = e.slot(r.b, false)
	}
	if r.accepted {
		switch r.effect {
		case 1:
			e.accounts[aSlot] = Account{ID: r.a, Balance: r.newBalanceA, Status: StatusOpen, Version: 1}
			e.present[aSlot] = true
		case 2:
			a := e.accounts[aSlot]
			a.Balance, a.Version = r.newBalanceA, a.Version+1
			e.accounts[aSlot] = a
		case 3:
			a, b := e.accounts[aSlot], e.accounts[bSlot]
			a.Balance, a.Version = r.newBalanceA, a.Version+1
			b.Balance, b.Version = r.newBalanceB, b.Version+1
			e.accounts[aSlot], e.accounts[bSlot] = a, b
		case 4:
			a := e.accounts[aSlot]
			a.Status, a.Version = r.newStatus, a.Version+1
			e.accounts[aSlot] = a
		}
		if r.effect >= 1 && r.effect <= 4 {
			e.ledger = append(e.ledger, LedgerEntry{Sequence: r.sequence, CommandID: r.commandID, From: r.a, To: r.b, Amount: r.amount, EffectTag: r.effect})
		}
	}
	e.agents[aSlot] = c2AgentState{Pending: pendingTransfer{Active: r.pendingActive, Target: r.pendingTarget, Amount: r.pendingAmount}, Turns: r.turns}
	result := Result{Sequence: r.sequence, CommandID: r.commandID, Accepted: r.accepted, ReasonTag: r.reason, EffectTag: r.effect, TransitionCount: r.turns}
	e.remember(r.commandID, result)
	e.sequence = r.sequence
}

func (e *SpecializedEngine) applyRecovered(r c2Record) error {
	if prior, ok := e.results[r.commandID]; ok && prior.Sequence != r.sequence {
		return fmt.Errorf("C2 duplicate durable command ID %q", r.commandID)
	}
	aSlot, aok := e.slot(r.a, r.effect == 1 && r.accepted)
	var bSlot uint32
	var bok bool
	if r.b != 0 {
		bSlot, bok = e.slot(r.b, false)
	}
	a := e.accountFor(aSlot, aok)
	b := e.accountFor(bSlot, bok)
	if a.Version != r.expectedA || b.Version != r.expectedB {
		return fmt.Errorf("C2 version mismatch at sequence %d", r.sequence)
	}
	e.applyDurable(r)
	return nil
}

func (e *SpecializedEngine) remember(id string, result Result) {
	e.results[id] = result
	e.order = append(e.order, id)
	if len(e.order)-e.orderHead > e.cfg.DedupeWindow {
		delete(e.results, e.order[e.orderHead])
		e.orderHead++
	}
	if e.orderHead >= e.cfg.DedupeWindow && e.orderHead*2 >= len(e.order) {
		n := copy(e.order, e.order[e.orderHead:])
		clear(e.order[n:])
		e.order = e.order[:n]
		e.orderHead = 0
	}
}

func openC2WAL(dir string, mode DurabilityMode, after uint64) (*c2WAL, []c2Record, int64, error) {
	w := &c2WAL{mode: mode}
	if mode == MemoryOnly {
		return w, nil, 0, nil
	}
	if dir == "" {
		return nil, nil, 0, errors.New("C2 durable mode requires StorageDir")
	}
	w.path = filepath.Join(dir, "c2.wal")
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, 0, err
	}
	w.file = f
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, 0, err
	}
	if info.Size() == 0 {
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, nil, 0, err
		}
	}
	records, valid, scanned, err := scanC2WAL(f, after)
	if err != nil {
		f.Close()
		return nil, nil, scanned, err
	}
	if err := f.Truncate(valid); err != nil {
		f.Close()
		return nil, nil, scanned, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, nil, scanned, err
	}
	return w, records, scanned, nil
}

func (w *c2WAL) writeHeader() error {
	header := make([]byte, c2HeaderSize)
	copy(header[:8], c2WALMagic[:])
	binary.LittleEndian.PutUint32(header[8:12], c2WALVersion)
	copy(header[12:20], []byte("SAFEGO01"))
	binary.LittleEndian.PutUint32(header[20:24], crc32.ChecksumIEEE(header[:20]))
	if _, err := w.file.WriteAt(header, 0); err != nil {
		return err
	}
	w.stats.WALBytesWritten += uint64(len(header))
	return nil
}

func (w *c2WAL) appendBatch(records []c2Record) error {
	if w.mode == MemoryOnly {
		w.stats.Committed += uint64(len(records))
		return nil
	}
	w.buf = w.buf[:0]
	w.buf = append(w.buf, 0, 0, 0, 0)
	w.buf = appendU32(w.buf, uint32(len(records)))
	for _, r := range records {
		w.buf = appendC2Record(w.buf, r)
	}
	payloadLen := len(w.buf) - 4
	if payloadLen > c2MaxFrameBytes {
		return errors.New("C2 WAL batch exceeds bounded frame size")
	}
	binary.LittleEndian.PutUint32(w.buf[:4], uint32(payloadLen))
	crc := crc32.ChecksumIEEE(w.buf[4:])
	w.buf = appendU32(w.buf, crc)
	if _, err := w.file.Write(w.buf); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.stats.WALBytesWritten += uint64(len(w.buf))
	w.stats.Syncs++
	w.stats.Committed += uint64(len(records))
	return nil
}

func scanC2WAL(f *os.File, after uint64) ([]c2Record, int64, int64, error) {
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(data) < c2HeaderSize || string(data[:8]) != string(c2WALMagic[:]) || binary.LittleEndian.Uint32(data[8:12]) != c2WALVersion || binary.LittleEndian.Uint32(data[20:24]) != crc32.ChecksumIEEE(data[:20]) {
		return nil, 0, int64(len(data)), errors.New("invalid C2 WAL header")
	}
	pos := c2HeaderSize
	var records []c2Record
	for pos < len(data) {
		start := pos
		if len(data)-pos < 4 {
			return records, int64(start), int64(len(data)), nil
		}
		length := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4
		if length < 4 || length > c2MaxFrameBytes {
			return nil, 0, int64(len(data)), fmt.Errorf("invalid C2 WAL frame length at %d", start)
		}
		if len(data)-pos < length+4 {
			return records, int64(start), int64(len(data)), nil
		}
		payload := data[pos : pos+length]
		pos += length
		want := binary.LittleEndian.Uint32(data[pos : pos+4])
		pos += 4
		if crc32.ChecksumIEEE(payload) != want {
			return nil, 0, int64(len(data)), fmt.Errorf("C2 WAL checksum failure at %d", start)
		}
		count := int(binary.LittleEndian.Uint32(payload[:4]))
		cursor := 4
		for i := 0; i < count; i++ {
			r, next, err := parseC2Record(payload, cursor)
			if err != nil {
				return nil, 0, int64(len(data)), fmt.Errorf("C2 WAL record %d at %d: %w", i, start, err)
			}
			cursor = next
			if r.sequence > after {
				records = append(records, r)
			}
		}
		if cursor != len(payload) {
			return nil, 0, int64(len(data)), fmt.Errorf("C2 WAL trailing bytes at %d", start)
		}
	}
	return records, int64(pos), int64(len(data)), nil
}

func appendC2Record(dst []byte, r c2Record) []byte {
	dst = appendU64(dst, r.sequence)
	dst = appendU32(dst, uint32(len(r.commandID)))
	dst = append(dst, r.commandID...)
	dst = appendI64(dst, int64(r.kind))
	dst = appendU64(dst, uint64(r.a))
	dst = appendU64(dst, uint64(r.b))
	dst = appendI64(dst, int64(r.amount))
	var flags byte
	if r.accepted {
		flags |= 1
	}
	if r.pendingActive {
		flags |= 2
	}
	dst = append(dst, flags)
	for _, value := range []int{r.reason, r.effect, r.turns, r.newBalanceA, r.newBalanceB, r.newStatus} {
		dst = appendI64(dst, int64(value))
	}
	dst = appendU64(dst, r.expectedA)
	dst = appendU64(dst, r.expectedB)
	dst = appendU64(dst, uint64(r.pendingTarget))
	dst = appendI64(dst, int64(r.pendingAmount))
	return dst
}

func parseC2Record(src []byte, pos int) (c2Record, int, error) {
	const fixed = 8 + 4 + 8 + 8 + 8 + 1 + 6*8 + 8 + 8 + 8 + 8
	if len(src)-pos < fixed {
		return c2Record{}, pos, io.ErrUnexpectedEOF
	}
	var r c2Record
	r.sequence, pos = takeU64(src, pos)
	idLen, next := takeU32(src, pos)
	pos = next
	if idLen == 0 || int(idLen) > len(src)-pos || len(src)-pos-int(idLen) < fixed-12 {
		return c2Record{}, pos, errors.New("invalid command ID length")
	}
	r.commandID = string(src[pos : pos+int(idLen)])
	pos += int(idLen)
	kind, next := takeI64(src, pos)
	r.kind, pos = int(kind), next
	a, next := takeU64(src, pos)
	r.a, pos = AccountID(a), next
	b, next := takeU64(src, pos)
	r.b, pos = AccountID(b), next
	amount, next := takeI64(src, pos)
	r.amount, pos = int(amount), next
	flags := src[pos]
	pos++
	r.accepted, r.pendingActive = flags&1 != 0, flags&2 != 0
	values := []*int{&r.reason, &r.effect, &r.turns, &r.newBalanceA, &r.newBalanceB, &r.newStatus}
	for _, target := range values {
		value, next := takeI64(src, pos)
		*target, pos = int(value), next
	}
	r.expectedA, pos = takeU64(src, pos)
	r.expectedB, pos = takeU64(src, pos)
	target, next := takeU64(src, pos)
	r.pendingTarget, pos = AccountID(target), next
	amount64, next := takeI64(src, pos)
	r.pendingAmount, pos = int(amount64), next
	return r, pos, nil
}

func (w *c2WAL) close() error {
	if w == nil || w.file == nil {
		return nil
	}
	return w.file.Close()
}

func appendU32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}
func appendU64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}
func appendI64(dst []byte, v int64) []byte { return appendU64(dst, uint64(v)) }
func takeU32(src []byte, pos int) (uint32, int) {
	return binary.LittleEndian.Uint32(src[pos : pos+4]), pos + 4
}
func takeU64(src []byte, pos int) (uint64, int) {
	return binary.LittleEndian.Uint64(src[pos : pos+8]), pos + 8
}
func takeI64(src []byte, pos int) (int64, int) { v, next := takeU64(src, pos); return int64(v), next }
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Snapshot writes a C2-only deterministic binary snapshot, installs it
// atomically, and resets the C2 WAL only after the snapshot is durable.
func (e *SpecializedEngine) Snapshot() (string, int64, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.cfg.StorageDir == "" || e.cfg.Durability == MemoryOnly {
		return "", 0, errors.New("C2 snapshots require durable StorageDir")
	}
	payload := e.encodeSnapshot()
	path := filepath.Join(e.cfg.StorageDir, "c2.snapshot")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return "", 0, err
	}
	if _, err = f.Write(payload); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return "", 0, err
	}
	// POSIX rename durability requires syncing the containing directory.
	// Windows does not support Sync on directory handles opened this way.
	if runtime.GOOS != "windows" {
		directory, err := os.Open(e.cfg.StorageDir)
		if err != nil {
			return path, int64(len(payload)), err
		}
		err = directory.Sync()
		closeErr := directory.Close()
		if err != nil {
			return path, int64(len(payload)), err
		}
		if closeErr != nil {
			return path, int64(len(payload)), closeErr
		}
	}
	if err = e.wal.file.Truncate(0); err != nil {
		return path, int64(len(payload)), err
	}
	if _, err = e.wal.file.Seek(0, io.SeekStart); err != nil {
		return path, int64(len(payload)), err
	}
	if err = e.wal.writeHeader(); err != nil {
		return path, int64(len(payload)), err
	}
	if err = e.wal.file.Sync(); err != nil {
		return path, int64(len(payload)), err
	}
	if _, err = e.wal.file.Seek(0, io.SeekEnd); err != nil {
		return path, int64(len(payload)), err
	}
	e.wal.stats.SnapshotBytesWritten += uint64(len(payload))
	return path, int64(len(payload)), nil
}

func (e *SpecializedEngine) encodeSnapshot() []byte {
	b := make([]byte, c2SnapshotHeader)
	copy(b[:8], c2SnapshotMagic[:])
	binary.LittleEndian.PutUint32(b[8:12], c2SnapshotVersion)
	b = appendU64(b, e.sequence)
	b = appendU32(b, uint32(len(e.accounts)))
	for i, a := range e.accounts {
		b = appendU64(b, uint64(a.ID))
		b = appendI64(b, int64(a.Balance))
		b = appendI64(b, int64(a.Status))
		b = appendU64(b, a.Version)
		var flags byte
		if e.present[i] {
			flags |= 1
		}
		if e.agents[i].Pending.Active {
			flags |= 2
		}
		b = append(b, flags)
		b = appendU64(b, uint64(e.agents[i].Pending.Target))
		b = appendI64(b, int64(e.agents[i].Pending.Amount))
		b = appendI64(b, int64(e.agents[i].Turns))
	}
	b = appendU64(b, uint64(len(e.ledger)))
	for _, entry := range e.ledger {
		b = appendU64(b, entry.Sequence)
		b = appendU32(b, uint32(len(entry.CommandID)))
		b = append(b, entry.CommandID...)
		b = appendU64(b, uint64(entry.From))
		b = appendU64(b, uint64(entry.To))
		b = appendI64(b, int64(entry.Amount))
		b = appendI64(b, int64(entry.EffectTag))
	}
	active := e.order[e.orderHead:]
	b = appendU32(b, uint32(e.cfg.DedupeWindow))
	b = appendU32(b, uint32(len(active)))
	for _, id := range active {
		r := e.results[id]
		b = appendU32(b, uint32(len(id)))
		b = append(b, id...)
		b = appendU64(b, r.Sequence)
		var accepted byte
		if r.Accepted {
			accepted = 1
		}
		b = append(b, accepted)
		b = appendI64(b, int64(r.ReasonTag))
		b = appendI64(b, int64(r.EffectTag))
		b = appendI64(b, int64(r.TransitionCount))
	}
	binary.LittleEndian.PutUint64(b[12:20], uint64(len(b)-c2SnapshotHeader))
	digest := sha256.Sum256(b[c2SnapshotHeader:])
	copy(b[20:52], digest[:])
	return b
}

func (e *SpecializedEngine) loadSnapshot() error {
	started := time.Now()
	path := filepath.Join(e.cfg.StorageDir, "c2.snapshot")
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data[minInt(len(data), c2SnapshotHeader):])
	if len(data) < c2SnapshotHeader || string(data[:8]) != string(c2SnapshotMagic[:]) || binary.LittleEndian.Uint32(data[8:12]) != c2SnapshotVersion || int(binary.LittleEndian.Uint64(data[12:20])) != len(data)-c2SnapshotHeader || string(data[20:52]) != string(digest[:]) {
		return errors.New("invalid C2 snapshot")
	}
	pos := c2SnapshotHeader
	if len(data)-pos < 12 {
		return io.ErrUnexpectedEOF
	}
	e.sequence, pos = takeU64(data, pos)
	count32, next := takeU32(data, pos)
	pos = next
	for i := 0; i < int(count32); i++ {
		if len(data)-pos < 57 {
			return io.ErrUnexpectedEOF
		}
		id, next := takeU64(data, pos)
		pos = next
		balance, next := takeI64(data, pos)
		pos = next
		status, next := takeI64(data, pos)
		pos = next
		version, next := takeU64(data, pos)
		pos = next
		flags := data[pos]
		pos++
		target, next := takeU64(data, pos)
		pos = next
		amount, next := takeI64(data, pos)
		pos = next
		turns, next := takeI64(data, pos)
		pos = next
		slot, _ := e.slot(AccountID(id), true)
		e.accounts[slot] = Account{ID: AccountID(id), Balance: int(balance), Status: int(status), Version: version}
		e.present[slot] = flags&1 != 0
		e.agents[slot] = c2AgentState{Pending: pendingTransfer{Active: flags&2 != 0, Target: AccountID(target), Amount: int(amount)}, Turns: int(turns)}
	}
	if len(data)-pos < 8 {
		return io.ErrUnexpectedEOF
	}
	ledgerCount, next := takeU64(data, pos)
	pos = next
	if ledgerCount > uint64(len(data)/44) {
		return errors.New("invalid C2 snapshot ledger count")
	}
	e.ledger = make([]LedgerEntry, 0, int(ledgerCount))
	var priorLedgerSequence uint64
	for i := uint64(0); i < ledgerCount; i++ {
		if len(data)-pos < 12 {
			return io.ErrUnexpectedEOF
		}
		sequence, next := takeU64(data, pos)
		pos = next
		idLen, next := takeU32(data, pos)
		pos = next
		if idLen == 0 || int(idLen) > len(data)-pos || len(data)-pos-int(idLen) < 32 {
			return io.ErrUnexpectedEOF
		}
		id := string(data[pos : pos+int(idLen)])
		pos += int(idLen)
		from, next := takeU64(data, pos)
		pos = next
		to, next := takeU64(data, pos)
		pos = next
		amount, next := takeI64(data, pos)
		pos = next
		effect, next := takeI64(data, pos)
		pos = next
		if sequence <= priorLedgerSequence || sequence > e.sequence || id == "" {
			return errors.New("invalid C2 snapshot ledger order or identity")
		}
		priorLedgerSequence = sequence
		e.ledger = append(e.ledger, LedgerEntry{Sequence: sequence, CommandID: id, From: AccountID(from), To: AccountID(to), Amount: int(amount), EffectTag: int(effect)})
	}
	if len(data)-pos < 8 {
		return io.ErrUnexpectedEOF
	}
	horizon, next := takeU32(data, pos)
	pos = next
	resultCount, next := takeU32(data, pos)
	pos = next
	if int(horizon) != e.cfg.DedupeWindow || int(resultCount) > e.cfg.DedupeWindow {
		return errors.New("C2 snapshot dedupe horizon mismatch")
	}
	seenResults := make(map[string]struct{}, int(resultCount))
	var priorResultSequence uint64
	for i := 0; i < int(resultCount); i++ {
		if len(data)-pos < 4 {
			return io.ErrUnexpectedEOF
		}
		idLen, next := takeU32(data, pos)
		pos = next
		if idLen == 0 || int(idLen) > len(data)-pos || len(data)-pos-int(idLen) < 33 {
			return io.ErrUnexpectedEOF
		}
		id := string(data[pos : pos+int(idLen)])
		pos += int(idLen)
		seq, next := takeU64(data, pos)
		pos = next
		accepted := data[pos] != 0
		pos++
		reason, next := takeI64(data, pos)
		pos = next
		effect, next := takeI64(data, pos)
		pos = next
		turns, next := takeI64(data, pos)
		pos = next
		if _, exists := seenResults[id]; exists || seq <= priorResultSequence || seq > e.sequence {
			return errors.New("invalid C2 snapshot dedupe order or identity")
		}
		seenResults[id] = struct{}{}
		priorResultSequence = seq
		e.remember(id, Result{Sequence: seq, CommandID: id, Accepted: accepted, ReasonTag: int(reason), EffectTag: int(effect), TransitionCount: int(turns)})
	}
	if pos != len(data) {
		return errors.New("C2 snapshot trailing bytes")
	}
	e.recoveryStats.SnapshotSequence, e.recoveryStats.SnapshotBytes, e.recoveryStats.SnapshotDecode = e.sequence, int64(len(data)), time.Since(started)
	return nil
}

func (e *SpecializedEngine) SortedAccounts() []Account {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Account, 0, len(e.accounts))
	for i, a := range e.accounts {
		if e.present[i] {
			out = append(out, a)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
