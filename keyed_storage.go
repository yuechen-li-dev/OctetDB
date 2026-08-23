package octetdb

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
)

const keyedFormatContents = "OCTETDB\nformat=1\nmodel=keyed-json-v1\nengine=safe-go\n"
const keyedSnapshotMagic = "OCTKEY01"
const maxKeyedFrameBytes = 64 << 20
const keyedMaxIdentityBytes = 4 << 10
const keyedMaxKeyBytes = 4 << 10
const keyedMaxCodeBytes = 1 << 10

type keyedMutation struct {
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"`
	Delete bool   `json:"delete,omitempty"`
}

type keyedWALRecord struct {
	Sequence  uint64          `json:"sequence"`
	CommandID string          `json:"command_id"`
	Applied   bool            `json:"applied"`
	Code      string          `json:"code,omitempty"`
	Result    []byte          `json:"result,omitempty"`
	Mutations []keyedMutation `json:"mutations,omitempty"`
}

type keyedSnapshot struct {
	Sequence uint64            `json:"sequence"`
	Records  map[string][]byte `json:"records"`
	Dedupe   []keyedWALRecord  `json:"dedupe"`
}

type keyedSnapshotEnvelope struct {
	Magic   string          `json:"magic"`
	Version int             `json:"version"`
	Payload json.RawMessage `json:"payload"`
	SHA256  string          `json:"sha256"`
}

type keyedWAL struct{ file *os.File }

func (db *KeyedDB) openStorage() error {
	if err := ensureKeyedFormat(db.path, db.format); err != nil {
		return err
	}
	if err := db.loadKeyedSnapshot(); err != nil {
		return err
	}
	walPath := filepath.Join(db.path, "wal")
	file, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	db.wal.file = file
	if err := db.replayKeyedWAL(); err != nil {
		_ = file.Close()
		return err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	return nil
}

func ensureKeyedFormat(path, expected string) error {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	formatPath := filepath.Join(path, "FORMAT")
	data, err := os.ReadFile(formatPath)
	if err == nil {
		if string(data) != expected {
			return &Error{Kind: ErrorIncompatible, Op: "open_keyed", err: errors.New("directory contains a different OctetDB model")}
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	if len(entries) == 1 && entries[0].Name() == "FORMAT.tmp" {
		if err := os.Remove(filepath.Join(path, entries[0].Name())); err != nil {
			return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
		}
		entries = nil
	}
	if len(entries) != 0 {
		return &Error{Kind: ErrorIncompatible, Op: "open_keyed", err: errors.New("non-empty directory has no keyed OctetDB FORMAT marker")}
	}
	tmp := formatPath + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	written, writeErr := file.WriteString(expected)
	if writeErr == nil && written != len(expected) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmp, formatPath)
	}
	if writeErr == nil {
		writeErr = syncDirectory(path)
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: writeErr}
	}
	return nil
}

func (db *KeyedDB) appendKeyed(record keyedWALRecord) error {
	if err := db.appendKeyedFrame(record); err != nil {
		return err
	}
	return db.syncKeyed()
}

func (db *KeyedDB) appendKeyedFrame(record keyedWALRecord) error {
	if db.beforeAppend != nil {
		if err := db.beforeAppend(record); err != nil {
			return &Error{Kind: ErrorStorage, Op: "submit_keyed", err: err}
		}
	}
	payload, err := json.Marshal(record)
	if err != nil {
		return &Error{Kind: ErrorInvalidInput, Op: "submit_keyed", err: err}
	}
	if len(payload) > maxKeyedFrameBytes {
		return &Error{Kind: ErrorCapacity, Op: "submit_keyed", err: errors.New("encoded command frame is too large")}
	}
	frame := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	binary.LittleEndian.PutUint32(frame[4+len(payload):], crc32.ChecksumIEEE(payload))
	if written, err := db.wal.file.Write(frame); err != nil {
		return &Error{Kind: ErrorStorage, Op: "submit_keyed", err: err}
	} else if written != len(frame) {
		return &Error{Kind: ErrorStorage, Op: "submit_keyed", err: io.ErrShortWrite}
	}
	return nil
}

func (db *KeyedDB) syncKeyed() error {
	if db.beforeSync != nil {
		if err := db.beforeSync(); err != nil {
			return &Error{Kind: ErrorStorage, Op: "submit_keyed", err: err}
		}
	}
	if err := db.wal.file.Sync(); err != nil {
		return &Error{Kind: ErrorStorage, Op: "submit_keyed", err: err}
	}
	return nil
}

func (db *KeyedDB) replayKeyedWAL() error {
	if _, err := db.wal.file.Seek(0, io.SeekStart); err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	reader := bufio.NewReader(db.wal.file)
	offset := int64(0)
	lastSequence := uint64(0)
	for {
		header := make([]byte, 4)
		_, err := io.ReadFull(reader, header)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return db.truncateKeyedTail(offset)
		}
		if err != nil {
			return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
		}
		length := int(binary.LittleEndian.Uint32(header))
		if length <= 0 || length > maxKeyedFrameBytes {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("invalid WAL frame length")}
		}
		body := make([]byte, length+4)
		_, err = io.ReadFull(reader, body)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return db.truncateKeyedTail(offset)
		}
		if err != nil {
			return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
		}
		payload := body[:length]
		if binary.LittleEndian.Uint32(body[length:]) != crc32.ChecksumIEEE(payload) {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("WAL checksum mismatch")}
		}
		var record keyedWALRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: fmt.Errorf("decode WAL: %w", err)}
		}
		if record.Sequence == 0 || (lastSequence != 0 && record.Sequence != lastSequence+1) {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("WAL sequence is not contiguous")}
		}
		if err := db.validateKeyedRecord(record); err != nil {
			return err
		}
		lastSequence = record.Sequence
		if record.Sequence > db.sequence {
			if record.Sequence != db.sequence+1 {
				return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("WAL does not continue snapshot sequence")}
			}
			if _, duplicate := db.dedupe[record.CommandID]; duplicate {
				return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("WAL repeats a retained command ID")}
			}
			if err := db.validateKeyedApply(record); err != nil {
				return err
			}
			db.applyKeyed(record)
		}
		offset += int64(4 + length + 4)
	}
	return nil
}

func (db *KeyedDB) validateKeyedRecord(record keyedWALRecord) error {
	corrupt := func(message string) error {
		return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New(message)}
	}
	capacity := func(message string) error {
		return &Error{Kind: ErrorCapacity, Op: "open_keyed", err: errors.New(message)}
	}
	if record.CommandID == "" {
		return corrupt("WAL command ID is empty")
	}
	if len(record.CommandID) > keyedMaxIdentityBytes {
		return corrupt("WAL command ID exceeds fixed bound")
	}
	if len(record.Code) > keyedMaxCodeBytes {
		return corrupt("WAL rejection code exceeds fixed bound")
	}
	if len(record.Result) > db.options.MaxValueBytes {
		return capacity("WAL result exceeds configured value bound")
	}
	if len(record.Result) > 0 && !json.Valid(record.Result) {
		return corrupt("WAL result is not valid JSON")
	}
	if !record.Applied && len(record.Mutations) != 0 {
		return corrupt("rejected WAL decision contains mutations")
	}
	total := 0
	seen := make(map[string]struct{}, len(record.Mutations))
	for _, mutation := range record.Mutations {
		if mutation.Key == "" {
			return corrupt("WAL mutation key is empty")
		}
		if len(mutation.Key) > db.maxStoredKeyBytes() {
			return corrupt("WAL key exceeds fixed bound")
		}
		if _, ok := seen[mutation.Key]; ok {
			return corrupt("WAL contains duplicate mutation key")
		}
		seen[mutation.Key] = struct{}{}
		if mutation.Delete && len(mutation.Value) != 0 {
			return corrupt("WAL delete contains a value")
		}
		if len(mutation.Value) > db.options.MaxValueBytes {
			return capacity("WAL value exceeds configured bound")
		}
		if !mutation.Delete && !json.Valid(mutation.Value) {
			return corrupt("WAL value is not valid JSON")
		}
		total += len(mutation.Key) + len(mutation.Value)
	}
	if total > db.options.MaxTransactionBytes {
		return capacity("WAL transaction exceeds configured bound")
	}
	return nil
}

func (db *KeyedDB) validateKeyedApply(record keyedWALRecord) error {
	if !record.Applied {
		return nil
	}
	count := len(db.records)
	for _, mutation := range record.Mutations {
		_, exists := db.records[mutation.Key]
		if mutation.Delete && exists {
			count--
		}
		if !mutation.Delete && !exists {
			count++
		}
	}
	if count > db.options.MaxRecords {
		return &Error{Kind: ErrorCapacity, Op: "open_keyed", err: errors.New("WAL exceeds configured record capacity")}
	}
	return nil
}

func (db *KeyedDB) truncateKeyedTail(offset int64) error {
	if err := db.wal.file.Truncate(offset); err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	if err := db.wal.file.Sync(); err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	return nil
}

func (db *KeyedDB) applyKeyed(record keyedWALRecord) {
	if record.Applied {
		for _, mutation := range record.Mutations {
			if mutation.Delete {
				delete(db.records, mutation.Key)
			} else {
				db.records[mutation.Key] = append([]byte(nil), mutation.Value...)
			}
		}
	}
	db.sequence = record.Sequence
	retained := record
	retained.Mutations = nil
	db.dedupe[record.CommandID] = retained
	db.dedupeIDs = append(db.dedupeIDs, record.CommandID)
	if len(db.dedupeIDs) > db.options.DedupeHorizon {
		expired := db.dedupeIDs[0]
		db.dedupeIDs = db.dedupeIDs[1:]
		delete(db.dedupe, expired)
	}
	if db.afterApply != nil {
		db.afterApply(record)
	}
}

func (db *KeyedDB) snapshotKeyed() error {
	if db == nil || db.wal.file == nil {
		return nil
	}
	dedupe := make([]keyedWALRecord, 0, len(db.dedupeIDs))
	for _, id := range db.dedupeIDs {
		dedupe = append(dedupe, db.dedupe[id])
	}
	payload, err := json.Marshal(keyedSnapshot{Sequence: db.sequence, Records: db.records, Dedupe: dedupe})
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "snapshot_keyed", err: err}
	}
	hash := sha256.Sum256(payload)
	envelope, err := json.Marshal(keyedSnapshotEnvelope{Magic: keyedSnapshotMagic, Version: 1, Payload: payload, SHA256: hex.EncodeToString(hash[:])})
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "snapshot_keyed", err: err}
	}
	snapshotPath := filepath.Join(db.path, "snapshot")
	tmp := snapshotPath + ".tmp"
	_ = os.Remove(tmp)
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "snapshot_keyed", err: err}
	}
	written, writeErr := file.Write(envelope)
	if writeErr == nil && written != len(envelope) {
		writeErr = io.ErrShortWrite
	}
	if writeErr == nil {
		writeErr = file.Sync()
	}
	if closeErr := file.Close(); writeErr == nil {
		writeErr = closeErr
	}
	if writeErr == nil {
		writeErr = os.Rename(tmp, snapshotPath)
	}
	if writeErr == nil {
		writeErr = syncDirectory(db.path)
	}
	if writeErr == nil {
		writeErr = db.wal.file.Truncate(0)
	}
	if writeErr == nil {
		_, writeErr = db.wal.file.Seek(0, io.SeekStart)
	}
	if writeErr == nil {
		writeErr = db.wal.file.Sync()
	}
	if writeErr != nil {
		_ = os.Remove(tmp)
		return &Error{Kind: ErrorStorage, Op: "snapshot_keyed", err: writeErr}
	}
	return nil
}

func (db *KeyedDB) loadKeyedSnapshot() error {
	data, err := os.ReadFile(filepath.Join(db.path, "snapshot"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "open_keyed", err: err}
	}
	var envelope keyedSnapshotEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: fmt.Errorf("decode snapshot envelope: %w", err)}
	}
	if envelope.Magic != keyedSnapshotMagic || envelope.Version != 1 {
		return &Error{Kind: ErrorIncompatible, Op: "open_keyed", err: errors.New("unsupported keyed snapshot")}
	}
	hash := sha256.Sum256(envelope.Payload)
	if !bytes.Equal([]byte(envelope.SHA256), []byte(hex.EncodeToString(hash[:]))) {
		return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("snapshot checksum mismatch")}
	}
	var snapshot keyedSnapshot
	if err := json.Unmarshal(envelope.Payload, &snapshot); err != nil {
		return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: fmt.Errorf("decode snapshot: %w", err)}
	}
	if len(snapshot.Records) > db.options.MaxRecords || len(snapshot.Dedupe) > db.options.DedupeHorizon {
		return &Error{Kind: ErrorCapacity, Op: "open_keyed", err: errors.New("snapshot exceeds configured bounds")}
	}
	for key, value := range snapshot.Records {
		if len(value) > db.options.MaxValueBytes {
			return &Error{Kind: ErrorCapacity, Op: "open_keyed", err: errors.New("snapshot record exceeds configured value bound")}
		}
		if key == "" || len(key) > db.maxStoredKeyBytes() || !json.Valid(value) {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("snapshot record violates configured bounds")}
		}
	}
	db.records = snapshot.Records
	if db.records == nil {
		db.records = make(map[string][]byte)
	}
	db.sequence = snapshot.Sequence
	lastDedupeSequence := uint64(0)
	for _, record := range snapshot.Dedupe {
		if record.CommandID == "" || record.Sequence == 0 || record.Sequence > snapshot.Sequence {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("invalid snapshot dedupe entry")}
		}
		if err := db.validateKeyedRecord(record); err != nil {
			return err
		}
		if record.Sequence <= lastDedupeSequence {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("snapshot dedupe order is invalid")}
		}
		if _, duplicate := db.dedupe[record.CommandID]; duplicate {
			return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("snapshot repeats a command ID")}
		}
		lastDedupeSequence = record.Sequence
		db.dedupe[record.CommandID] = record
		db.dedupeIDs = append(db.dedupeIDs, record.CommandID)
	}
	if snapshot.Sequence != 0 && (len(snapshot.Dedupe) == 0 || lastDedupeSequence != snapshot.Sequence) {
		return &Error{Kind: ErrorCorruption, Op: "open_keyed", err: errors.New("snapshot dedupe frontier does not match sequence")}
	}
	return nil
}

func (db *KeyedDB) maxStoredKeyBytes() int {
	if db.format == catalogKeyedFormatContents {
		return keyedMaxKeyBytes + 9
	}
	return keyedMaxKeyBytes
}

func (wal *keyedWAL) close() error {
	if wal == nil || wal.file == nil {
		return nil
	}
	err := wal.file.Close()
	wal.file = nil
	if err != nil {
		return &Error{Kind: ErrorStorage, Op: "close_keyed", err: err}
	}
	return nil
}

func syncDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	if closeErr := directory.Close(); syncErr == nil {
		syncErr = closeErr
	}
	return syncErr
}
