package engine

import (
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
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	m1WALVersion      = 3
	m1SnapshotVersion = 2
	m1WALMagic        = "OCTWAL01"
	m1EndMagic        = "OCTEND01"
	m1SnapshotMagic   = "OCTSNAP1"
	m1SchemaID        = "octetdb-write/account-state/v1"
	m1ProgramID       = "account-agent/309da01b60ec0f7917d4fd5efd1707bd71d2d40f"
	m1GoProgramID     = "go-account-control/v1"
	segmentHeaderSize = 32
	segmentFooterSize = 24
)

type snapshotAgent struct {
	ID         AccountID `json:"id"`
	Checkpoint []byte    `json:"checkpoint"`
}

type snapshotResult struct {
	CommandID string `json:"command_id"`
	Result    Result `json:"result"`
}

type durableSnapshot struct {
	Version       int              `json:"version"`
	Sequence      uint64           `json:"sequence"`
	SchemaID      string           `json:"schema_id"`
	ProgramID     string           `json:"program_id"`
	Accounts      []Account        `json:"accounts"`
	Ledger        []LedgerEntry    `json:"ledger"`
	Dedupe        []snapshotResult `json:"dedupe,omitempty"`
	DedupeFormat  string           `json:"dedupe_format,omitempty"`
	DedupeCompact []byte           `json:"dedupe_compact,omitempty"`
	DedupeHorizon int              `json:"dedupe_horizon"`
	Agents        []snapshotAgent  `json:"agents"`
	Octagon       []byte           `json:"octagon"`
	OctHash       string           `json:"octagon_sha256"`
}

type segmentMeta struct {
	path, name      string
	id, first, last uint64
	records         int
	closed          bool
	size            int64
}

type m1Storage struct {
	dir            string
	mode           DurabilityMode
	segmentRecords int
	file           *os.File
	segment        segmentMeta
	nextSegmentID  uint64
	stats          StorageStats
	failpoint      func(FailurePoint) error
	programID      string
}

func openM1Storage(cfg Config, after uint64) (*m1Storage, []logRecord, RecoveryStats, error) {
	started := time.Now()
	if cfg.SegmentRecords <= 0 {
		cfg.SegmentRecords = 4096
	}
	if err := os.MkdirAll(filepath.Join(cfg.StorageDir, "wal"), 0o755); err != nil {
		return nil, nil, RecoveryStats{}, err
	}
	if err := os.MkdirAll(filepath.Join(cfg.StorageDir, "snapshots"), 0o755); err != nil {
		return nil, nil, RecoveryStats{}, err
	}
	if stale, _ := filepath.Glob(filepath.Join(cfg.StorageDir, "snapshots", "*.tmp")); len(stale) > 0 {
		for _, path := range stale {
			if err := os.Remove(path); err != nil {
				return nil, nil, RecoveryStats{}, err
			}
		}
	}
	s := &m1Storage{dir: cfg.StorageDir, mode: cfg.Durability, segmentRecords: cfg.SegmentRecords, nextSegmentID: 1, failpoint: cfg.FailureInjector, programID: expectedProgramID(cfg)}
	startScan := time.Now()
	records, metas, bytesScanned, truncated, err := scanSegments(filepath.Join(cfg.StorageDir, "wal"), after, s.programID)
	stats := RecoveryStats{WALScan: time.Since(startScan), WALBytesScanned: bytesScanned, RecordsReplayed: len(records)}
	if err != nil {
		return nil, nil, stats, err
	}
	if len(metas) > 0 {
		s.nextSegmentID = metas[len(metas)-1].id + 1
		last := metas[len(metas)-1]
		if !last.closed {
			if truncated >= 0 {
				if err := os.Truncate(last.path, truncated); err != nil {
					return nil, nil, stats, err
				}
				last.size = truncated
			}
			file, err := os.OpenFile(last.path, os.O_RDWR|os.O_APPEND, 0o600)
			if err != nil {
				return nil, nil, stats, err
			}
			s.file, s.segment = file, last
			s.nextSegmentID = last.id + 1
		}
	}
	stats.TotalReady = time.Since(started)
	return s, records, stats, nil
}

func segmentName(id uint64) string { return fmt.Sprintf("segment-%020d.wal", id) }

func encodeSegmentHeader(id, first uint64) []byte {
	b := make([]byte, segmentHeaderSize)
	copy(b[:8], m1WALMagic)
	binary.BigEndian.PutUint32(b[8:12], m1WALVersion)
	binary.BigEndian.PutUint64(b[12:20], id)
	binary.BigEndian.PutUint64(b[20:28], first)
	binary.BigEndian.PutUint32(b[28:32], crc32.ChecksumIEEE(b[:28]))
	return b
}

func encodeSegmentFooter(last uint64, records int) []byte {
	b := make([]byte, segmentFooterSize)
	copy(b[:8], m1EndMagic)
	binary.BigEndian.PutUint64(b[8:16], last)
	binary.BigEndian.PutUint32(b[16:20], uint32(records))
	binary.BigEndian.PutUint32(b[20:24], crc32.ChecksumIEEE(b[:20]))
	return b
}

func parseSegmentID(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "segment-") || !strings.HasSuffix(name, ".wal") {
		return 0, false
	}
	id, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "segment-"), ".wal"), 10, 64)
	return id, err == nil
}

func scanSegments(dir string, after uint64, programID string) ([]logRecord, []segmentMeta, int64, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, nil, 0, -1, err
	}
	var metas []segmentMeta
	for _, entry := range entries {
		if id, ok := parseSegmentID(entry.Name()); ok {
			info, err := entry.Info()
			if err != nil {
				return nil, nil, 0, -1, err
			}
			metas = append(metas, segmentMeta{id: id, name: entry.Name(), path: filepath.Join(dir, entry.Name()), size: info.Size()})
		}
	}
	sort.Slice(metas, func(i, j int) bool { return metas[i].id < metas[j].id })
	var out []logRecord
	var total int64
	truncated := int64(-1)
	var expectedSeq = after + 1
	for i := range metas {
		data, err := os.ReadFile(metas[i].path)
		if err != nil {
			return nil, metas, total, -1, err
		}
		total += int64(len(data))
		if len(data) < segmentHeaderSize {
			return nil, metas, total, -1, fmt.Errorf("invalid segment header %s", metas[i].name)
		}
		h := data[:segmentHeaderSize]
		if string(h[:8]) != m1WALMagic || binary.BigEndian.Uint32(h[8:12]) != m1WALVersion || binary.BigEndian.Uint32(h[28:32]) != crc32.ChecksumIEEE(h[:28]) {
			return nil, metas, total, -1, fmt.Errorf("invalid segment header %s", metas[i].name)
		}
		if binary.BigEndian.Uint64(h[12:20]) != metas[i].id {
			return nil, metas, total, -1, fmt.Errorf("segment identity mismatch %s", metas[i].name)
		}
		metas[i].first = binary.BigEndian.Uint64(h[20:28])
		bodyEnd := len(data)
		if len(data) >= segmentHeaderSize+segmentFooterSize && string(data[len(data)-segmentFooterSize:len(data)-segmentFooterSize+8]) == m1EndMagic {
			f := data[len(data)-segmentFooterSize:]
			if binary.BigEndian.Uint32(f[20:24]) != crc32.ChecksumIEEE(f[:20]) {
				return nil, metas, total, -1, fmt.Errorf("invalid segment footer %s", metas[i].name)
			}
			metas[i].closed, metas[i].last, metas[i].records = true, binary.BigEndian.Uint64(f[8:16]), int(binary.BigEndian.Uint32(f[16:20]))
			bodyEnd -= segmentFooterSize
		} else if i != len(metas)-1 {
			return nil, metas, total, -1, fmt.Errorf("closed segment %s has no valid footer", metas[i].name)
		}
		r := bytes.NewReader(data[segmentHeaderSize:bodyEnd])
		offset := int64(segmentHeaderSize)
		count := 0
		for r.Len() > 0 {
			var lenBuf [4]byte
			n, err := io.ReadFull(r, lenBuf[:])
			if errors.Is(err, io.ErrUnexpectedEOF) || (errors.Is(err, io.EOF) && n == 0) {
				if metas[i].closed {
					return nil, metas, total, -1, fmt.Errorf("truncated closed segment %s", metas[i].name)
				}
				truncated = offset
				break
			}
			if err != nil {
				return nil, metas, total, -1, err
			}
			length := binary.BigEndian.Uint32(lenBuf[:])
			if length == 0 || length > maxRecordBytes {
				return nil, metas, total, -1, fmt.Errorf("invalid record length in %s at %d", metas[i].name, offset)
			}
			body := make([]byte, int(length)+4)
			if _, err := io.ReadFull(r, body); err != nil {
				if !metas[i].closed && (errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF)) {
					truncated = offset
					break
				}
				return nil, metas, total, -1, fmt.Errorf("truncated closed segment %s", metas[i].name)
			}
			payload := body[:length]
			if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(body[length:]) {
				return nil, metas, total, -1, fmt.Errorf("checksum failure in %s at %d", metas[i].name, offset)
			}
			var record logRecord
			if err := json.Unmarshal(payload, &record); err != nil {
				return nil, metas, total, -1, fmt.Errorf("decode record in %s: %w", metas[i].name, err)
			}
			if record.Version != m1WALVersion {
				return nil, metas, total, -1, fmt.Errorf("unsupported WAL record version %d", record.Version)
			}
			if record.SchemaID != m1SchemaID || record.ProgramID != programID {
				return nil, metas, total, -1, fmt.Errorf("incompatible WAL identity at sequence %d", record.Sequence)
			}
			if record.Sequence < expectedSeq {
				if record.Sequence <= after {
					offset += int64(8 + length)
					count++
					continue
				}
				return nil, metas, total, -1, fmt.Errorf("out-of-order sequence %d", record.Sequence)
			}
			if record.Sequence != expectedSeq {
				return nil, metas, total, -1, fmt.Errorf("missing sequence before %d", record.Sequence)
			}
			expectedSeq++
			out = append(out, record)
			metas[i].last = record.Sequence
			offset += int64(8 + length)
			count++
		}
		if metas[i].closed && count != metas[i].records {
			return nil, metas, total, -1, fmt.Errorf("record count mismatch in %s", metas[i].name)
		}
		if !metas[i].closed {
			metas[i].records = count
		}
	}
	return out, metas, total, truncated, nil
}

func (s *m1Storage) ensureSegment(first uint64) error {
	if s.mode == MemoryOnly || s.file != nil {
		return nil
	}
	id := first
	name := segmentName(id)
	path := filepath.Join(s.dir, "wal", name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	header := encodeSegmentHeader(id, first)
	if _, err := file.Write(header); err != nil {
		file.Close()
		return err
	}
	s.stats.WALBytesWritten += uint64(len(header))
	s.file = file
	s.segment = segmentMeta{id: id, first: first, path: path, name: name, size: int64(len(header))}
	s.nextSegmentID = id + 1
	return nil
}

func (s *m1Storage) appendGroup(records []logRecord) error {
	if s.mode == MemoryOnly {
		s.stats.Committed += uint64(len(records))
		return nil
	}
	if err := s.inject(BeforeWALAppend); err != nil {
		return err
	}
	for _, record := range records {
		s.stats.FlowDeltaBytes += uint64(len(record.FlowDelta))
		if err := s.ensureSegment(record.Sequence); err != nil {
			return fmt.Errorf("ensure WAL segment: %w", err)
		}
		if s.segment.records >= s.segmentRecords {
			if err := s.inject(DuringSegmentRotation); err != nil {
				return err
			}
			if err := s.closeSegment(true); err != nil {
				return fmt.Errorf("rotate WAL segment: %w", err)
			}
			if err := s.ensureSegment(record.Sequence); err != nil {
				return fmt.Errorf("ensure rotated WAL segment: %w", err)
			}
		}
		framed, err := frame(record)
		if err != nil {
			return err
		}
		if err := s.inject(DuringWALAppend); err != nil {
			_, _ = s.file.Write(framed[:len(framed)/2])
			return err
		}
		if _, err := s.file.Write(framed); err != nil {
			return fmt.Errorf("append WAL frame: %w", err)
		}
		s.segment.records++
		s.segment.last = record.Sequence
		s.segment.size += int64(len(framed))
		s.stats.WALBytesWritten += uint64(len(framed))
	}
	if err := s.inject(AfterWALAppendBeforeSync); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("sync WAL group: %w", err)
	}
	s.stats.Syncs++
	s.stats.Committed += uint64(len(records))
	return nil
}

func (s *m1Storage) closeSegment(sync bool) error {
	if s.file == nil {
		return nil
	}
	footer := encodeSegmentFooter(s.segment.last, s.segment.records)
	if _, err := s.file.Write(footer); err != nil {
		return err
	}
	s.stats.WALBytesWritten += uint64(len(footer))
	if sync {
		if err := s.file.Sync(); err != nil {
			return err
		}
		s.stats.Syncs++
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil
	return nil
}

func (s *m1Storage) installSnapshot(snapshot durableSnapshot) (string, int64, error) {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return "", 0, err
	}
	digest := sha256.Sum256(payload)
	framed := make([]byte, 8+4+8+32+len(payload))
	copy(framed[:8], m1SnapshotMagic)
	binary.BigEndian.PutUint32(framed[8:12], m1SnapshotVersion)
	binary.BigEndian.PutUint64(framed[12:20], uint64(len(payload)))
	copy(framed[20:52], digest[:])
	copy(framed[52:], payload)
	base := fmt.Sprintf("snapshot-%020d.snap", snapshot.Sequence)
	tmp := filepath.Join(s.dir, "snapshots", base+".tmp")
	final := filepath.Join(s.dir, "snapshots", base)
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", 0, err
	}
	if injected := s.inject(DuringSnapshotWrite); injected != nil {
		_, _ = f.Write(framed[:len(framed)/2])
		_ = f.Close()
		return "", 0, injected
	}
	if _, err = f.Write(framed); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", 0, err
	}
	if err := s.inject(AfterSnapshotSyncBeforeInstall); err != nil {
		return "", 0, err
	}
	if err := os.Rename(tmp, final); err != nil {
		return "", 0, err
	}
	s.stats.SnapshotBytesWritten += uint64(len(framed))
	if err := s.inject(AfterSnapshotInstallBeforeCleanup); err != nil {
		return final, int64(len(framed)), err
	}
	return final, int64(len(framed)), nil
}

func (s *m1Storage) inject(point FailurePoint) error {
	if s.failpoint == nil {
		return nil
	}
	return s.failpoint(point)
}

func loadLatestSnapshot(dir, programID string) (*durableSnapshot, int64, time.Duration, error) {
	started := time.Now()
	entries, err := os.ReadDir(filepath.Join(dir, "snapshots"))
	if err != nil && !os.IsNotExist(err) {
		return nil, 0, 0, err
	}
	type candidate struct {
		seq  uint64
		path string
	}
	var candidates []candidate
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "snapshot-") || !strings.HasSuffix(name, ".snap") {
			continue
		}
		seq, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(name, "snapshot-"), ".snap"), 10, 64)
		if err == nil {
			candidates = append(candidates, candidate{seq, filepath.Join(dir, "snapshots", name)})
		}
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].seq > candidates[j].seq })
	if len(candidates) == 0 {
		return nil, 0, time.Since(started), nil
	}
	data, err := os.ReadFile(candidates[0].path)
	if err != nil {
		return nil, 0, 0, err
	}
	if len(data) < 52 || string(data[:8]) != m1SnapshotMagic || binary.BigEndian.Uint32(data[8:12]) != m1SnapshotVersion {
		return nil, int64(len(data)), 0, errors.New("invalid snapshot header")
	}
	length := binary.BigEndian.Uint64(data[12:20])
	if length != uint64(len(data)-52) {
		return nil, int64(len(data)), 0, errors.New("snapshot length mismatch")
	}
	digest := sha256.Sum256(data[52:])
	if !bytes.Equal(digest[:], data[20:52]) {
		return nil, int64(len(data)), 0, errors.New("snapshot hash mismatch")
	}
	var snapshot durableSnapshot
	if err := json.Unmarshal(data[52:], &snapshot); err != nil {
		return nil, int64(len(data)), 0, err
	}
	if snapshot.Version != m1SnapshotVersion || snapshot.Sequence != candidates[0].seq || snapshot.SchemaID != m1SchemaID || snapshot.ProgramID != programID {
		return nil, int64(len(data)), 0, errors.New("incompatible snapshot identity")
	}
	logical := sha256.Sum256(snapshot.Octagon)
	if hex.EncodeToString(logical[:]) != snapshot.OctHash {
		return nil, int64(len(data)), 0, errors.New("snapshot publication hash mismatch")
	}
	return &snapshot, int64(len(data)), time.Since(started), nil
}

func expectedProgramID(cfg Config) string {
	if cfg.GoBehavioralControl {
		return m1GoProgramID
	}
	return m1ProgramID
}

func (s *m1Storage) retireThrough(sequence uint64) error {
	entries, err := os.ReadDir(filepath.Join(s.dir, "wal"))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		id, ok := parseSegmentID(entry.Name())
		if !ok {
			continue
		}
		path := filepath.Join(s.dir, "wal", entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if len(data) < segmentFooterSize || string(data[len(data)-segmentFooterSize:len(data)-segmentFooterSize+8]) != m1EndMagic {
			continue
		}
		last := binary.BigEndian.Uint64(data[len(data)-segmentFooterSize+8:])
		if last <= sequence && (s.file == nil || id != s.segment.id) {
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *m1Storage) statsSnapshot() StorageStats { return s.stats }

func (s *m1Storage) close() error { return s.closeSegment(true) }
