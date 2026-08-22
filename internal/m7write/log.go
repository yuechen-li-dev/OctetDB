package m7write

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

const maxRecordBytes = 1 << 20

type logRecord struct {
	Version      int       `json:"version"`
	SchemaID     string    `json:"schema_id,omitempty"`
	ProgramID    string    `json:"program_id,omitempty"`
	Sequence     uint64    `json:"sequence"`
	AgentID      AccountID `json:"agent_id"`
	CommandID    string    `json:"command_id"`
	CommandKind  int       `json:"command_kind"`
	AccountA     AccountID `json:"account_a"`
	AccountB     AccountID `json:"account_b,omitempty"`
	Amount       int       `json:"amount"`
	Result       Result    `json:"result"`
	EffectTag    int       `json:"effect_tag"`
	NewBalanceA  int       `json:"new_balance_a"`
	NewBalanceB  int       `json:"new_balance_b"`
	NewStatusTag int       `json:"new_status_tag"`
	ExpectedA    uint64    `json:"expected_a"`
	ExpectedB    uint64    `json:"expected_b"`
	Checkpoint   []byte    `json:"checkpoint"`
}

type commitLog struct {
	mu        sync.Mutex
	file      *os.File
	mode      DurabilityMode
	batchSize int
	pending   int
	failNext  error
}

func openCommitLog(path string, mode DurabilityMode, batchSize int) (*commitLog, []logRecord, bool, error) {
	if mode == MemoryOnly {
		return &commitLog{mode: mode}, nil, false, nil
	}
	if batchSize <= 0 {
		batchSize = 64
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, false, err
	}
	records, validBytes, truncated, err := scanLog(file)
	if err != nil {
		file.Close()
		return nil, nil, false, err
	}
	if truncated {
		if err := file.Truncate(validBytes); err != nil {
			file.Close()
			return nil, nil, false, err
		}
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, nil, false, err
	}
	return &commitLog{file: file, mode: mode, batchSize: batchSize}, records, truncated, nil
}

func scanLog(file *os.File) ([]logRecord, int64, bool, error) {
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, false, err
	}
	r := bufio.NewReader(file)
	var records []logRecord
	var offset int64
	for {
		var header [4]byte
		n, err := io.ReadFull(r, header[:])
		if errors.Is(err, io.EOF) && n == 0 {
			return records, offset, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return records, offset, true, nil
		}
		if err != nil {
			return nil, offset, false, err
		}
		length := binary.BigEndian.Uint32(header[:])
		if length == 0 || length > maxRecordBytes {
			return nil, offset, false, fmt.Errorf("invalid record length %d at %d", length, offset)
		}
		body := make([]byte, int(length)+4)
		n, err = io.ReadFull(r, body)
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return records, offset, true, nil
		}
		if err != nil {
			return nil, offset, false, err
		}
		payload := body[:length]
		wantCRC := binary.BigEndian.Uint32(body[length:])
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return nil, offset, false, fmt.Errorf("checksum failure at %d", offset)
		}
		var record logRecord
		if err := json.Unmarshal(payload, &record); err != nil {
			return nil, offset, false, fmt.Errorf("decode record at %d: %w", offset, err)
		}
		if record.Version != 1 {
			return nil, offset, false, fmt.Errorf("unsupported log record version %d", record.Version)
		}
		records = append(records, record)
		offset += int64(4 + len(body))
	}
}

func frame(record logRecord) ([]byte, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRecordBytes {
		return nil, fmt.Errorf("record is %d bytes", len(payload))
	}
	framed := make([]byte, 4+len(payload)+4)
	binary.BigEndian.PutUint32(framed[:4], uint32(len(payload)))
	copy(framed[4:], payload)
	binary.BigEndian.PutUint32(framed[4+len(payload):], crc32.ChecksumIEEE(payload))
	return framed, nil
}

func (l *commitLog) append(record logRecord) error {
	if l.mode == MemoryOnly {
		return nil
	}
	framed, err := frame(record)
	if err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.failNext != nil {
		err := l.failNext
		l.failNext = nil
		return err
	}
	if _, err := l.file.Write(framed); err != nil {
		return err
	}
	l.pending++
	if l.mode == SyncEach || (l.mode == BatchSync && l.pending >= l.batchSize) {
		if err := l.file.Sync(); err != nil {
			return err
		}
		l.pending = 0
	}
	return nil
}

func (l *commitLog) flush() error {
	if l.mode == MemoryOnly {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Sync(); err != nil {
		return err
	}
	l.pending = 0
	return nil
}

func (l *commitLog) close() error {
	if l.mode == MemoryOnly {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.pending > 0 {
		if err := l.file.Sync(); err != nil {
			return err
		}
	}
	return l.file.Close()
}

func (l *commitLog) injectFailure(err error) { l.mu.Lock(); l.failNext = err; l.mu.Unlock() }
