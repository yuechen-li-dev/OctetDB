package m7write

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const compactDedupeMagic = "OCD1"

func encodeCompactDedupe(entries []snapshotResult, horizon int) ([]byte, error) {
	if horizon < 0 || len(entries) > horizon {
		return nil, fmt.Errorf("dedupe count %d exceeds horizon %d", len(entries), horizon)
	}
	b := bytes.NewBuffer(make([]byte, 0, 16+len(entries)*40))
	b.WriteString(compactDedupeMagic)
	b.WriteByte(1)
	_ = binary.Write(b, binary.BigEndian, uint32(horizon))
	_ = binary.Write(b, binary.BigEndian, uint32(len(entries)))
	var prior uint64
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.CommandID == "" || entry.Result.CommandID != entry.CommandID {
			return nil, fmt.Errorf("dedupe command identity mismatch")
		}
		if _, ok := seen[entry.CommandID]; ok {
			return nil, fmt.Errorf("duplicate command ID %q in dedupe horizon", entry.CommandID)
		}
		seen[entry.CommandID] = struct{}{}
		if entry.Result.Sequence <= prior {
			return nil, fmt.Errorf("dedupe sequence %d is not ordered", entry.Result.Sequence)
		}
		prior = entry.Result.Sequence
		putBinaryUvarint(b, uint64(len(entry.CommandID)))
		b.WriteString(entry.CommandID)
		putBinaryUvarint(b, entry.Result.Sequence)
		if entry.Result.Accepted {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
		putBinaryVarint(b, int64(entry.Result.ReasonTag))
		putBinaryVarint(b, int64(entry.Result.EffectTag))
		putBinaryVarint(b, int64(entry.Result.TransitionCount))
	}
	return b.Bytes(), nil
}

func decodeCompactDedupe(data []byte, configuredHorizon int) ([]snapshotResult, int, error) {
	if len(data) < 13 || string(data[:4]) != compactDedupeMagic {
		return nil, 0, fmt.Errorf("invalid compact dedupe header")
	}
	if data[4] != 1 {
		return nil, 0, fmt.Errorf("unsupported compact dedupe version %d", data[4])
	}
	horizon := int(binary.BigEndian.Uint32(data[5:9]))
	count := int(binary.BigEndian.Uint32(data[9:13]))
	if horizon <= 0 || count > horizon {
		return nil, horizon, fmt.Errorf("dedupe count/horizon mismatch: %d/%d", count, horizon)
	}
	if configuredHorizon != horizon {
		return nil, horizon, fmt.Errorf("dedupe horizon mismatch: snapshot %d configured %d", horizon, configuredHorizon)
	}
	r := bytes.NewReader(data[13:])
	out := make([]snapshotResult, 0, count)
	seen := make(map[string]struct{}, count)
	var prior uint64
	for i := 0; i < count; i++ {
		length, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, horizon, fmt.Errorf("truncated dedupe command ID: %w", err)
		}
		if length == 0 || length > uint64(r.Len()) || length > 1<<20 {
			return nil, horizon, fmt.Errorf("invalid dedupe command ID length %d", length)
		}
		idBytes := make([]byte, int(length))
		if _, err := r.Read(idBytes); err != nil {
			return nil, horizon, err
		}
		id := string(idBytes)
		if _, ok := seen[id]; ok {
			return nil, horizon, fmt.Errorf("duplicate command ID %q in dedupe horizon", id)
		}
		seen[id] = struct{}{}
		seq, err := binary.ReadUvarint(r)
		if err != nil {
			return nil, horizon, fmt.Errorf("truncated dedupe sequence: %w", err)
		}
		if seq <= prior {
			return nil, horizon, fmt.Errorf("out-of-order dedupe sequence %d", seq)
		}
		prior = seq
		accepted, err := r.ReadByte()
		if err != nil {
			return nil, horizon, fmt.Errorf("truncated dedupe outcome: %w", err)
		}
		if accepted > 1 {
			return nil, horizon, fmt.Errorf("invalid dedupe accepted value %d", accepted)
		}
		reason, err := binary.ReadVarint(r)
		if err != nil {
			return nil, horizon, fmt.Errorf("truncated dedupe reason: %w", err)
		}
		effect, err := binary.ReadVarint(r)
		if err != nil {
			return nil, horizon, fmt.Errorf("truncated dedupe effect: %w", err)
		}
		turns, err := binary.ReadVarint(r)
		if err != nil {
			return nil, horizon, fmt.Errorf("truncated dedupe transition count: %w", err)
		}
		if reason < 0 || reason > 8 || effect < 0 || effect > 4 || turns <= 0 {
			return nil, horizon, fmt.Errorf("invalid dedupe result fields reason=%d effect=%d turns=%d", reason, effect, turns)
		}
		out = append(out, snapshotResult{CommandID: id, Result: Result{Sequence: seq, CommandID: id, Accepted: accepted == 1, ReasonTag: int(reason), EffectTag: int(effect), TransitionCount: int(turns)}})
	}
	if r.Len() != 0 {
		return nil, horizon, fmt.Errorf("compact dedupe trailing bytes")
	}
	return out, horizon, nil
}

func putBinaryVarint(b *bytes.Buffer, v int64) {
	var x [binary.MaxVarintLen64]byte
	n := binary.PutVarint(x[:], v)
	b.Write(x[:n])
}
func putBinaryUvarint(b *bytes.Buffer, v uint64) {
	var x [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(x[:], v)
	b.Write(x[:n])
}
