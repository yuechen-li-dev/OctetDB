package m7generated

// This file is the retained generated-package facade for logical FLOW state
// deltas. It deliberately lives beside the generated checkpoint codec: hosts
// see typed opaque bytes and never inspect compiler-private flow fields.

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
)

const (
	accountAgentDeltaVersion = 1
	accountAgentDeltaMagic   = "OFD1"
	accountAgentFingerprint  = "36d8d2187661e140f142ab7b43473b3bf7e0bb37f885fb5b7343ffa0f0b987f8"
	accountAgentBoardSchema  = "TransitionCount:Int;Pending:Bool;WasPending:Bool;PendingTarget:Int;PendingAmount:Int;"
)

const (
	deltaState uint16 = 1 << iota
	deltaInstruction
	deltaTransitionCount
	deltaPending
	deltaWasPending
	deltaPendingTarget
	deltaPendingAmount
	deltaLastYield
	deltaHasYield
	deltaKnown = deltaState | deltaInstruction | deltaTransitionCount | deltaPending | deltaWasPending | deltaPendingTarget | deltaPendingAmount | deltaLastYield | deltaHasYield
)

// AccountAgentDelta is the exact semantic change between two stable yielded
// AccountAgent frontiers. Its bytes are deterministic and versioned.
type AccountAgentDelta struct{ data []byte }

func (d AccountAgentDelta) Bytes() []byte { return append([]byte(nil), d.data...) }

func ParseAccountAgentDelta(data []byte) (AccountAgentDelta, error) {
	if len(data) < 40 || string(data[:4]) != accountAgentDeltaMagic {
		return AccountAgentDelta{}, fmt.Errorf("flow delta: invalid header")
	}
	if data[4] != accountAgentDeltaVersion {
		return AccountAgentDelta{}, fmt.Errorf("flow delta: unsupported version %d", data[4])
	}
	if data[5] != 1 { // feature layout/schema version
		return AccountAgentDelta{}, fmt.Errorf("flow delta: unknown feature layout %d", data[5])
	}
	flags := binary.BigEndian.Uint16(data[38:40])
	if flags & ^deltaKnown != 0 {
		return AccountAgentDelta{}, fmt.Errorf("flow delta: unknown field mask %#x", flags & ^deltaKnown)
	}
	return AccountAgentDelta{data: append([]byte(nil), data...)}, nil
}

// ExportAccountAgentDelta compares logical checkpoint fields. Dirty flags are
// serialization aids only; they are not installed into the restored machine.
func ExportAccountAgentDelta(previous *AccountAgentCheckpoint, current AccountAgentCheckpoint) (AccountAgentDelta, error) {
	cur, err := checkedAccountAgentPayload(current)
	if err != nil {
		return AccountAgentDelta{}, err
	}
	prev := initialAccountAgentPayload(cur.Parameterowner)
	var from [32]byte
	if previous != nil {
		prev, err = checkedAccountAgentPayload(*previous)
		if err != nil {
			return AccountAgentDelta{}, err
		}
		if prev.Parameterowner != cur.Parameterowner {
			return AccountAgentDelta{}, fmt.Errorf("flow delta: construction parameter changed")
		}
		from = sha256.Sum256(previous.data)
	}
	return exportAccountAgentPayloadDelta(prev, cur, from)
}

// DurableAccountAgent is the generated-package durable facade. The underlying
// generated machine remains untouched and regeneration-safe.
type DurableAccountAgent struct {
	machine       *AccountAgent
	committed     __octAccountAgentCheckpointPayload
	committedHash [32]byte
}

func NewDurableAccountAgent(owner int) *DurableAccountAgent {
	return &DurableAccountAgent{machine: NewAccountAgent(owner), committed: initialAccountAgentPayload(owner)}
}

func RestoreDurableAccountAgent(checkpoint AccountAgentCheckpoint) (*DurableAccountAgent, error) {
	machine, err := RestoreAccountAgent(checkpoint)
	if err != nil {
		return nil, err
	}
	payload, err := checkedAccountAgentPayload(checkpoint)
	if err != nil {
		return nil, err
	}
	return &DurableAccountAgent{machine: machine, committed: payload, committedHash: accountAgentCheckpointIdentity(checkpoint)}, nil
}

func (m *DurableAccountAgent) Step(input Main_CommandContext) (AccountAgentTurn, error) {
	return m.machine.Step(input)
}
func (m *DurableAccountAgent) Checkpoint() (AccountAgentCheckpoint, error) {
	return m.machine.Checkpoint()
}

// ExportDelta uses the facade's committed frontier, avoiding checkpoint
// parsing and object reconstruction on the hot Step path.
func (m *DurableAccountAgent) ExportDelta() (AccountAgentDelta, error) {
	if m == nil || m.machine == nil || m.machine.flow == nil || m.machine.flow.completed || !m.machine.flow.hasYield {
		return AccountAgentDelta{}, __octAccountAgentCheckpointError(AccountAgentNotAtYield, "delta export requires a yielded machine")
	}
	return exportAccountAgentPayloadDelta(m.committed, m.currentAccountAgentPayload(), m.committedHash)
}

// AcceptCommitted advances only the in-memory dirty frontier. Hosts call it
// after the WAL sync succeeds; failure rollback restores the older checkpoint.
func (m *DurableAccountAgent) AcceptCommitted(checkpoint AccountAgentCheckpoint) error {
	if m == nil || m.machine == nil || m.machine.flow == nil || m.machine.flow.completed || !m.machine.flow.hasYield {
		return __octAccountAgentCheckpointError(AccountAgentNotAtYield, "commit acceptance requires a yielded machine")
	}
	m.committed = m.currentAccountAgentPayload()
	m.committedHash = accountAgentCheckpointIdentity(checkpoint)
	return nil
}

func (m *DurableAccountAgent) currentAccountAgentPayload() __octAccountAgentCheckpointPayload {
	f := m.machine.flow
	p := initialAccountAgentPayload(f.owner)
	p.CurrentState = f.__octActive()
	p.Instruction = f.instruction
	p.Board = __octAccountAgentCheckpointBoard{TransitionCount: f.board.TransitionCount, Pending: f.board.Pending, WasPending: f.board.WasPending, PendingTarget: f.board.PendingTarget, PendingAmount: f.board.PendingAmount}
	p.LastYield, p.HasYield = f.lastYield, f.hasYield
	return p
}

func accountAgentCheckpointIdentity(checkpoint AccountAgentCheckpoint) [32]byte {
	return sha256.Sum256(checkpoint.data)
}

func exportAccountAgentPayloadDelta(prev, cur __octAccountAgentCheckpointPayload, from [32]byte) (AccountAgentDelta, error) {
	if prev.Parameterowner != cur.Parameterowner {
		return AccountAgentDelta{}, fmt.Errorf("flow delta: construction parameter changed")
	}

	var flags uint16
	if prev.CurrentState != cur.CurrentState {
		flags |= deltaState
	}
	if prev.Instruction != cur.Instruction {
		flags |= deltaInstruction
	}
	if prev.Board.TransitionCount != cur.Board.TransitionCount {
		flags |= deltaTransitionCount
	}
	if prev.Board.Pending != cur.Board.Pending {
		flags |= deltaPending
	}
	if prev.Board.WasPending != cur.Board.WasPending {
		flags |= deltaWasPending
	}
	if prev.Board.PendingTarget != cur.Board.PendingTarget {
		flags |= deltaPendingTarget
	}
	if prev.Board.PendingAmount != cur.Board.PendingAmount {
		flags |= deltaPendingAmount
	}
	if prev.LastYield != cur.LastYield {
		flags |= deltaLastYield
	}
	if prev.HasYield != cur.HasYield {
		flags |= deltaHasYield
	}

	b := bytes.NewBuffer(make([]byte, 0, 128))
	b.WriteString(accountAgentDeltaMagic)
	b.WriteByte(accountAgentDeltaVersion)
	b.WriteByte(1)
	b.Write(from[:])
	_ = binary.Write(b, binary.BigEndian, flags)
	if flags&deltaState != 0 {
		if cur.CurrentState != "Active" {
			return AccountAgentDelta{}, fmt.Errorf("flow delta: unknown state %q", cur.CurrentState)
		}
		b.WriteByte(0)
	}
	if flags&deltaInstruction != 0 {
		putUvarint(b, uint64(cur.Instruction))
	}
	if flags&deltaTransitionCount != 0 {
		putVarint(b, int64(cur.Board.TransitionCount))
	}
	if flags&deltaPending != 0 {
		putBool(b, cur.Board.Pending)
	}
	if flags&deltaWasPending != 0 {
		putBool(b, cur.Board.WasPending)
	}
	if flags&deltaPendingTarget != 0 {
		putVarint(b, int64(cur.Board.PendingTarget))
	}
	if flags&deltaPendingAmount != 0 {
		putVarint(b, int64(cur.Board.PendingAmount))
	}
	if flags&deltaLastYield != 0 {
		encodeDecision(b, cur.LastYield)
	}
	if flags&deltaHasYield != 0 {
		putBool(b, cur.HasYield)
	}
	return AccountAgentDelta{data: b.Bytes()}, nil
}

// ApplyAccountAgentDelta reconstructs an exact full logical checkpoint without
// executing historical behavior. owner is used only for the initial frontier.
func ApplyAccountAgentDelta(previous *AccountAgentCheckpoint, owner int, delta AccountAgentDelta) (AccountAgentCheckpoint, error) {
	parsed, err := ParseAccountAgentDelta(delta.data)
	if err != nil {
		return AccountAgentCheckpoint{}, err
	}
	r := bytes.NewReader(parsed.data[40:])
	flags := binary.BigEndian.Uint16(parsed.data[38:40])
	payload := initialAccountAgentPayload(owner)
	var want [32]byte
	copy(want[:], parsed.data[6:38])
	if previous != nil {
		payload, err = checkedAccountAgentPayload(*previous)
		if err != nil {
			return AccountAgentCheckpoint{}, err
		}
		got := sha256.Sum256(previous.data)
		if got != want {
			return AccountAgentCheckpoint{}, fmt.Errorf("flow delta: prior checkpoint identity mismatch")
		}
	} else if want != ([32]byte{}) {
		return AccountAgentCheckpoint{}, fmt.Errorf("flow delta: missing prior checkpoint")
	}
	if payload.Parameterowner != owner {
		return AccountAgentCheckpoint{}, fmt.Errorf("flow delta: construction parameter mismatch")
	}
	if flags&deltaState != 0 {
		state, err := r.ReadByte()
		if err != nil {
			return AccountAgentCheckpoint{}, truncatedDelta(err)
		}
		if state != 0 {
			return AccountAgentCheckpoint{}, fmt.Errorf("flow delta: unknown state ID %d", state)
		}
		payload.CurrentState = "Active"
	}
	if flags&deltaInstruction != 0 {
		v, err := binary.ReadUvarint(r)
		if err != nil {
			return AccountAgentCheckpoint{}, truncatedDelta(err)
		}
		payload.Instruction = int(v)
	}
	if flags&deltaTransitionCount != 0 {
		v, err := binary.ReadVarint(r)
		if err != nil {
			return AccountAgentCheckpoint{}, truncatedDelta(err)
		}
		payload.Board.TransitionCount = int(v)
	}
	if flags&deltaPending != 0 {
		payload.Board.Pending, err = readBool(r)
		if err != nil {
			return AccountAgentCheckpoint{}, err
		}
	}
	if flags&deltaWasPending != 0 {
		payload.Board.WasPending, err = readBool(r)
		if err != nil {
			return AccountAgentCheckpoint{}, err
		}
	}
	if flags&deltaPendingTarget != 0 {
		v, e := binary.ReadVarint(r)
		if e != nil {
			return AccountAgentCheckpoint{}, truncatedDelta(e)
		}
		payload.Board.PendingTarget = int(v)
	}
	if flags&deltaPendingAmount != 0 {
		v, e := binary.ReadVarint(r)
		if e != nil {
			return AccountAgentCheckpoint{}, truncatedDelta(e)
		}
		payload.Board.PendingAmount = int(v)
	}
	if flags&deltaLastYield != 0 {
		payload.LastYield, err = decodeDecision(r)
		if err != nil {
			return AccountAgentCheckpoint{}, err
		}
	}
	if flags&deltaHasYield != 0 {
		payload.HasYield, err = readBool(r)
		if err != nil {
			return AccountAgentCheckpoint{}, err
		}
	}
	if r.Len() != 0 {
		return AccountAgentCheckpoint{}, fmt.Errorf("flow delta: trailing bytes")
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return AccountAgentCheckpoint{}, err
	}
	cp := AccountAgentCheckpoint{data: data}
	if _, err := RestoreAccountAgent(cp); err != nil {
		return AccountAgentCheckpoint{}, err
	}
	return cp, nil
}

func initialAccountAgentPayload(owner int) __octAccountAgentCheckpointPayload {
	return __octAccountAgentCheckpointPayload{Version: 1, Package: "Main", Flow: "AccountAgent", Fingerprint: accountAgentFingerprint, BoardSchema: accountAgentBoardSchema, ConstructionSchema: "owner:Int;", UtilitySchema: "", YieldSchema: "Main.TransitionDecision", Parameterowner: owner}
}

func checkedAccountAgentPayload(cp AccountAgentCheckpoint) (__octAccountAgentCheckpointPayload, error) {
	var p __octAccountAgentCheckpointPayload
	if err := json.Unmarshal(cp.data, &p); err != nil {
		return p, err
	}
	if _, err := RestoreAccountAgent(cp); err != nil {
		return p, err
	}
	if err := validateDecisionShape(p.LastYield); err != nil {
		return p, err
	}
	return p, nil
}

func encodeDecision(b *bytes.Buffer, d Main_TransitionDecision) {
	putBool(b, d.Accepted)
	putVarint(b, int64(d.Reason.Tag))
	putVarint(b, int64(d.Effect.Tag))
	putVarint(b, int64(d.AccountA))
	putVarint(b, int64(d.AccountB))
	putVarint(b, int64(d.Amount))
	putVarint(b, int64(d.NewBalanceA))
	putVarint(b, int64(d.NewBalanceB))
	putVarint(b, int64(d.NewStatus.Tag))
	putVarint(b, int64(d.ExpectedVersionA))
	putVarint(b, int64(d.ExpectedVersionB))
	putVarint(b, int64(d.TransitionCount))
}

func decodeDecision(r *bytes.Reader) (Main_TransitionDecision, error) {
	var d Main_TransitionDecision
	var err error
	d.Accepted, err = readBool(r)
	if err != nil {
		return d, err
	}
	values := make([]int, 11)
	for i := range values {
		v, e := binary.ReadVarint(r)
		if e != nil {
			return d, truncatedDelta(e)
		}
		values[i] = int(v)
	}
	d.Reason.Tag, d.Effect.Tag, d.AccountA, d.AccountB, d.Amount = values[0], values[1], values[2], values[3], values[4]
	d.NewBalanceA, d.NewBalanceB, d.NewStatus.Tag = values[5], values[6], values[7]
	d.ExpectedVersionA, d.ExpectedVersionB, d.TransitionCount = values[8], values[9], values[10]
	if err := validateDecisionShape(d); err != nil {
		return d, err
	}
	return d, nil
}

func validateDecisionShape(d Main_TransitionDecision) error {
	if d.Reason.Payload != nil || d.Effect.Payload != nil || d.NewStatus.Payload != nil {
		return fmt.Errorf("flow delta: unsupported nominal payload")
	}
	if d.Reason.Tag < 0 || d.Reason.Tag > DecisionReason_InvalidWorkflow_tag {
		return fmt.Errorf("flow delta: invalid reason tag %d", d.Reason.Tag)
	}
	if d.Effect.Tag < 0 || d.Effect.Tag > EffectKind_SetStatus_tag {
		return fmt.Errorf("flow delta: invalid effect tag %d", d.Effect.Tag)
	}
	if d.NewStatus.Tag < 0 || d.NewStatus.Tag > AccountStatus_Frozen_tag {
		return fmt.Errorf("flow delta: invalid status tag %d", d.NewStatus.Tag)
	}
	return nil
}

func putVarint(b *bytes.Buffer, v int64) {
	var x [binary.MaxVarintLen64]byte
	n := binary.PutVarint(x[:], v)
	b.Write(x[:n])
}
func putUvarint(b *bytes.Buffer, v uint64) {
	var x [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(x[:], v)
	b.Write(x[:n])
}
func putBool(b *bytes.Buffer, v bool) {
	if v {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
}
func readBool(r *bytes.Reader) (bool, error) {
	b, err := r.ReadByte()
	if err != nil {
		return false, truncatedDelta(err)
	}
	if b > 1 {
		return false, fmt.Errorf("flow delta: invalid bool %d", b)
	}
	return b == 1, nil
}
func truncatedDelta(err error) error { return fmt.Errorf("flow delta: truncated payload: %w", err) }
