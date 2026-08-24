package bsosim

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// Octagon is deliberately a narrow typed codec for the versioned M1 DTOs. The
// wire form is valid data-only Octagon, not JSON with another extension.
func EncodeEnvelope(e ProtocolEnvelopeV1) ([]byte, error) {
	if e.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version %d", e.ProtocolVersion)
	}
	if e.Schema != schemaFor(e.Kind) {
		return nil, fmt.Errorf("schema %q does not match %s", e.Schema, e.Kind)
	}
	return []byte(fmt.Sprintf("ProtocolEnvelopeV1 { ProtocolVersion: %d Schema: %s MessageID: %s TransferID: %s From: %s To: %s Kind: MessageKind.%s Amount: %d State: TransferState.%s Auth: %s }\n",
		e.ProtocolVersion, quote(e.Schema), quote(e.MessageID), quote(e.TransferID), quote(e.From), quote(e.To), enumName(string(e.Kind)), e.Amount, enumName(string(e.State)), quote(e.Auth))), nil
}

func DecodeEnvelope(data []byte) (ProtocolEnvelopeV1, error) {
	record, fields, err := parseRecord(string(data))
	if err != nil {
		return ProtocolEnvelopeV1{}, err
	}
	if record != "ProtocolEnvelopeV1" {
		return ProtocolEnvelopeV1{}, fmt.Errorf("wrong schema identity %q", record)
	}
	want := []string{"ProtocolVersion", "Schema", "MessageID", "TransferID", "From", "To", "Kind", "Amount", "State", "Auth"}
	if err := exactFields(fields, want); err != nil {
		return ProtocolEnvelopeV1{}, err
	}
	version, err := strconv.Atoi(fields["ProtocolVersion"])
	if err != nil || version != ProtocolVersion {
		return ProtocolEnvelopeV1{}, fmt.Errorf("unsupported protocol version %q", fields["ProtocolVersion"])
	}
	amount, err := strconv.ParseInt(fields["Amount"], 10, 64)
	if err != nil {
		return ProtocolEnvelopeV1{}, errors.New("invalid Amount")
	}
	e := ProtocolEnvelopeV1{ProtocolVersion: version, Schema: unquote(fields["Schema"]), MessageID: unquote(fields["MessageID"]), TransferID: unquote(fields["TransferID"]), From: unquote(fields["From"]), To: unquote(fields["To"]), Kind: MessageKind(enumValue(fields["Kind"])), Amount: Money(amount), State: TransferState(enumValue(fields["State"])), Auth: unquote(fields["Auth"])}
	if e.Schema != schemaFor(e.Kind) {
		return ProtocolEnvelopeV1{}, fmt.Errorf("schema %q does not match kind %q", e.Schema, e.Kind)
	}
	return e, nil
}

func EncodeCheckpoint(a TransactionAgent) ([]byte, error) {
	if a.ProtocolVersion != ProtocolVersion {
		return nil, fmt.Errorf("unsupported protocol version %d", a.ProtocolVersion)
	}
	return []byte(fmt.Sprintf("TransactionAgentCheckpointV1 { ProtocolVersion: %d TransferID: %s SenderBSO: %s ReceiverBSO: %s Amount: %d Phase: AgentPhase.%s RetryCount: %d NextLogicalDeadline: %d LastObservedSenderVersion: %d LastObservedReceiverVersion: %d PlacementGeneration: %d LastMessageKind: MessageKind.%s }\n", a.ProtocolVersion, quote(a.TransferID), quote(a.SenderBSO), quote(a.ReceiverBSO), a.Amount, enumName(string(a.Phase)), a.RetryCount, a.NextLogicalDeadline, a.LastObservedSenderVersion, a.LastObservedReceiverVersion, a.PlacementGeneration, enumName(string(a.LastMessageKind)))), nil
}

func DecodeCheckpoint(data []byte) (TransactionAgent, error) {
	record, f, err := parseRecord(string(data))
	if err != nil {
		return TransactionAgent{}, err
	}
	if record != "TransactionAgentCheckpointV1" {
		return TransactionAgent{}, fmt.Errorf("wrong schema identity %q", record)
	}
	want := []string{"ProtocolVersion", "TransferID", "SenderBSO", "ReceiverBSO", "Amount", "Phase", "RetryCount", "NextLogicalDeadline", "LastObservedSenderVersion", "LastObservedReceiverVersion", "PlacementGeneration", "LastMessageKind"}
	if err := exactFields(f, want); err != nil {
		return TransactionAgent{}, err
	}
	ints := make([]int64, 7)
	names := []string{"ProtocolVersion", "Amount", "RetryCount", "NextLogicalDeadline", "LastObservedSenderVersion", "LastObservedReceiverVersion", "PlacementGeneration"}
	for i, n := range names {
		ints[i], err = strconv.ParseInt(f[n], 10, 64)
		if err != nil {
			return TransactionAgent{}, fmt.Errorf("invalid %s", n)
		}
	}
	if ints[0] != ProtocolVersion {
		return TransactionAgent{}, fmt.Errorf("unsupported protocol version %d", ints[0])
	}
	return TransactionAgent{ProtocolVersion: int(ints[0]), TransferID: unquote(f["TransferID"]), SenderBSO: unquote(f["SenderBSO"]), ReceiverBSO: unquote(f["ReceiverBSO"]), Amount: Money(ints[1]), Phase: AgentPhase(enumValue(f["Phase"])), RetryCount: int(ints[2]), NextLogicalDeadline: int(ints[3]), LastObservedSenderVersion: int(ints[4]), LastObservedReceiverVersion: int(ints[5]), PlacementGeneration: int(ints[6]), LastMessageKind: MessageKind(enumValue(f["LastMessageKind"]))}, nil
}

func schemaFor(k MessageKind) string {
	switch k {
	case MessageOffer:
		return "TransferOfferV1"
	case MessageAccept:
		return "TransferAcceptV1"
	case MessageCommit:
		return "TransferCommitV1"
	case MessageAcknowledge:
		return "TransferAcknowledgeV1"
	case MessageReconcile:
		return "TransferReconcileV1"
	case MessageReject:
		return "TransferRejectV1"
	}
	return ""
}
func newEnvelope(id, from, to string, amount Money, kind MessageKind, state TransferState) ProtocolEnvelopeV1 {
	e := ProtocolEnvelopeV1{ProtocolVersion: 1, Schema: schemaFor(kind), MessageID: fmt.Sprintf("%s/%s/%s", id, kind, from), TransferID: id, From: from, To: to, Kind: kind, Amount: amount, State: state}
	e.Auth = envelopeAuth(e)
	return e
}
func envelopeAuth(e ProtocolEnvelopeV1) string {
	copy := e
	copy.Auth = ""
	b, _ := EncodeEnvelope(copy)
	sum := sha256.Sum256(append(b, []byte("identity-secret/"+e.From)...))
	return hex.EncodeToString(sum[:])
}
func validateEnvelope(e ProtocolEnvelopeV1) error {
	if e.ProtocolVersion != 1 || e.Schema != schemaFor(e.Kind) || e.From == "" || e.To == "" || e.From == e.To || e.TransferID == "" || e.Amount <= 0 {
		return errors.New("invalid envelope")
	}
	if envelopeAuth(e) != e.Auth {
		return errors.New("authentication failed")
	}
	return nil
}

func quote(s string) string   { return strconv.Quote(s) }
func unquote(s string) string { v, _ := strconv.Unquote(s); return v }
func enumName(s string) string {
	if s == "" {
		return "None"
	}
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '_' || r == '-' })
	for i := range parts {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}
func enumValue(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		s = s[i+1:]
	}
	if s == "None" {
		return ""
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && unicode.IsUpper(r) {
			b.WriteByte('_')
		}
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}

func exactFields(f map[string]string, want []string) error {
	if len(f) != len(want) {
		return errors.New("Octagon record field count mismatch")
	}
	for _, n := range want {
		if _, ok := f[n]; !ok {
			return fmt.Errorf("Octagon record missing field %s", n)
		}
	}
	return nil
}
func parseRecord(s string) (string, map[string]string, error) {
	s = strings.TrimSpace(s)
	open := strings.IndexByte(s, '{')
	if open < 1 || !strings.HasSuffix(s, "}") {
		return "", nil, errors.New("invalid Octagon record")
	}
	name := strings.TrimSpace(s[:open])
	body := strings.TrimSpace(s[open+1 : len(s)-1])
	f := map[string]string{}
	for len(body) > 0 {
		colon := strings.IndexByte(body, ':')
		if colon < 1 {
			return "", nil, errors.New("invalid Octagon field")
		}
		key := strings.TrimSpace(body[:colon])
		body = strings.TrimSpace(body[colon+1:])
		var value string
		if strings.HasPrefix(body, "\"") {
			i := 1
			esc := false
			for ; i < len(body); i++ {
				if body[i] == '"' && !esc {
					i++
					break
				}
				if body[i] == '\\' && !esc {
					esc = true
				} else {
					esc = false
				}
			}
			if i > len(body) {
				return "", nil, errors.New("unterminated string")
			}
			value = body[:i]
			body = strings.TrimSpace(body[i:])
		} else {
			i := strings.IndexAny(body, " \t\r\n")
			if i < 0 {
				value = body
				body = ""
			} else {
				value = body[:i]
				body = strings.TrimSpace(body[i:])
			}
		}
		if key == "" || value == "" {
			return "", nil, errors.New("empty Octagon field")
		}
		if _, ok := f[key]; ok {
			return "", nil, fmt.Errorf("duplicate field %s", key)
		}
		f[key] = value
	}
	return name, f, nil
}
