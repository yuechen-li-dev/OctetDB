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

// Octagon is deliberately a narrow typed codec for the versioned trust and
// settlement DTOs. The
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
	roles := make([]string, len(a.RequiredRoles))
	for i := range a.RequiredRoles {
		roles[i] = string(a.RequiredRoles[i])
	}
	thresholds := make([]string, len(a.TrustThresholds))
	for i := range a.TrustThresholds {
		thresholds[i] = strconv.Itoa(a.TrustThresholds[i])
	}
	return []byte(fmt.Sprintf("TransactionAgentCheckpointV1 { ProtocolVersion: %d TransferID: %s SenderBSO: %s ReceiverBSO: %s Amount: %d Phase: AgentPhase.%s RetryCount: %d NextLogicalDeadline: %d LastObservedSenderVersion: %d LastObservedReceiverVersion: %d PlacementGeneration: %d LastMessageKind: MessageKind.%s TransactionClass: TransactionClass.%s ApplicationReference: %s TrustResolutionID: %s TrustFailureReason: %s RequiredRoles: %s TrustThresholds: %s TrustCandidates: %s SelectedProviders: %s CollectedAttestationIDs: %s TrustProviderIndex: %d SenderPolicyVersion: %d ReceiverPolicyVersion: %d TrustProvidersConsulted: %d FreshProviderCalls: %d ReusedTrustAttestations: %d }\n", a.ProtocolVersion, quote(a.TransferID), quote(a.SenderBSO), quote(a.ReceiverBSO), a.Amount, enumName(string(a.Phase)), a.RetryCount, a.NextLogicalDeadline, a.LastObservedSenderVersion, a.LastObservedReceiverVersion, a.PlacementGeneration, enumName(string(a.LastMessageKind)), enumName(string(a.TransactionClass)), quote(a.ApplicationReference), quote(a.TrustResolutionID), quote(a.TrustFailureReason), quote(strings.Join(roles, ",")), quote(strings.Join(thresholds, ",")), quote(strings.Join(a.TrustCandidates, ",")), quote(strings.Join(a.SelectedProviders, ",")), quote(strings.Join(a.CollectedAttestationIDs, ",")), a.TrustProviderIndex, a.SenderPolicyVersion, a.ReceiverPolicyVersion, a.TrustProvidersConsulted, a.FreshProviderCalls, a.ReusedTrustAttestations)), nil
}

func DecodeCheckpoint(data []byte) (TransactionAgent, error) {
	record, f, err := parseRecord(string(data))
	if err != nil {
		return TransactionAgent{}, err
	}
	if record != "TransactionAgentCheckpointV1" {
		return TransactionAgent{}, fmt.Errorf("wrong schema identity %q", record)
	}
	want := []string{"ProtocolVersion", "TransferID", "SenderBSO", "ReceiverBSO", "Amount", "Phase", "RetryCount", "NextLogicalDeadline", "LastObservedSenderVersion", "LastObservedReceiverVersion", "PlacementGeneration", "LastMessageKind", "TransactionClass", "ApplicationReference", "TrustResolutionID", "TrustFailureReason", "RequiredRoles", "TrustThresholds", "TrustCandidates", "SelectedProviders", "CollectedAttestationIDs", "TrustProviderIndex", "SenderPolicyVersion", "ReceiverPolicyVersion", "TrustProvidersConsulted", "FreshProviderCalls", "ReusedTrustAttestations"}
	if err := exactFields(f, want); err != nil {
		return TransactionAgent{}, err
	}
	ints := make([]int64, 13)
	names := []string{"ProtocolVersion", "Amount", "RetryCount", "NextLogicalDeadline", "LastObservedSenderVersion", "LastObservedReceiverVersion", "PlacementGeneration", "TrustProviderIndex", "SenderPolicyVersion", "ReceiverPolicyVersion", "TrustProvidersConsulted", "FreshProviderCalls", "ReusedTrustAttestations"}
	for i, n := range names {
		ints[i], err = strconv.ParseInt(f[n], 10, 64)
		if err != nil {
			return TransactionAgent{}, fmt.Errorf("invalid %s", n)
		}
	}
	if ints[0] != ProtocolVersion {
		return TransactionAgent{}, fmt.Errorf("unsupported protocol version %d", ints[0])
	}
	var roles []TrustRole
	for _, role := range splitCSV(unquote(f["RequiredRoles"])) {
		roles = append(roles, TrustRole(role))
	}
	var thresholds []int
	for _, raw := range splitCSV(unquote(f["TrustThresholds"])) {
		value, e := strconv.Atoi(raw)
		if e != nil {
			return TransactionAgent{}, errors.New("invalid TrustThresholds")
		}
		thresholds = append(thresholds, value)
	}
	return TransactionAgent{ProtocolVersion: int(ints[0]), TransferID: unquote(f["TransferID"]), SenderBSO: unquote(f["SenderBSO"]), ReceiverBSO: unquote(f["ReceiverBSO"]), Amount: Money(ints[1]), Phase: AgentPhase(enumValue(f["Phase"])), RetryCount: int(ints[2]), NextLogicalDeadline: int(ints[3]), LastObservedSenderVersion: int(ints[4]), LastObservedReceiverVersion: int(ints[5]), PlacementGeneration: int(ints[6]), LastMessageKind: MessageKind(enumValue(f["LastMessageKind"])), TransactionClass: TransactionClass(enumValue(f["TransactionClass"])), ApplicationReference: unquote(f["ApplicationReference"]), TrustResolutionID: unquote(f["TrustResolutionID"]), TrustFailureReason: unquote(f["TrustFailureReason"]), RequiredRoles: roles, TrustThresholds: thresholds, TrustCandidates: splitCSV(unquote(f["TrustCandidates"])), SelectedProviders: splitCSV(unquote(f["SelectedProviders"])), CollectedAttestationIDs: splitCSV(unquote(f["CollectedAttestationIDs"])), TrustProviderIndex: int(ints[7]), SenderPolicyVersion: int(ints[8]), ReceiverPolicyVersion: int(ints[9]), TrustProvidersConsulted: int(ints[10]), FreshProviderCalls: int(ints[11]), ReusedTrustAttestations: int(ints[12])}, nil
}

func splitCSV(value string) []string {
	if value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func EncodeIdentityAttestation(a IdentityAttestationV1) []byte {
	return []byte(fmt.Sprintf("IdentityAttestationV1 { ProviderID: %s SubjectBSO: %s IdentityLevel: IdentityLevel.%s IssuedAt: %d ValidUntil: %d PolicyVersion: %s AttestationID: %s Auth: %s }\n", quote(a.ProviderID), quote(a.SubjectBSO), enumName(string(a.IdentityLevel)), a.IssuedAt, a.ValidUntil, quote(a.PolicyVersion), quote(a.AttestationID), quote(a.Auth)))
}

func EncodeRiskAttestation(a RiskAttestationV1) []byte {
	return []byte(fmt.Sprintf("RiskAttestationV1 { ProviderID: %s TransferID: %s Decision: RiskDecision.%s PolicyVersion: %s IssuedAt: %d ValidUntil: %d ReasonCode: %s AttestationID: %s Auth: %s }\n", quote(a.ProviderID), quote(a.TransferID), enumName(string(a.Decision)), quote(a.PolicyVersion), a.IssuedAt, a.ValidUntil, quote(a.ReasonCode), quote(a.AttestationID), quote(a.Auth)))
}

func EncodeAuthorizationAttestation(a AuthorizationAttestationV1) []byte {
	return []byte(fmt.Sprintf("AuthorizationAttestationV1 { ProviderID: %s SubjectBSO: %s TransactionClass: TransactionClass.%s MaxAmount: %d IssuedAt: %d ValidUntil: %d PolicyVersion: %s AttestationID: %s ApplicationReference: %s Auth: %s }\n", quote(a.ProviderID), quote(a.SubjectBSO), enumName(string(a.TransactionClass)), a.MaxAmount, a.IssuedAt, a.ValidUntil, quote(a.PolicyVersion), quote(a.AttestationID), quote(a.ApplicationReference), quote(a.Auth)))
}

func EncodeEscrowAttestation(a EscrowAttestationV1) []byte {
	return []byte(fmt.Sprintf("EscrowAttestationV1 { ProviderID: %s TransferID: %s HoldAccepted: %t ReleasePolicyID: %s PolicyVersion: %s IssuedAt: %d ValidUntil: %d AttestationID: %s Auth: %s }\n", quote(a.ProviderID), quote(a.TransferID), a.HoldAccepted, quote(a.ReleasePolicyID), quote(a.PolicyVersion), a.IssuedAt, a.ValidUntil, quote(a.AttestationID), quote(a.Auth)))
}

func EncodeDisputeAttestation(a DisputeAttestationV1) []byte {
	return []byte(fmt.Sprintf("DisputeAttestationV1 { ProviderID: %s OriginalTransferID: %s Decision: DisputeDecision.%s PolicyVersion: %s IssuedAt: %d ValidUntil: %d AttestationID: %s Auth: %s }\n", quote(a.ProviderID), quote(a.OriginalTransferID), enumName(string(a.Decision)), quote(a.PolicyVersion), a.IssuedAt, a.ValidUntil, quote(a.AttestationID), quote(a.Auth)))
}

func EncodeProviderCapabilities(c TrustProviderCapabilitiesV1) []byte {
	roles := make([]string, len(c.Roles))
	for i := range c.Roles {
		roles[i] = "TrustRole." + enumName(string(c.Roles[i]))
	}
	return []byte(fmt.Sprintf("TrustProviderCapabilitiesV1 { ProviderID: %s Roles: [%s] PolicyVersion: %d }\n", quote(c.ProviderID), strings.Join(roles, ", "), c.PolicyVersion))
}

func EncodeTrustRule(rule TrustRuleV1) []byte {
	providers := make([]string, len(rule.AcceptedProviders))
	for i := range rule.AcceptedProviders {
		providers[i] = quote(rule.AcceptedProviders[i])
	}
	return []byte(fmt.Sprintf("TrustRuleV1 { Role: TrustRole.%s AcceptedProviders: [%s] Threshold: %d MaxAmount: %d TransactionClass: TransactionClass.%s ValidUntil: %d }", enumName(string(rule.Role)), strings.Join(providers, ", "), rule.Threshold, rule.MaxAmount, enumName(string(rule.TransactionClass)), rule.ValidUntil))
}

func EncodeTrustPolicy(policy TrustPolicyV1) []byte {
	rules := make([]string, len(policy.Rules))
	for i := range policy.Rules {
		rules[i] = string(EncodeTrustRule(policy.Rules[i]))
	}
	revoked := make([]string, len(policy.RevokedProviders))
	for i := range policy.RevokedProviders {
		revoked[i] = quote(policy.RevokedProviders[i])
	}
	return []byte(fmt.Sprintf("TrustPolicyV1 { BSOID: %s Version: %d DirectLimit: %d ValidUntil: %d Rules: [%s] RevokedProviders: [%s] }\n", quote(policy.BSOID), policy.Version, policy.DirectLimit, policy.ValidUntil, strings.Join(rules, ", "), strings.Join(revoked, ", ")))
}

func EncodeTrustResolution(resolution TrustResolutionV1) []byte {
	roles := make([]string, len(resolution.RequiredRoles))
	for i := range resolution.RequiredRoles {
		roles[i] = "TrustRole." + enumName(string(resolution.RequiredRoles[i]))
	}
	providers := make([]string, len(resolution.SelectedProviders))
	for i := range resolution.SelectedProviders {
		providers[i] = quote(resolution.SelectedProviders[i])
	}
	ids := make([]string, len(resolution.AttestationIDs))
	for i := range resolution.AttestationIDs {
		ids[i] = quote(resolution.AttestationIDs[i])
	}
	return []byte(fmt.Sprintf("TrustResolutionV1 { ResolutionID: %s TransferID: %s RequiredRoles: [%s] SelectedProviders: [%s] AttestationIDs: [%s] Admitted: %t FailureReason: %s SenderPolicyVersion: %d ReceiverPolicyVersion: %d IssuedAt: %d ProvidersConsulted: %d FreshProviderCalls: %d ReusedAttestations: %d }\n", quote(resolution.ResolutionID), quote(resolution.TransferID), strings.Join(roles, ", "), strings.Join(providers, ", "), strings.Join(ids, ", "), resolution.Admitted, quote(resolution.FailureReason), resolution.SenderPolicyVersion, resolution.ReceiverPolicyVersion, resolution.IssuedAt, resolution.ProvidersConsulted, resolution.FreshProviderCalls, resolution.ReusedAttestations))
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
