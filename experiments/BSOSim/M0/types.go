// Package bsosim implements BSO-SIM-M0, a deterministic proof-of-concept
// distributed OLTP simulation. It is not a banking or payment product.
package bsosim

import "time"

type Money int64

type TransferState string

const (
	StateReserved     TransferState = "reserved"
	StateAccepted     TransferState = "accepted"
	StateCommitted    TransferState = "committed"
	StateAcknowledged TransferState = "acknowledged"
	StateRejected     TransferState = "rejected"
	StateExpired      TransferState = "expired"
)

type MessageKind string

const (
	MessageOffer     MessageKind = "offer"
	MessageAccept    MessageKind = "accept"
	MessageReject    MessageKind = "reject"
	MessageCommit    MessageKind = "commit"
	MessageAck       MessageKind = "acknowledge"
	MessageReconcile MessageKind = "reconcile"
)

type Transfer struct {
	ID            string        `json:"id"`
	From          string        `json:"from"`
	To            string        `json:"to"`
	Amount        Money         `json:"amount"`
	State         TransferState `json:"state"`
	DebitApplied  bool          `json:"debit_applied,omitempty"`
	CreditApplied bool          `json:"credit_applied,omitempty"`
	CreatedRound  int           `json:"created_round"`
}

type AuditEntry struct {
	TransferID string        `json:"transfer_id"`
	State      TransferState `json:"state"`
	Delta      Money         `json:"delta"`
}

type BSOState struct {
	ID              string              `json:"id"`
	Balance         Money               `json:"balance"`
	Reserved        Money               `json:"reserved"`
	Outgoing        map[string]Transfer `json:"outgoing"`
	Incoming        map[string]Transfer `json:"incoming"`
	SeenMessages    map[string]bool     `json:"seen_messages"`
	Audit           []AuditEntry        `json:"audit"`
	ProtocolVersion int                 `json:"protocol_version"`
}

type Envelope struct {
	MessageID  string        `json:"message_id"`
	TransferID string        `json:"transfer_id"`
	From       string        `json:"from"`
	To         string        `json:"to"`
	Kind       MessageKind   `json:"kind"`
	Amount     Money         `json:"amount"`
	State      TransferState `json:"state,omitempty"`
	Auth       string        `json:"auth"`
}

type FaultProfile struct {
	Name          string  `json:"name"`
	DropRate      float64 `json:"drop_rate"`
	DuplicateRate float64 `json:"duplicate_rate"`
	MaxDelay      int     `json:"max_delay"`
	ReorderWindow int     `json:"reorder_window"`
}

var FaultProfiles = map[string]FaultProfile{
	"none": {Name: "none"},
	"fun":  {Name: "fun", DropRate: 0.01, DuplicateRate: 0.02, MaxDelay: 3, ReorderWindow: 5},
	"mean": {Name: "mean", DropRate: 0.05, DuplicateRate: 0.10, MaxDelay: 5, ReorderWindow: 9},
}

type CrashPoint string

const (
	CrashAfterReserve        CrashPoint = "after_reserve"
	CrashAfterAccept         CrashPoint = "after_accept"
	CrashAfterSenderCommit   CrashPoint = "after_sender_commit"
	CrashAfterReceiverCommit CrashPoint = "after_receiver_commit"
	CrashBeforeAck           CrashPoint = "before_ack"
)

type Config struct {
	Seed              int64        `json:"seed"`
	BSOs              int          `json:"bsos"`
	Transfers         int          `json:"transfers"`
	InitialBalance    Money        `json:"initial_balance"`
	Workload          string       `json:"workload"`
	Faults            FaultProfile `json:"faults"`
	MaxRounds         int          `json:"max_rounds"`
	ReservationExpiry int          `json:"reservation_expiry"`
	CrashSchedule     []CrashPoint `json:"crash_schedule,omitempty"`
	DataDir           string       `json:"-"`
}

type Attempt struct {
	ID     string `json:"id"`
	From   string `json:"from"`
	To     string `json:"to"`
	Amount Money  `json:"amount"`
}

type Metrics struct {
	Attempted                int     `json:"attempted"`
	Successful               int     `json:"successful"`
	Rejected                 int     `json:"rejected"`
	TransfersPerSecond       float64 `json:"transfers_per_second"`
	P50LogicalLatency        int     `json:"p50_logical_latency"`
	P95LogicalLatency        int     `json:"p95_logical_latency"`
	P99LogicalLatency        int     `json:"p99_logical_latency"`
	MessagesSent             int     `json:"messages_sent"`
	MessagesDelivered        int     `json:"messages_delivered"`
	MessagesDropped          int     `json:"messages_dropped"`
	DuplicatesInjected       int     `json:"duplicates_injected"`
	DelayedOrReordered       int     `json:"delayed_or_reordered"`
	DuplicatesSuppressed     int     `json:"duplicates_suppressed"`
	Retries                  int     `json:"retries"`
	ReconciliationActions    int     `json:"reconciliation_actions"`
	Unresolved               int     `json:"unresolved"`
	LocalDurableMutations    int     `json:"local_durable_mutations"`
	MessagesPerSuccess       float64 `json:"messages_per_success"`
	DurableCommitsPerSuccess float64 `json:"durable_commits_per_success"`
	GlobalSerializationOps   int     `json:"global_serialization_ops,omitempty"`
	DoubleDebits             int     `json:"double_debits"`
	DoubleCredits            int     `json:"double_credits"`
	AuthenticationFailures   int     `json:"authentication_failures"`
	Crashes                  int     `json:"crashes"`
}

type Result struct {
	Config              Config        `json:"config"`
	Metrics             Metrics       `json:"metrics"`
	InitialTotal        Money         `json:"initial_total"`
	FinalTotal          Money         `json:"final_total"`
	Conservation        bool          `json:"conservation"`
	CorrectnessDigest   string        `json:"correctness_digest"`
	Elapsed             time.Duration `json:"-"`
	ElapsedMilliseconds int64         `json:"elapsed_ms"`
}

type Comparison struct {
	BSO    Result `json:"bso"`
	Global Result `json:"global"`
}

func DefaultConfig() Config {
	return Config{Seed: 20260823, BSOs: 100, Transfers: 2000, InitialBalance: 100_000,
		Workload: "random", Faults: FaultProfiles["fun"], MaxRounds: 24, ReservationExpiry: 8}
}
